const std = @import("std");
const db = @import("db.zig");
const DB = db.DB;
const node = @import("node.zig");
const consts = @import("consts.zig");

// A tuple of two values.
pub const Tuple = struct {
    pub fn t2(comptime firstType: type, comptime secondType: type) type {
        return struct {
            first: firstType,
            second: secondType,
        };
    }
    pub fn t3(comptime firstType: type, comptime secondType: type, comptime thirdType: type) type {
        return struct {
            first: firstType,
            second: secondType,
            third: thirdType,
        };
    }
};

/// A test context.
pub const TestContext = struct {
    allocator: std.mem.Allocator,
    db: *DB,
    pub fn generateBytes(self: @This(), bufSize: usize) []usize {
        const buffer = self.allocator.alloc(usize, bufSize) catch unreachable;
        randomBuf(buffer);
        return buffer;
    }

    pub fn repeat(self: @This(), c: u8, bufferSize: usize) []u8 {
        const buffer = self.allocator.alloc(u8, bufferSize) catch unreachable;
        @memset(buffer, c);
        return buffer;
    }
};

/// Setup a test context.
pub fn setup() !TestContext {
    // if (std.testing.log_level != .err) {
    //     std.testing.log_level = .debug;
    // }
    // std.testing.log_level = .debug;
    var options = db.defaultOptions;
    options.readOnly = false;
    options.initialMmapSize = 100000 * consts.PageSize;
    // options.strictMode = true;
    const filePath = try std.fmt.allocPrint(std.testing.allocator, "dirty/{}.db", .{std.time.milliTimestamp()});
    defer std.testing.allocator.free(filePath);

    const kvDB = DB.open(std.testing.allocator, filePath, null, options) catch unreachable;
    return TestContext{ .allocator = std.testing.allocator, .db = kvDB };
}

/// Teardown a test context.
pub fn teardown(ctx: TestContext) void {
    const path = ctx.allocator.dupe(u8, ctx.db.path()) catch unreachable;
    defer ctx.allocator.free(path);
    ctx.db.close() catch unreachable;
    std.fs.cwd().deleteFile(path) catch unreachable;
    std.log.debug("delete dirty file: {s}\n", .{path});
}

/// Generate a random buffer.
pub fn randomBuf(buf: []usize) void {
    var prng = std.Random.DefaultPrng.init(buf.len);
    var random = prng.random();
    for (0..buf.len) |i| {
        buf[i] = @intCast(i);
    }
    var i: usize = buf.len - 1;
    while (i > 0) : (i -= 1) {
        const j = random.intRangeAtMost(usize, 0, i);
        std.mem.swap(usize, &buf[i], &buf[j]);
    }
}

/// testing/quick defaults to 5 iterations and a random seed.
/// You can override these settings from the command line:
///   -quick.count     The number of iterations to perform.
///   -quick.seed      The seed to use for randomizing.
///   -quick.maxitems  The maximum number of items to insert into a DB.
///   -quick.maxksize  The maximum size of a key.
///   -quick.maxvsize  The maximum size of a value.
pub const Quick = struct {
    count: usize = 5,
    seed: u64 = 0,
    maxItems: usize = 10000,
    maxKeySize: usize = 1024,
    maxValueSize: usize = 1024,
    items: std.ArrayList(TestDataItem) = undefined,
    allocator: std.mem.Allocator,

    /// Initialize a Quick instance.
    pub fn init(allocator: std.mem.Allocator) Quick {
        return Quick{
            .allocator = allocator,
            .seed = @intCast(std.time.microTimestamp()),
            .items = undefined,
        };
    }

    /// Deinitialize a Quick instance.
    pub fn deinit(self: *@This()) void {
        for (self.items.items) |item| {
            self.allocator.free(item.key);
            self.allocator.free(item.value);
        }
        self.items.deinit();
    }

    /// Generate a set of test data.
    pub fn generate(self: *@This(), allocator: std.mem.Allocator) !std.ArrayList(TestDataItem) {
        var randItems = try RevTestData.generate(allocator, self);
        const slice = try randItems.toOwnedSlice();
        self.items = std.ArrayList(TestDataItem).fromOwnedSlice(self.allocator, slice);
        return self.items;
    }

    /// Sort the items by key.
    pub fn sort(self: *@This()) void {
        std.mem.sort(TestDataItem, self.items.items, {}, struct {
            fn lessThan(_: void, lhs: TestDataItem, rhs: TestDataItem) bool {
                return std.mem.lessThan(u8, lhs.key, rhs.key);
            }
        }.lessThan);
    }

    /// Reverse the items.
    pub fn reverse(self: *@This()) void {
        self.sort();
        std.mem.reverse(TestDataItem, self.items.items);
    }
};

/// A test data item.
pub const TestDataItem = struct {
    key: []u8,
    value: []u8,
};

/// A test data generator.
pub const RevTestData = struct {
    const Self = @This();
    /// Generate a set of test data.
    pub fn generate(
        allocator: std.mem.Allocator,
        q: *const Quick,
    ) !std.ArrayList(TestDataItem) {
        var prng = std.Random.DefaultPrng.init(q.seed);
        var random = prng.random();
        const n = random.intRangeAtMost(usize, 1, q.maxItems);
        var items = std.ArrayList(TestDataItem).init(allocator);
        try items.appendNTimes(TestDataItem{ .key = undefined, .value = undefined }, n);
        var used = std.StringHashMap(bool).init(allocator);
        defer used.deinit();
        for (0..items.items.len) |i| {
            while (true) {
                const randBytes = try Self.randByteSlice(allocator, random, 1, q.maxKeySize);
                const got = try used.getOrPut(randBytes);
                if (got.found_existing) {
                    // std.debug.print("Key collision: {any}\n", .{randBytes});
                    allocator.free(randBytes);
                    continue;
                } else {
                    got.value_ptr.* = true;
                    items.items[i].key = randBytes;
                    // std.debug.print("Generated key: {any}\n", .{randBytes});
                }
                break;
            }

            const randBytes = try Self.randByteSlice(allocator, random, 1, q.maxValueSize);
            items.items[i].value = randBytes;
        }
        return items;
    }

    /// Generate a random byte slice.
    fn randByteSlice(allocator: std.mem.Allocator, random: std.Random, minSize: usize, maxSize: usize) ![]u8 {
        const n = random.intRangeAtMost(usize, minSize, maxSize);
        var b = try allocator.alloc(u8, n);
        for (0..n) |i| {
            b[i] = @intCast(random.intRangeAtMost(u8, 0, 255));
        }
        return b;
    }
};
