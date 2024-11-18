const std = @import("std");
const db = @import("db.zig");
const DB = db.DB;
const node = @import("node.zig");
const consts = @import("consts.zig");
const Error = @import("error.zig").Error;

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
pub fn setup(allocator: std.mem.Allocator) !TestContext {
    var options = consts.defaultOptions;
    options.readOnly = false;
    options.initialMmapSize = 100000 * consts.PageSize;
    return setupWithOptions(allocator, options);
}

/// Setup a test context with custom options.
pub fn setupWithOptions(allocator: std.mem.Allocator, options: consts.Options) !TestContext {
    const filePath = try std.fmt.allocPrint(allocator, "dirty/{}.db", .{std.time.milliTimestamp()});
    defer allocator.free(filePath);

    const kvDB = DB.open(allocator, filePath, null, options) catch unreachable;
    return TestContext{ .allocator = allocator, .db = kvDB };
}

/// Teardown a test context.
pub fn teardown(ctx: *TestContext) void {
    std.log.debug("teardown", .{});
    const path = ctx.allocator.dupe(u8, ctx.db.path()) catch unreachable;
    ctx.db.close() catch unreachable;
    std.fs.cwd().deleteFile(path) catch unreachable;
    std.log.debug("delete dirty file: {s}\n", .{path});
    ctx.allocator.free(path);
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

/// Create a temporary file.
pub fn createTmpFile(name: ?[]const u8) struct {
    file: std.fs.File,
    tmpDir: std.testing.TmpDir,
} {
    var tmpDir = std.testing.tmpDir(.{});
    if (name) |n| {
        return .{ .file = tmpDir.dir.createFile(n, .{}) catch unreachable, .tmpDir = tmpDir };
    } else {
        return .{ .file = tmpDir.dir.createFile("bolt.db.tmp", .{}) catch unreachable, .tmpDir = tmpDir };
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

    pub fn checkWithContext(self: *@This(), context: anytype, config: ?Config, comptime travel: fn (@TypeOf(context)) Error!void) std.ArrayList(Error) {
        if (config == null) {
            config = .{
                .rand = std.Random.DefaultPrng.init(0).random(),
            };
        }
        const maxCount = config.?.getMaxCount();
        const randor = config.?.getRand();
        _ = randor; // autofix

        var errors = std.ArrayList(Error).init(self.allocator);

        for (0..maxCount) |i| {
            _ = i; // autofix
            travel(context) catch |err| {
                errors.append(err) catch unreachable;
            };
        }
        return errors;
    }
};

pub const Config = struct {
    // MaxCount sets the maximum number of iterations.
    // If zero, MaxCountScale is used.
    maxCount: usize = 0,
    // MaxCountScale is a non-negative scale factor applied to the
    // default maximum.
    // A count of zero implies the default, which is usually 100
    // but can be set by the -quickchecks flag.
    maxCountScale: f64 = 1.0,
    maxKeySize: usize = 1024,
    // Rand specifies a source of random numbers.
    // If nil, a default pseudo-random source will be used.
    rand: ?std.Random.Xoshiro256 = null,

    pub fn getRand(self: @This()) std.Random {
        if (self.rand) |r| {
            return r.random();
        } else {
            return std.Random.DefaultPrng.init(0).random();
        }
    }

    pub fn getMaxCount(self: @This()) usize {
        if (self.maxCount == 0) {
            if (self.maxCountScale != 0) {
                const count: f64 = self.maxCountScale * 100.0;
                return @as(usize, @intFromFloat(count));
            }
            return 100;
        } else {
            return self.maxCount;
        }
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
                    allocator.free(randBytes);
                    continue;
                } else {
                    got.value_ptr.* = true;
                    items.items[i].key = randBytes;
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

// test "copy allocator memory" {
//     var gp = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gp.allocator();
//     var key = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
//     for (0..10) |i| {
//         _ = i; // autofix
//         const ts = std.time.microTimestamp();
//         const buf = allocator.dupe(u8, key[0..]) catch unreachable;
//         std.debug.print("cost: {}\n", .{std.time.microTimestamp() - ts});
//         allocator.free(buf);
//     }

//     var area = std.heap.ArenaAllocator.init(allocator);
//     var arenaAllocator = area.allocator();
//     for (0..10) |i| {
//         _ = i; // autofix
//         const ts = std.time.microTimestamp();
//         const buf = arenaAllocator.dupe(u8, key[0..]) catch unreachable;
//         std.debug.print("cost: {}\n", .{std.time.microTimestamp() - ts});
//         arenaAllocator.free(buf);
//     }
//     area.deinit();
//     _ = gp.deinit();
// }

test "tempFilePath" {
    var tmpFile = createTmpFile(null);
    defer tmpFile.tmpDir.cleanup();
    std.debug.print("tmp file: {s}\n", .{tmpFile.tmpDir.sub_path});
}
