const std = @import("std");
const db = @import("db.zig");
const DB = db.DB;
const node = @import("node.zig");
const consts = @import("consts.zig");

/// A test context.
pub const TestContext = struct {
    allocator: std.mem.Allocator,
    db: *DB,
};

/// Setup a test context.
pub fn setup() !TestContext {
    if (std.testing.log_level != .err) {
        std.testing.log_level = .debug;
    }
    // std.testing.log_level = .debug;
    var options = db.defaultOptions;
    options.readOnly = false;
    options.initialMmapSize = 10000 * consts.PageSize;
    // options.strictMode = true;
    const filePath = try std.fmt.allocPrint(std.testing.allocator, "dirty/{}.db", .{std.time.milliTimestamp()});
    defer std.testing.allocator.free(filePath);

    const kvDB = DB.open(std.testing.allocator, filePath, null, options) catch unreachable;
    return TestContext{ .allocator = std.testing.allocator, .db = kvDB };
}

/// Teardown a test context.
pub fn teardown(ctx: TestContext) void {
    ctx.db.close() catch unreachable;
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
