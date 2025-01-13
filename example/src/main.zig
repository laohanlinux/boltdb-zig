//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const std = @import("std");
const db = @import("boltdb");
pub const log_level: std.log.Level = .debug;
pub fn main() !void {
    // Prints to stderr (it's a shortcut based on `std.io.getStdErr()`)
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});

    // stdout is for the actual output of your application, for example if you
    // are implementing gzip, then only the compressed bytes should be sent to
    // stdout, not any debugging messages.
    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();

    try stdout.print("Run `zig build test` to run the tests.\n", .{});

    var allocator = std.heap.GeneralPurposeAllocator(.{}).init;
    const option = db.consts.defaultOptions;
    const database = try db.DB.open(allocator.allocator(), "boltdb.tmp", null, option);
    defer database.close() catch unreachable;
    try database.update(struct {
        fn update(tx: *db.TX) db.Error!void {
            const b = try tx.createBucketIfNotExists("user");
            try b.put(db.consts.KeyPair.init("Lusy", "29"));
        }
    }.update);
    const viewTrans = try database.begin(false);
    defer viewTrans.rollbackAndDestroy() catch unreachable;
    const b = viewTrans.getBucket("user").?;
    const age = b.get("Lusy").?;
    stdout.print("Lusy's age is: {s}\n", .{age}) catch unreachable;

    try bw.flush(); // Don't forget to flush!
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // Try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

test "fuzz example" {
    const global = struct {
        fn testOne(input: []const u8) anyerror!void {
            // Try passing `--fuzz` to `zig build test` and see if it manages to fail this test case!
            try std.testing.expect(!std.mem.eql(u8, "canyoufindme", input));
        }
    };
    try std.testing.fuzz(global.testOne, .{});
}
