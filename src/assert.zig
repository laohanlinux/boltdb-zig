const std = @import("std");

pub inline fn assert(expect: bool, comptime fmt: []const u8, args: anytype) void {
    if (!expect) {
        std.debug.print(fmt, args);
        std.debug.assert(expect);
    }
}

test "assert" {
    assert(std.time.milliTimestamp() < 0, "it should be happen, {d}\n", .{std.time.milliTimestamp()});
}
