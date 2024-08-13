const std = @import("std");

/// Asserts that `expect` is true. If not, prints the formatted string `fmt` with the arguments `args` and then asserts.
pub inline fn assert(expect: bool, comptime fmt: []const u8, args: anytype) void {
    if (!expect) {
        std.debug.print(fmt, args);
        std.debug.assert(expect);
    }
}
