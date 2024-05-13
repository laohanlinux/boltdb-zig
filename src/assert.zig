const std = @import("std");
pub fn assert(expect: bool, comptime fmt: []const u8, args: anytype) void {
    if (!expect) {
        std.debug.print(fmt, args);
        std.debug.assert(expect);
    }
}
