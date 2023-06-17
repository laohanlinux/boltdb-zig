const std = @import("std");
const page = @import("page.zig");
const freelist = @import("freelist.zig");
const testing = std.testing;

export fn add(a: i32, b: i32) i32 {
    return a + b;
}

test "basic add functionality" {
    std.debug.print("Hello Word!!", .{});
}
