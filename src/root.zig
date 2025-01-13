const std = @import("std");
const testing = std.testing;

export fn add(a: i32, b: i32) i32 {
    return a + b;
}

test "basic add functionality" {
    std.testing.log_level = .debug;
    std.log.warn("run test", .{});
    try testing.expect(add(3, 7) == 10);
}

test {
    _ = @import("cursor_test.zig");
    _ = @import("node_test.zig");
}
