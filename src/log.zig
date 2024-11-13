const std = @import("std");

pub fn Logger(comptime Context: type) type {
    return struct {
        context: Context,
        scope: @TypeOf(.EnumLiteral),

        const Self = @This();

        pub fn init(scope: @TypeOf(.EnumLiteral)) Self {
            return .{
                .context = Context{},
                .scope = scope,
            };
        }

        pub inline fn with(self: Self, ctx: Context) Self {
            return .{
                .context = ctx,
                .scope = self.scope,
            };
        }

        fn writeContext(ctx: Context, msg: []const u8, writer: anytype) !void {
            try std.json.stringify(.{ .ctx = ctx, .msg = msg }, .{}, writer);
        }

        pub fn debug(self: Self, comptime fmt: []const u8, args: anytype) void {
            const l = std.log.scoped(self.scope);
            var buf: [1024]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&buf);
            var string = std.ArrayList(u8).init(fba.allocator());
            try std.fmt.format(string.writer(), fmt, args);
            l.debug("{s}", .{string.items});
        }

        pub fn info(self: Self, comptime fmt: []const u8, args: anytype) void {
            const l = std.log.scoped(self.scope);
            var buf: [1024]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&buf);
            var string = std.ArrayList(u8).init(fba.allocator());

            var msg_buf: [512]u8 = undefined;
            const msg = std.fmt.bufPrint(&msg_buf, fmt, args) catch return;
            writeContext(self.context, msg, string.writer()) catch return;
            l.info("{s}", .{string.items});
        }

        pub fn warn(self: Self, comptime fmt: []const u8, args: anytype) void {
            const l = std.log.scoped(self.scope);
            var buf: [1024]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&buf);
            var string = std.ArrayList(u8).init(fba.allocator());
            try std.fmt.format(string.writer(), fmt, args);
            l.warn("{s}", .{string.items});
        }

        pub fn err(self: Self, comptime fmt: []const u8, args: anytype) void {
            const l = std.log.scoped(self.scope);
            var buf: [1024]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&buf);
            var string = std.ArrayList(u8).init(fba.allocator());

            var msg_buf: [512]u8 = undefined;
            const msg = std.fmt.bufPrint(&msg_buf, fmt, args) catch return;
            writeContext(self.context, msg, string.writer()) catch return;
            l.err("{s}", .{string.items});
        }
    };
}

test "Logger with JSON context" {
    const TestContext = struct {
        id: ?u32 = null,
        name: ?[]const u8 = null,
        is_test: bool = true,
    };
    std.testing.log_level = .debug;
    const TestLogger = Logger(TestContext);
    const logger = TestLogger.init(.Bolt);

    // Test with context
    const l = logger.with(.{
        .id = 123,
        .name = "test_case",
        .is_test = true,
    });
    l.info("test with context", .{});
}
