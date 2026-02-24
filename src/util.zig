const std = @import("std");

/// Asserts that `ok` is true. If not, it will print the formatted message and panic.
pub inline fn assert(ok: bool, comptime fmt: []const u8, args: anytype) void {
    if (ok) {
        return;
    }
    std.debug.print(fmt ++ "\n", args);
    std.debug.assert(ok);
}

/// panic the program with the formatted message
pub inline fn panicFmt(comptime fmt: []const u8, args: anytype) noreturn {
    const allocator = std.heap.page_allocator;
    const s = std.fmt.allocPrint(allocator, fmt, args) catch unreachable;
    std.debug.print("{s}\n", .{s});
    defer allocator.free(s);
    @panic(s);
}

/// check the platform is Windows
pub inline fn isWindows() bool {
    const tag = @import("builtin").os.tag;
    return (tag == .windows);
}

/// check the platform is Linux
pub inline fn isLinux() bool {
    const tag = @import("builtin").os.tag;
    return (tag == .linux);
}

/// check the platform is MacOS
pub inline fn isMacOS() bool {
    const tag = @import("builtin").os.tag;
    return tag.isDarwin();
}

/// TODO check platform
pub inline fn maxMapSize() usize {
    return 1 << 32;
}

/// mmap the file to the memory
pub fn mmap(fp: std.fs.File, fileSize: u64, writeable: bool) ![]u8 {
    var port: u32 = std.posix.PROT.READ;
    if (writeable) {
        port |= std.posix.PROT.WRITE;
    }
    const ptr = try std.posix.mmap(null, fileSize, port, .{ .TYPE = .SHARED }, fp.handle, 0);
    return ptr;
}

pub fn munmap(ptr: []u8) void {
    // std.debug.print("the ptr size: {}, {}\n", .{ ptr.len, std.mem.page_size });
    const alignData: []align(std.heap.page_size_min) const u8 = @alignCast(ptr);
    if (isLinux() or isMacOS()) {
        std.posix.munmap(alignData);
    } else {
        @panic("not support the os");
    }
}

/// binary search the key in the items, if found, return the index and exact, if not found, return the position of the first element that is greater than the key
pub fn binarySearch(
    comptime T: type,
    items: []const T,
    context: anytype,
    comptime compareFn: fn (@TypeOf(context), T) std.math.Order,
) struct { index: usize, exact: bool } {
    if (items.len == 0) {
        return .{ .index = 0, .exact = false };
    }
    var left: usize = 0;
    var right: usize = items.len;
    while (left < right) {
        const mid = left + (right - left) / 2;
        const element = items[mid];
        const cmp = compareFn(context, element);
        switch (cmp) {
            .eq => return .{ .index = mid, .exact = true },
            .lt => left = mid + 1,
            .gt => right = mid,
        }
    }
    return .{ .index = left, .exact = false };
}
