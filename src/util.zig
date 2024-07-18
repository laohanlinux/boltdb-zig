const std = @import("std");

/// Compares two byte slices lexicographically.
pub fn cmpBytes(a: []const u8, b: []const u8) std.math.Order {
    var i: usize = 0;
    var j: usize = 0;
    while (i < a.len and j < b.len) {
        if (a[i] < b[j]) {
            return std.math.Order.lt;
        } else if (a[i] > b[j]) {
            return std.math.Order.gt;
        }
        i += 1;
        j += 1;
    }

    if (i < a.len) {
        return std.math.Order.gt;
    } else if (j < b.len) {
        return std.math.Order.lt;
    } else {
        return std.math.Order.eq;
    }
}

pub fn cloneBytes(allocator: std.mem.Allocator, b: []const u8) []u8 {
    const dest = allocator.alloc(u8, b.len) catch unreachable;
    @memcpy(dest, b[0..]);
    return dest;
}

/// Returns true if `a` is less than `b`.
pub fn lessThan(a: []const u8, b: []const u8) bool {
    return cmpBytes(a, b) == std.math.Order.lt;
}

/// Returns true if `a` is equal to `b`.
pub fn equals(a: []const u8, b: []const u8) bool {
    return cmpBytes(a, b) == std.math.Order.eq;
}

/// Returns true if `a` is greater than `b`.
pub fn greaterThan(a: []const u8, b: []const u8) bool {
    return cmpBytes(a, b) == std.math.Order.gt;
}

pub fn assert(ok: bool, comptime fmt: []const u8, args: anytype) void {
    if (ok) {
        return;
    }
    const allocator = std.heap.page_allocator;
    const s = std.fmt.allocPrint(allocator, fmt, args) catch unreachable;
    std.debug.print("{s}\n", .{s});
    defer allocator.free(s);
    @panic(s);
}

pub inline fn isWindows() bool {
    const tag = @import("builtin").os.tag;
    return (tag == .windows);
}

pub inline fn isLinux() bool {
    const tag = @import("builtin").os.tag;
    return (tag == .linux);
}

pub inline fn isMacOS() bool {
    const tag = @import("builtin").os.tag;
    return tag.isDarwin();
}

/// TODO check platform
pub inline fn maxMapSize() usize {
    return 1 << 32;
}

pub fn mmap(fp: std.fs.File, fileSize: u64, writeable: bool) ![]u8 {
    var port: u32 = std.posix.PROT.READ;
    if (writeable) {
        port |= std.posix.PROT.WRITE;
    }
    const ptr = try std.posix.mmap(null, fileSize, port, .{ .TYPE = .SHARED }, fp.handle, 0);
    return ptr;
}

pub fn munmap(ptr: []u8) void {
    std.debug.print("the ptr size: {}, {}\n", .{ ptr.len, std.mem.page_size });
    const alignData: []align(std.mem.page_size) const u8 = @alignCast(ptr);
    if (isLinux() or isMacOS()) {
        std.posix.munmap(alignData);
    } else {
        @panic("not support the os");
    }
}

test "arm" {
    // const arch = @import("builtin").cpu.arch;

    // if (target == .arm or target == .aarch64) {
    //     std.debug.print("This is an ARM platform.\n", .{});
    // } else {
    //     std.debug.print("This is not an ARM platform.\n", .{});
    // }
    // std.debug.print("{}\n", .{std.Target.Cpu.Arch.isAARCH64(arch)});

    // const fp = try std.fs.cwd().createFile("map.test", .{});
    // defer fp.close();
    // const fileSize = 1024 * 1024;
    // const ptr = try mmap(fp, fileSize, true);
    // defer munmap(ptr);

    // const file_path = "example.txt";
    //
    // // 打开文件
    // const file_descriptor = try std.fs.cwd().createFile(file_path, .{});
    // try file_descriptor.setEndPos(std.mem.page_size);
    // const buf = try mmap(file_descriptor, std.mem.page_size, true);
    // const alignData: []align(std.mem.page_size) const u8 = @alignCast(buf);
    // defer std.posix.munmap(alignData); // 确保在函数结束时撤销映射
    //
    // // 关闭文件描述符
    // _ = std.posix.close(file_descriptor.handle);
}
