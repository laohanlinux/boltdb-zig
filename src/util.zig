const std = @import("std");

/// Asserts that `ok` is true. If not, it will print the formatted message and panic.
pub inline fn assert(ok: bool, comptime fmt: []const u8, args: anytype) void {
    if (ok) {
        return;
    }
    const allocator = std.heap.page_allocator;
    const s = std.fmt.allocPrint(allocator, fmt, args) catch unreachable;
    std.debug.print("{s}\n", .{s});
    defer allocator.free(s);
    @panic(s);
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
    const alignData: []align(std.mem.page_size) const u8 = @alignCast(ptr);
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

pub fn Closure(comptime T: type) type {
    return struct {
        captureVar: *T,
        _callback: *const fn (t: *T) void,
        const Self = @This();
        pub fn init(capture: *T, callback: fn (_: *T) void) Self {
            return Self{
                .captureVar = capture,
                ._callback = callback,
            };
        }
        pub fn getCallBack(self: *const Self) *const fn (_: *T) void {
            return self._callback;
        }
        pub fn onCommit(self: *const Self) void {
            self._callback(self.captureVar);
        }

        pub fn execute(self: *const Self) void {
            self._callback(self.captureVar);
        }
    };
}

// const Context = struct {
//     fn init(num: usize) struct {
//         num: usize,
//         fn onCommit(self: *anyopaque) void {
//             const context: *Context = @ptrCast(@alignCast(self));
//             context.num += 1;
//         }
//     } {
//         return .{ .num = num, .onCommit = onCmt3 };
//     }
// };
// fn onCmt3(ctx: *anyopaque) void {
//     const context: *Context = @ptrCast(@alignCast(ctx));
//     context.num += 1;
// }

// test "arm" {
//     var n: usize = 2000;
//     const c = Closure(usize).init(&n, onCmt);
//     var closures = std.ArrayList(Closure(usize)).init(std.testing.allocator);
//     defer closures.deinit();
//     try closures.append(c);
//     try closures.append(c);
//     for (closures.items) |cFn| {
//         cFn.execute();
//         std.debug.print("{}\n", .{c.captureVar.*});
//     }

//     // const arch = @import("builtin").cpu.arch;

//     // if (target == .arm or target == .aarch64) {
//     //     std.debug.print("This is an ARM platform.\n", .{});
//     // } else {
//     //     std.debug.print("This is not an ARM platform.\n", .{});
//     // }
//     // std.debug.print("{}\n", .{std.Target.Cpu.Arch.isAARCH64(arch)});

//     // const fp = try std.fs.cwd().createFile("map.test", .{});
//     // defer fp.close();
//     // const fileSize = 1024 * 1024;
//     // const ptr = try mmap(fp, fileSize, true);
//     // defer munmap(ptr);

//     // const file_path = "example.txt";
//     //
//     // // 打开文件
//     // const file_descriptor = try std.fs.cwd().createFile(file_path, .{});
//     // try file_descriptor.setEndPos(std.mem.page_size);
//     // const buf = try mmap(file_descriptor, std.mem.page_size, true);
//     // const alignData: []align(std.mem.page_size) const u8 = @alignCast(buf);
//     // defer std.posix.munmap(alignData); // 确保在函数结束时撤销映射
//     //
//     // // 关闭文件描述符
//     // _ = std.posix.close(file_descriptor.handle);
// }

// test "lowerBound" {
//     const cmp = struct {
//         fn cmp(context: []const u8, b: []const u8) std.math.Order {
//             return std.mem.order(u8, context, b);
//         }
//     };
//     var slice = std.ArrayList([]const u8).init(std.testing.allocator);
//     defer slice.deinit();
//     try slice.append("0000000493");
//     try slice.append("0000000494");
//     try slice.append("0000000495");
//     try slice.append("0000000496");
//     try slice.append("0000000497");
//     const key: []const u8 = "0000000493";
//     const index = std.sort.binarySearch([]const u8, slice.items, key, cmp.cmp);
//     assert(index.? == 0, "index should be 0, but got {}", .{index.?});
//     _ = slice.orderedRemove(index.?);
//     const index2 = std.sort.binarySearch([]const u8, slice.items[0..], key, cmp.cmp);
//     assert(index2 == null, "index should be null, but got {any}", .{index2});
// }

test "iterator" {
    var list = std.ArrayList(*u8).init(std.testing.allocator);
    defer list.deinit();
    defer for (list.items) |item| {
        std.testing.allocator.destroy(item);
    };
    for (0..10) |i| {
        const ptr = try std.testing.allocator.create(u8);
        ptr.* = @intCast(i);
        try list.append(ptr);
    }

    for (list.items, 0..) |item, i| {
        item.* = @intCast(2 * i);
        std.debug.print("{}, {}\n", .{ i, item.* });
    }

    for (list.items) |item| {
        std.debug.print("{}\n", .{item.*});
    }

    // const Transaction = struct {
    //     a: u8,
    //     onCommit: ?*const fn (*anyopaque) void = null,
    // };

    // var ctx = Context{ .num = 1 };
    // const trxRef = try std.testing.allocator.create(Transaction);
    // defer std.testing.allocator.destroy(trxRef);
    // trxRef.* = .{ .a = 1, .onCommit = null };

    // assert(ctx.num == 2, "ctx.num should be 2, but got {}", .{ctx.num});
}
