const std = @import("std");
const Page = @import("page.zig").Page;
const assert = @import("util.zig").assert;

/// A simple garbage collector that frees slices of memory when triggered.
pub const GC = struct {
    slices: std.AutoHashMap(u64, struct {
        allocator: std.mem.Allocator,
        bytes: []u8,
    }),
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Initializes the GC with a given allocator.
    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .slices = std.AutoHashMap(u64, struct {
                allocator: std.mem.Allocator,
                bytes: []u8,
            }).init(allocator),
            .allocator = allocator,
        };
    }

    /// Creates a new GC.
    pub fn create(allocator: std.mem.Allocator) *Self {
        const self = allocator.create(Self) catch unreachable;
        self.* = Self.init(allocator);
        return self;
    }

    /// Deinitializes the GC and frees all allocated memory.
    pub fn deinit(self: *Self) void {
        self.trigger();
    }

    /// Destroys the GC and frees all allocated memory.
    pub fn destroy(self: *Self) void {
        self.deinit();
        self.allocator.destroy(self);
    }

    /// Adds a new slice to the GC.
    pub fn add(self: *Self, allocator: std.mem.Allocator, bytes: []u8) !void {
        const ptr = @intFromPtr(bytes.ptr);
        const entry = self.slices.getOrPut(ptr) catch unreachable;
        if (!entry.found_existing) {
            entry.value_ptr.allocator = allocator;
            entry.value_ptr.bytes = bytes;
        }
    }

    pub fn addArrayList(self: *Self, list: std.ArrayList(u8)) !void {
        const bytes = try list.toOwnedSlice();
        const allocator = list.allocator;
        try self.add(allocator, bytes);
    }

    /// Triggers the GC to free all slices.
    pub fn trigger(self: *Self) void {
        var itr = self.slices.iterator();
        while (itr.next()) |entry| {
            entry.value_ptr.allocator.free(entry.value_ptr.bytes);
        }
        self.slices.clearAndFree();
    }
};

/// A pool of pages that can be reused.
pub const PagePool = struct {
    free: std.ArrayList(*Page),
    arena: std.heap.ArenaAllocator,
    lock: std.Thread.Mutex, // Protects meta page access.
    pageSize: usize = 0,
    allocSize: usize = 0,

    /// Initializes the PagePool with a given allocator and page size.
    pub fn init(allocator: std.mem.Allocator, pageSize: usize) @This() {
        return .{ .arena = std.heap.ArenaAllocator.init(allocator), .free = std.ArrayList(*Page).init(allocator), .lock = .{}, .pageSize = pageSize };
    }

    /// Deinitializes the PagePool and frees all allocated memory.
    pub fn deinit(self: *@This()) void {
        self.lock.lock();
        defer self.lock.unlock();
        self.free.deinit();
        self.arena.deinit();
    }

    /// Allocates a new page from the pool or creates a new one if the pool is empty.
    pub fn new(self: *@This()) !*Page {
        self.lock.lock();
        defer self.lock.unlock();
        const p = if (self.free.popOrNull()) |hasPage| hasPage else {
            const buffer = try self.arena.allocator().alloc(u8, self.pageSize);
            @memset(buffer, 0);
            self.allocSize += buffer.len;
            return Page.init(buffer);
        };
        return p;
    }

    /// Deletes a page from the pool.
    pub fn delete(self: *@This(), p: *Page) void {
        const buffer = p.asSlice();
        assert(buffer.len == self.pageSize, "page size mismatch", .{});
        @memset(buffer, 0);
        self.lock.lock();
        defer self.lock.unlock();
        self.free.append(p) catch unreachable;
    }

    /// Returns the total allocated size of the PagePool.
    pub fn getAllocSize(self: *@This()) usize {
        self.lock.lock();
        defer self.lock.unlock();
        return self.allocSize;
    }
};

test "Page Pool" {
    const consts = @import("consts.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var pagePool = PagePool.init(arena.allocator(), consts.PageSize);
    defer pagePool.deinit();
    for (0..10000) |_| {
        const p = try pagePool.new();
        pagePool.delete(p);
    }
}
