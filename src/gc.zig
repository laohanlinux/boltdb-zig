const std = @import("std");
const Page = @import("page.zig").Page;

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

pub const PagePool = struct {
    allocator: std.mem.Allocator,
    free: std.ArrayList(*Page),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return {.allocator = allocator, .free = std.ArrayList.init(allocator)};
    }
};
