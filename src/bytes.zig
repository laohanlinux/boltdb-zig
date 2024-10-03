const std = @import("std");

// TODO: use BufStr instead of std.ArrayList(u8)
pub const Bytes = struct {
    s: ?std.ArrayList(u8),
    ref: *std.atomic.Value(i64),
    const Self = @This();

    /// Init a Bytes.
    pub fn init(allocator: std.mem.Allocator, slice: ?[]u8) !Self {
        const refValue = allocator.create(std.atomic.Value(i64)) catch unreachable;
        refValue.store(1, .seq_cst);
        if (slice) |_slice| {
            const s = try std.ArrayList(u8).fromOwnedSlice(allocator, _slice);
            return Self{ .s = s, .ref = refValue };
        }

        return Self{ .s = null, .ref = refValue };
    }

    /// Deinit a Bytes.
    pub fn deinit(self: *Self) void {
        const refCount = self.ref.fetchSub(1, .seq_cst);
        std.debug.assert(refCount > 0);
        if (refCount == 1) {
            if (self.s) |s| {
                // std.log.info("deinit bytes: {s}", .{s.items});
                s.deinit();
            }
            self.ref.destroy();
        }
    }

    /// Increment the reference count.
    pub fn incrRef(self: *Self) void {
        _ = self.ref.fetchAdd(1, .seq_cst);
    }

    /// Decrement the reference count.
    pub fn decrRef(self: *Self) void {
        const refCount = self.ref.fetchSub(1, .seq_cst);
        if (refCount == 1) {
            self.deinit();
        }
    }

    /// Get the slice of the bytes.
    pub fn asSlice(self: *Self) ?[]u8 {
        if (self.s) |s| {
            return s.items;
        }
        return null;
    }

    /// Clone the bytes.
    pub fn clone(self: *Self) !Self {
        const new = try Self.init(self.ref.allocator(), self.asSlice());
        new.incrRef();
        return new;
    }

    /// Copy the bytes.
    pub fn copy(self: Self, allocator: std.mem.Allocator) !Self {
        var new: Self = undefined;
        new.ref = allocator.create(std.atomic.Value(i64)) catch unreachable;
        new.ref.store(1, .seq_cst);
        if (self.s) |s| {
            new.s = try std.ArrayList(u8).init(allocator);
            try new.s.appendSlice(s.items);
            return new;
        }
        new.s = null;
        return new;
    }

    /// Get the owned slice of the bytes.
    pub fn toOwnedSlice(self: *Self) ![]u8 {
        if (self.s) |s| {
            return try s.toOwnedSlice();
        }
        return null;
    }

    /// Get the length of the bytes.
    pub fn len(self: *Self) usize {
        if (self.s) |s| {
            return s.items.len;
        }
        return 0;
    }

    /// Get the capacity of the bytes.
    pub fn capacity(self: *Self) usize {
        if (self.s) |s| {
            return s.capacity;
        }
        return 0;
    }

    /// Get the pointer of the bytes.
    pub fn ptr(self: *Self) ?*u8 {
        if (self.s) |s| {
            return s.items.ptr;
        }
        return null;
    }

    /// Check if the bytes is empty.
    pub fn isEmpty(self: *Self) bool {
        return self.len() == 0;
    }
};
