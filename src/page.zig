const std = @import("std");
const db = @import("./db.zig");

pub const min_keys_page: usize = 2;

pub const branchPageElementSize = BranchPageElement.headerSize();
pub const leafPageElementSize = LeafPageElement.headerSize();

pub const PageFlage = enum(u8) {
    branch = 0x01,
    leaf = 0x02,
    meta = 0x04,
    free_list = 0x10,
};

pub const bucket_leaf_flag: u32 = 0x01;

pub const PgidType = u64;

pub const PgIds = []PgidType;

pub const page_size: usize = std.mem.page_size;

/// Returns the size of a page given the page size and branching factor.
pub fn intFromFlags(pageFlage: PageFlage) u16 {
    return @as(u16, @intFromEnum(pageFlage));
}

pub const Page = struct {
    id: PgidType,
    flags: u16,
    count: u16,
    overflow: u32,
    const Self = @This();
    // the size of this, but why align(4)?
    // pub const headerSize: usize = 16; // Has some bug if use @sizeOf(Page), when other file reference it;

    pub fn init(slice: []u8) *Page {
        const ptr: *Page = @ptrCast(@alignCast(slice));
        return ptr;
    }

    pub inline fn headerSize() usize {
        return @sizeOf(Self);
    }

    pub fn typ(self: *const Self) []const u8 {
        if (self.flags & intFromFlags(PageFlage.branch) != 0) {
            return "branch";
        } else if (self.flags & intFromFlags(PageFlage.leaf) != 0) {
            return "leaf";
        } else if (self.flags & intFromFlags(PageFlage.meta) != 0) {
            return "meta";
        } else if (self.flags & intFromFlags(PageFlage.free_list) != 0) {
            return "freelist";
        } else {
            return "unkown";
        }
    }

    pub fn isLeaf(self: *const Self) bool {
        return self.flags & intFromFlags(PageFlage.leaf) != 0;
    }

    // Returns a pointer to the metadata section of the page.
    pub fn meta(self: *Self) *db.Meta {
        const ptr: usize = self.getDataPtrInt();
        const _meta: *db.Meta = @ptrFromInt(ptr);
        return _meta;
    }

    // Retrives the branch node by index.
    pub fn branchPageElement(self: *Self, index: usize) ?*BranchPageElement {
        if (self.count <= index) {
            return null;
        }
        const ptr = self.getDataPtrInt() + index * BranchPageElement.headerSize();
        const dPtr: *BranchPageElement = @ptrFromInt(ptr);
        return dPtr;
    }

    pub fn opaqPtrTo(_: *Self, ptr: ?*anyopaque, comptime T: type) T {
        return @ptrCast(@alignCast(ptr));
    }

    // Retrives a list of leaf nodes.
    pub fn branchPageElements(self: *Self) ?[]BranchPageElement {
        if (self.count == 0) {
            return null;
        }
        const firstPtr = self.branchPageElementPtr(0);
        var elements: [*]BranchPageElement = @ptrCast(firstPtr);
        return elements[0..self.count];
    }

    pub fn branchPageElementPtr(self: *Self, index: usize) *BranchPageElement {
        if (self.count <= index) {
            return undefined;
        }
        const ptr = self.getDataPtrInt() + index * BranchPageElement.headerSize();
        const dPtr: *BranchPageElement = @ptrFromInt(ptr);
        return dPtr;
    }

    // Retrives the leaf node by index.
    pub fn leafPageElement(self: *Self, index: usize) ?*LeafPageElement {
        if (self.count <= index) {
            return null;
        }
        const ptr = self.getDataPtrInt() + index * LeafPageElement.headerSize();
        const dPtr: *LeafPageElement = @ptrFromInt(ptr);
        return dPtr;
    }

    pub fn leafPageElementPtr(self: *Self, index: usize) *LeafPageElement {
        if (self.count <= index) {
            return undefined;
        }
        const ptr = self.getDataPtrInt() + index * LeafPageElement.headerSize();
        const dPtr: *LeafPageElement = @ptrFromInt(ptr);
        return dPtr;
    }

    // Retrives a list of leaf nodes.
    pub fn leafPageElements(self: *Self) ?[]LeafPageElement {
        if (self.count == 0) {
            return null;
        }
        const firstPtr = self.leafPageElementPtr(0);
        var elements: [*]LeafPageElement = @ptrCast(firstPtr);
        return elements[0..self.count];
    }

    pub fn getDataPtrInt(self: *Self) usize {
        const ptr = @intFromPtr(self);
        return ptr + Self.headerSize();
    }

    pub fn getDataSlice(self: *Self) []u8 {
        const ptr = self.getDataPtrInt();
        const slice: [*]u8 = @ptrFromInt(ptr);
        return slice[0..(page_size - Self.headerSize())];
    }
};

pub const BranchPageElement = packed struct {
    //
    // |pageHeader| --> |element0|, |element1|, |element2|, |element3|, |element4| --> |key1| --> |key2| --> |key3| --> |key4|
    //
    pos: u32,
    kSize: u32,
    pgid: PgidType,

    const Self = @This();
    pub inline fn headerSize() usize {
        return @sizeOf(Self);
    }
    /// Returns a byte slice of the node key.
    pub fn key(self: *Self) []u8 {
        const buf: [*]u8 = @ptrCast(self);
        return buf[0..][self.pos..(self.pos + self.kSize)];
    }
};

pub const LeafPageElement = packed struct {
    flags: u32,
    // pos is the offset from first position of the element.
    //
    // |pageHeader| --> |element0|, |element1|, |element2|, |element3|, |element4| --> |key1, value1| --> |key2, value2| --> |key3, value3| --> |key4, value4|
    //
    pos: u32,
    kSize: u32,
    vSize: u32,

    const Self = @This();
    // pub const headerSize = @sizeOf(LeafPageElement);

    pub inline fn headerSize() usize {
        return @sizeOf(Self);
    }

    // Return a byte slice of the node key.
    pub fn key(self: *Self) []u8 {
        const buf: [*]u8 = @ptrCast(self);
        return buf[0..][self.pos..(self.pos + self.kSize)];
    }

    // Returns a byte slice of the node value.
    pub fn value(self: *Self) []u8 {
        const buf: [*]u8 = @ptrCast(self);
        return buf[0..][(self.pos + self.kSize)..(self.pos + self.kSize + self.vSize)];
    }
};

// PageInfo represents human readable information about a page.
pub const PageInfo = struct {
    id: isize,
    typ: []u8,
    count: isize,
    over_flow_count: isize,
};

/// Returns the sorted union of a and b.
pub fn merge(allocator: std.mem.Allocator, a: PgIds, b: PgIds) PgIds {
    // Return the opposite if one is nil.
    if (a.len == 0) {
        return b;
    }
    if (b.len == 0) {
        return a;
    }
    const merged = allocator.alloc(PgidType, a.len + b.len) catch unreachable;
    mergePgIds(merged, a, b);
    return merged;
}

/// Copies the sorted union of a and b into dst,
/// If dst is too small, it panics.
fn mergePgIds(dst: PgIds, a: PgIds, b: PgIds) void {
    if (dst.len < (a.len + b.len)) {
        @panic("mergepids bad len");
    }

    // Copy in the opposite slice if one is nil.
    if (a.len == 0) {
        std.mem.copyBackwards(PgidType, dst, a);
        return;
    }
    if (b.len == 0) {
        std.mem.copyBackwards(PgidType, dst, b);
        return;
    }

    // Merged will hold all elements from both lists.
    const merged: usize = 0;

    // Asign lead to the slice with a lower starting value, follow to the higher value.
    var lead = a;
    var follow = b;
    if (b[0] < a[0]) {
        lead = b;
        follow = a;
    }

    // Continue while there elements in the lead.
    while (lead.len > 0) {
        // Merge largest prefix the lead that is ahead of follow[0].
        const n = std.sort.upperBound(
            PgidType,
            follow[0],
            lead,
            .{},
            lessThanPid,
        );

        std.mem.copyBackwards(PgidType, dst[merged..], lead[0..n]);
        merged += n;
        if (n >= lead.len) {
            break;
        }
        // Swap lead and follow.
        lead = follow;
        follow = lead[n..];
    }

    // Append what's left in follow.
    std.mem.copyBackwards(PgidType, dst[merged..], follow);
}

fn lessThanPid(context: void, lhs: PgidType, rhs: PgidType) bool {
    _ = context;
    return lhs < rhs;
}

fn cmp(context: void, lhs: PgidType, rhs: PgidType) std.math.Order {
    _ = context;
    if (lhs > rhs) {
        return std.math.Order.gt;
    } else if (lhs < rhs) {
        return std.math.Order.lt;
    } else {
        return std.math.Order.eq;
    }
}

fn sortPgIds(ids: PgIds) void {
    std.mem.sort(PgidType, ids, .{}, cmp);
}

test "page struct" {
    const page = Page{ .id = 1, .flags = 2, .count = 1, .overflow = 1 };
    _ = page;
    const slice = std.testing.allocator.alloc(u8, page_size) catch unreachable;
    defer std.testing.allocator.free(slice);
    @memset(slice, 0);
    // Meta
    {
        std.debug.print("Test Meta\n", .{});
        var page1 = Page.init(slice);
        var page2 = Page.init(slice);
        page2.id = 200;
        page2.flags = @as(u16, @intFromEnum(PageFlage.leaf));
        page2.meta().*.version = 1;
        page2.meta().*.version = 2;
        try std.testing.expectEqual(page1.meta().*.version, 2);
        try std.testing.expectEqual(page1.meta().*.version, page2.meta().*.version);
        try std.testing.expectEqual(page1.flags, page2.flags);
    }
    @memset(slice, 0);
    // Branch
    {
        std.debug.print("Test Branch\n", .{});
        const pageRef = Page.init(slice);
        pageRef.count = 10;
        for (0..10) |i| {
            const branch = pageRef.branchPageElement(i);
            branch.?.pos = @as(u32, @intCast(i * 9 + 300));
            branch.?.kSize = @as(u32, @intCast(i + 1));
            branch.?.pgid = @as(u64, i + 2);
        }
        const branchElements = pageRef.branchPageElements().?;
        std.debug.print("{}\n", .{branchElements.len});
        for (0..10) |i| {
            const branch = pageRef.branchPageElement(i);
            std.debug.print("{} {}\n", .{ branch.?, branchElements[i] });
        }
    }
    @memset(slice, 0);
    std.debug.print("-------------------------------page size {}-----------\n", .{page_size});
    // Leaf
    {
        const pageRef = Page.init(slice);
        pageRef.count = 10;
        const n: usize = @as(usize, pageRef.count);
        var leftPos = pageRef.getDataPtrInt();
        var rightPos: usize = @intFromPtr(slice.ptr) + page_size - 1;
        // store it
        for (0..n) |i| {
            const leaf = pageRef.leafPageElement(i).?;
            leaf.flags = 0;
            leaf.kSize = @as(u32, @intCast(i + 1));
            leaf.vSize = @as(u32, @intCast(i + 2));
            const kvSize = leaf.kSize + leaf.vSize;

            leaf.pos = @as(u32, @intCast(rightPos - leftPos)) - kvSize;
            std.debug.assert(leaf.pos == pageRef.leafPageElement(i).?.pos);
            leftPos += LeafPageElement.headerSize();
            rightPos -= @as(usize, kvSize);
            const key = leaf.key();
            for (0..key.len) |index| {
                key[index] = @as(u8, @intCast(index + 'B'));
            }
            const value = leaf.value();
            for (0..value.len) |index| {
                value[index] = @as(u8, @intCast(index + 'E'));
            }
        }
        const leafElements = pageRef.leafPageElements();
        for (leafElements.?) |leaf| {
            std.debug.print("{?}\n", .{leaf});
        }
    }
}

pub fn main() !void {
    std.testing.run();
}
