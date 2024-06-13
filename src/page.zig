const std = @import("std");
const db = @import("./db.zig");

const min_keys_page: usize = 2;

pub const branchPageElementSize = BranchPageElement.headerSize;
pub const leafPageElementSize = LeafPageElement.header_size;

pub const PageFlage = enum(u8) {
    branch = 0x01,
    leaf = 0x02,
    meta = 0x04,
    free_list = 0x10,
};

pub const bucket_leaf_flag = 0x01;

pub const PgidType = u64;

pub const PgIds = []PgidType;

pub const page_size: usize = 1024;

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
    pub const HeaderSize = @sizeOf(@This());

    pub fn init(slice: []u8) *Page {
        const ptr: *Page = @ptrCast(@alignCast(slice));
        return ptr;
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
        const elements = self.branchPageElements();
        if (elements == null) {
            return null;
        }
        return &elements.?[index];
    }

    // Retrives a list of leaf nodes.
    pub fn branchPageElements(self: *Self) ?[]BranchPageElement {
        if (self.count == 0) {
            return null;
        }
        const ptrInt: usize = self.getDataPtrInt();
        const tPtr: *[]BranchPageElement = @ptrFromInt(ptrInt);
        return tPtr.*[0..self.count];
    }

    // Retrives the leaf node by index.
    pub fn leafPageElement(self: *Self, index: usize) *LeafPageElement {
        const elements = self.leafPageElements();
        if (elements == null) {
            return null;
        }
        return elements[index];
    }

    // Retrives a list of leaf nodes.
    pub fn leafPageElements(self: *Self) ?[]LeafPageElement {
        if (self.count == 0) {
            return null;
        }

        const ptr = @intFromPtr(self);
        const elements: [self.count]LeafPageElement = @ptrFromInt(ptr + self.count * LeafPageElement.headerSize);
        return elements;
    }

    pub fn getDataPtrInt(self: *Self) usize {
        const ptr = @intFromPtr(self);
        return ptr + Self.HeaderSize;
    }

    pub fn getDataSlice(self: *Self) []u8 {
        const ptr = self.getDataPtrInt();
        const slice: [self.count]u8 = @ptrFromInt(ptr);
        return slice;
    }
};

pub const BranchPageElement = packed struct {
    pos: u32,
    kSize: u32,
    pgid: PgidType,

    const Self = @This();
    pub const headerSize = @sizeOf(BranchPageElement);

    /// Returns a byte slice of the node key.
    pub fn key(self: *Self) []u8 {
        const ptr = @intFromPtr(self);
        const keyPtr: *[]u8 = @ptrFromInt(ptr + @as(usize, self.pos));
        return keyPtr.*[0..@as(usize, self.kSize)];
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
    pub const headerSize = @sizeOf(LeafPageElement);

    // Return a byte slice of the node key.
    pub fn key(self: *Self) []u8 {
        const ptr = @intFromPtr(self);
        const keyPtr: [self.kSize]u8 = @ptrFromInt(ptr + self.pos);
        return keyPtr;
    }

    // Returns a byte slice of the node value.
    pub fn value(self: *Self) []u8 {
        const ptr = @intFromPtr(self);
        const valuePtr: [self.vSize]u8 = @ptrFromInt(ptr + self.pos + self.kSize);
        return valuePtr;
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

    for (slice) |*p| {
        p.* = 0;
    }
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

    // Branch
    // {
    //     std.debug.print("Test Branch\n", .{});
    //     var page1 = Page.init(slice);
    //     var page2 = Page.init(slice);
    //     page2.id = 200;
    //     page2.flags = @as(u16, @intFromEnum(PageFlage.branch));
    //     page2.count = 1;
    //     page2.branch_page_element(0).?.*.pos = 1;
    //     page2.branch_page_element(0).?.*.kSize = 2;
    //     page2.branch_page_element(0).?.*.pgid = 3;
    //     try std.testing.expectEqual(page1.branch_page_element(0).?.*.pos, 0);
    //     try std.testing.expectEqual(page1.branch_page_element(0).?.*.kSize, 0);
    //     try std.testing.expectEqual(page1.branch_page_element(0).?.*.pgid, 0);
    //     try std.testing.expectEqual(page1.flags, page2.flags);
    //     try std.testing.expectEqual(page1.count, page2.count);
    // }
}

pub fn main() !void {
    std.testing.run();
}
