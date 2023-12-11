const std = @import("std");
const db = @import("./db.zig");

const min_keys_page: usize = 2;

pub const PageFlage = enum(u8) {
    branch = 0x01,
    leaf = 0x02,
    meta = 0x04,
    free_list = 0x10,
};

pub const bucket_leaf_flag = 0x01;

pub const pgid_type = u64;

pub const page_size = 4096;

pub const Page = packed struct {
    const Self = @This();
    id: pgid_type,
    flags: u16,
    count: u16,
    overflow: u32,

    // the size of this, but why align(4)?
    pub const header_size = @sizeOf(@This());

    pub fn typ(self: *Self) []u8 {
        if (self.flags & PageFlage.branch != 0) {
            return "branch";
        } else if (self.flags & PageFlage.leaf != 0) {
            return "leaf";
        } else if (self.flags & PageFlage.meta != 0) {
            return "meta";
        } else if (self.flags & PageFlage.free_list != 0) {
            return "freelist";
        } else {
            return "unkown";
        }
    }

    // Returns a pointer to the metadata section of the page.
    pub fn meta(self: *Self) *db.Meta {
        const ptr = @intFromPtr(self);
        const _meta = @as(db.Meta, @ptrFromInt(ptr));
        return _meta;
    }

    // Retrives the branch node by index.
    pub fn branch_page_element(self: *Self, index: usize) *BranchPageElement {
        const elements = self.branch_page_elements();
        if (elements == null) {
            return null;
        }
        return elements[index];
    }

    // Retrives a list of leaf nodes.
    pub fn branch_page_elements(self: *Self) ?[]BranchPageElement {
        if (self.count == 0) {
            return null;
        }
        const ptr = @intFromPtr(self);
        const elements = @as([self.count]BranchPageElement, @ptrFromInt(ptr + self.count * BranchPageElement.header_size));
        return elements;
    }

    // Retrives the leaf node by index.
    pub fn leaf_page_element(self: *Self, index: usize) *LeafPageElement {
        const elements = self.leaf_page_elements();
        if (elements == null) {
            return null;
        }
        return elements[index];
    }

    // Retrives th leaf node by index.
    pub fn leaf_page_elements(self: *Self) ?[]LeafPageElement {
        if (self.count == 0) {
            return null;
        }

        const ptr = @intFromPtr(self);
        const elements = @as([self.count]LeafPageElement, @ptrFromInt(ptr + self.count * LeafPageElement.header_size));
        return elements;
    }
};

pub const BranchPageElement = packed struct {
    pos: u32,
    k_size: u32,
    pgid: pgid_type,

    const Self = @This();
    pub const header_size = @sizeOf(BranchPageElement);
};

pub const LeafPageElement = packed struct {
    flags: u32,
    pos: u32,
    k_size: u32,
    v_size: u32,

    const Self = @This();
    pub const header_size = @sizeOf(LeafPageElement);

    // Return a byte slice of the node key.
    pub fn key(self: *Self) []u8 {
        const ptr = @intFromPtr(self);
        const key_ptr = @as([self.k_size]u8, @ptrFromInt(ptr + self.pos));
        return key_ptr;
    }

    // Returns a byte slice of the node value.
    pub fn value(self: *Self) []u8 {
        const ptr = @intFromPtr(self);
        const value_ptr = @as([self.v_size]u8, @ptrFromInt(ptr + self.pos + self.k_size));
        return value_ptr;
    }
};

pub const PageInfo = packed struct {
    id: isize,
    typ: []u8,
    count: isize,
    over_flow_count: isize,
};

test "Test compiled" {
    //    std.testing.expectEqual(1, 2);
    std.debug.print("Hello test", .{});
    std.log.info("Hello World", .{});
}

test "pointer" {
    const page = Page{ .id = 1, .flags = 2, .count = 1, .overflow = 1 };
    _ = page;
    var slice = try std.heap.page_allocator.alloc(u8, page_size);
    defer std.heap.page_allocator.free(slice);
    for (slice) |*value| {
        value.* = 0;
    }

    std.debug.print("type of: {} \n", .{@TypeOf(slice)});

    var page1 = std.mem.bytesAsValue(Page, slice[0..Page.header_size]);

    std.debug.print(">> {}\n", .{page1});
    std.debug.print(">> size of: {}\n", .{@sizeOf(Page)});
}

pub fn main() !void {
    std.testing.run();
}
