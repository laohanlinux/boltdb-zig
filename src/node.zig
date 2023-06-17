const std = @import("std");
const page = @import("./page.zig");
const bucket = @import("./bucket.zig");
const tx = @import("./tx.zig");

pub const Node = struct {
    bucekt: ?*bucket.Bucket,
    is_leaf: bool,
    unbalance: bool,
    spilled: bool,
    key: ?[]u8,
    pgid: page.pgid_type,
    parent: ?*Node,
    children: ?[]?*Node,
    inodes: INodes,

    const Self = @This();

    // Returns the top-level node this node is attached to.
    fn root(self: *Self) void {
        if (self.parent == null) {
            return;
        }

        return self.parent.?.root();
    }

    // Returns the minimum number of inodes this node should have.
    fn min_keys(self: *Self) usize {
        if (self.is_leaf) {
            return 1;
        }

        return 2;
    }

    // Returns the size of the node after serialization.
    fn size(self: *Self) usize {
        var sz = page.Page.header_size;
        const elsz = self.page_element_size();
        for (self.inodes) |item| {
            sz += elsz + item.key.len() + item.value.len();
        }

        return sz;
    }

    // Returns true if the node is less than a given size.
    // This is an optimization to avoid calculating a large node when we only need
    // to know if it fits inside a certain page size.
    fn size_less_than(self: *Self, v: usize) bool {
        var sz = page.Page.header_size;
        const elsz = self.page_element_size();
        for (self.inodes) |item| {
            sz += elsz + item.key.len() + item.value.len();
            if (sz >= v) {
                return false;
            }
        }
        return true;
    }

    // Returns the size of each page element based on the type of node.
    fn page_element_size(self: *Self) usize {
        if (self.is_leaf) {
            return page.leafPageElementSize;
        } else {
            return page.branchPageElementSize;
        }
    }

    fn child_at(self: *Self, _index: usize) ?*Node {
        std.debug.print("{} {}", .{ self, _index });
        unreachable;
    }

    // Returns the index of a given child node.
    fn child_index(self: *Self, child: *Node) usize {
        const inode = std.sort.binarySearch(INode, child.key, self.inodes, {}, INode.order);
        return try inode;
    }

    // Returns the number of children.
    fn num_children(self: *Self) usize {
        return self.inodes.?.len();
    }

    // Returns the next node with the same parent.
    fn next_slibling(self: *Self) ?*Self {
        if (self.parent == null) {
            return null;
        }
        // Get child index.
        //      parent
        //        |
        //       [c1, c3, self, c4, c5]
        const index = self.parent.?.child_index(self);
        const right = self.parent.?.num_children() - 1;
        // Self is the righest node
        if (index >= right) {
            return null;
        }
        return self.parent.?.child_at(index + 1);
    }

    // Returns the previous node with the same parent.
    fn pre_slibling_slibling(self: *Self) ?*Self {
        if (self.parent == null) {
            return null;
        }
        const index = self.parent.?.child_index(self);
        if (index == 0) {
            return null;
        }

        return self.parent.?.child_at(index - 1);
    }

    // Inserts a key/value.
    fn put(self: *Self, oldKey: []u8, newKey: []u8, value: []u8, pgid: page.pgid_type, flags: u32) void {
        _ = flags;
        _ = value;
        if (pgid > self.bucket.tx.meta.pgid) {
            unreachable;
        } else if (oldKey.len() <= 0) {
            unreachable;
        } else if (newKey.len() <= 0) {
            unreachable;
        }
    }

    // Add the node's undering page to the freelist.
    fn free(self: *Self) void {
        if (self.pgid != 0) {
            // free bucket
            // TODO
            // self.bucekt.?.tx.db.freelist.free()
        }
    }
};

const INode = struct {
    flags: u32,
    pgid: page.pgid_type,
    key: ?[]u8,
    value: ?[]u8,

    const Self = @This();

    fn order(context: void, key: []u8, mid_item: @This()) std.math.Order {
        _ = context;

        if (key < mid_item.key) {
            return .lt;
        }

        if (key > mid_item.key) {
            return .gt;
        }

        return .eq;
    }
};

const INodes = ?[]INode;

test "node" {}
