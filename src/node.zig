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

    //fn child_at(self: *Self, _index: usize) ?*Node {
    //
    //  if (self.is_leaf) {
    //     unreachable;
    //}

    //TODO

    //}
};

const INode = struct {
    flags: u32,
    pgid: page.pgid_type,
    key: ?[]u8,
    value: ?[]u8,
};

const INodes = ?[]INode;
