const std = @import("std");
const page = @import("./page.zig");
const bucket = @import("./bucket.zig");
const tx = @import("./tx.zig");
const util = @import("./util.zig");

/// Represents an in-memory, deserialized page.
pub const Node = struct {
    bucekt: ?*bucket.Bucket,
    isLeaf: bool,
    unbalance: bool,
    spilled: bool,
    key: ?[]u8,
    pgid: page.pgid_type,
    parent: ?*Node,
    children: ?[]?*Node,
    // The inodes for this node. If the node is a leaf, the inodes are key/value pairs.
    // If the node is a branch, the inodes are child page ids. The inodes are kept in sorted order.
    // The inodes are reference to the inodes in the page, so the inodes should not be free.
    inodes: INodes,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init() *Self {
        return undefined;
    }

    // free the node memory
    pub fn deinit(self: *Self) void {
        // Just free the inodes, the inode are reference of page, so the inode should not be free.
        self.allocator.free(self.inodes);
    }

    // Returns the top-level node this node is attached to.
    fn root(self: *Self) ?*Node {
        if (self.parent) |parent| {
            return parent.*.root();
        } else {
            return self;
        }
    }

    // Returns the minimum number of inodes this node should have.
    fn minKeys(self: *const Self) usize {
        if (self.isLeaf) {
            return 1;
        }
        return 2;
    }

    // Returns the size of the node after serialization.
    fn size(self: *const Self) usize {
        var sz = page.Page.header_size;
        const elsz = self.pageElementSize();
        for (self.inodes.?) |item| {
            const vLen: usize = if (item.value) |value| value.len else 0;
            sz += elsz + item.key.?.len + vLen;
        }

        return sz;
    }

    // Returns true if the node is less than a given size.
    // This is an optimization to avoid calculating a large node when we only need
    // to know if it fits inside a certain page size.
    fn sizeLessThan(self: *const Self, v: usize) bool {
        // page header size
        var sz = page.Page.HeaderSize;
        // page element size
        const elsz = self.pageElementSize();
        for (self.inodes.?) |item| {
            // page element size + key size + value size, if the page is branch node, the value is pgid
            const vLen: usize = if (item.value) |value| value.len else 0;
            sz += elsz + item.key.?.len + vLen;
            if (sz >= v) {
                return false;
            }
        }
        return true;
    }

    // Returns the size of each page element based on the type of node.
    fn pageElementSize(self: *const Self) usize {
        if (self.is_leaf) {
            return page.leafPageElementSize;
        } else {
            return page.branchPageElementSize;
        }
    }

    /// Returns the child node at a given index.
    fn childAt(self: *const Self, _: usize) ?*Node {
        if (self.isLeaf) {
            @panic("invalid childAt call on a leaf node");
        } else {
            // TODO
            return null;
        }
    }

    // Returns the index of a given child node.
    fn childIndex(self: *Self, child: *Node) usize {
        const index = std.sort.binarySearch(*Node, child, self.inodes.?, {}, findFn) catch unreachable;
        return index;
    }

    // Returns the number of children.
    fn numChildren(self: *Self) usize {
        return self.inodes.?.len;
    }

    /// Returns the next node with the same parent.
    fn nextSlibling(self: *Self) ?*Self {
        if (self.parent == null) {
            return null;
        }
        // Get child index.
        //      parent
        //        |
        //       [c1, c3, self, c4, c5]
        // c4 is the next slibling of self
        // index = 2
        const index = self.parent.?.childIndex(self);
        // Get the rightest node
        // right = 4
        const right = self.parent.?.numChildren() - 1;
        if (index >= right) {
            return null;
        }
        // Self is the righest node, so the next slibling is index + 1 = 3 = c4
        return self.parent.?.childAt(index + 1);
    }

    /// Returns the previous node with the same parent.
    fn preSlibling(self: *Self) ?*Self {
        if (self.parent == null) {
            return null;
        }
        const index = self.parent.?.childIndex(self);
        // Self is the leftest node, so the previous slibling is null
        if (index == 0) {
            return null;
        }
        // Self is the middle node, so the previous slibling is index - 1
        return self.parent.?.childAt(index - 1);
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

    /// Read initializes the node from a page.
    fn read(self: *Self, p: *page.Page) void {
        self.pgid = p.id;
        self.isLeaf = p.isLeaf();
        self.inodes = self.allocator.alloc(*INode, @intCast(p.count)) catch unreachable;

        for (0..@as(usize, p.count)) |i| {
            var inode = self.inodes.?[i];
            inode = INode.init(self.allocator, 0, 0, null, null);
            if (self.isLeaf) {
                const elem = p.leafPageElement(i);
                inode.flags = elem.flags;
                inode.key = elem.key();
                inode.value = elem.value();
            } else {
                const elem = p.branchPageElement(i);
                inode.pgid = elem.?.pgid;
                inode.key = elem.?.key();
            }
            std.debug.assert(inode.key.?.len > 0);
        }

        // Save first key so we can find the node in the parent when we spill.
        if (self.inodes.?.len > 0) {
            self.key = self.inodes.?[0].key;
            std.debug.assert(self.key.?.len > 0);
        } else {
            self.key = null;
        }
    }

    /// Writes the items into one or more pages.
    fn write(self: *Self, p: *page.Page) void {
        // Initialize page.
        if (self.pgid == 0) {
            p.flags |= page.intFromFlags(page.PageFlage.leaf);
        } else {
            p.flags |= page.intFromFlags(page.PageFlage.branch);
        }

        if (self.inodes.?.len >= 0xFFFF) {
            @panic("inode overflow");
        }

        // Stop here if there are no items to write.
        if (self.count == 0) {
            return;
        }

        // Loop pver each inode and write it to the page.
        for (self.inodes.?, 0..) |inode, i| {
            std.debug.assert(inode.key.?.len > 0);
            // Write the page element.
            if (self.isLeaf) {
                const elem = p.leafPageElement(i);
                elem.pgid = @as(u32, p.getDataPtrInt() - @intFromPtr(elem));
                elem.flags = inode.flags;
                elem.kSize = inode.key.?.len;
                elem.vSize = inode.value.?.len;
            } else {
                const elem = p.branchPageElement(i);
                elem.pgid = @as(u32, p.getDataPtrInt() - @intFromPtr(elem));
                elem.kSize = inode.key.?.len;
                std.debug.assert(inode.pgid != elem.pgid);
            }
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

/// Represents a node on a page.
const INode = struct {
    flags: u32,
    // If the pgid is 0 then it's a leaf node, if it's greater than 0 then it's a branch node, and the value is the pgid of the child.
    pgid: page.PgidType,
    // The key is the first key in the inodes. the key is reference to the key in the inodes that bytes slice is reference to the key in the page.
    // so the key should not be free. it will be free when the page is free.
    key: ?[]u8,
    // If the value is nil then it's a branch node.
    // same as key, the value is reference to the value in the inodes that bytes slice is reference to the value in the page.
    value: ?[]u8,
    const Self = @This();

    /// Initializes a node.
    fn init(allocator: std.mem.Allocator, flags: u32, pgid: page.PgidType, key: ?[]u8, value: ?[]u8) *Self {
        const inode = allocator.create(INode) catch unreachable;
        inode.*.flags = flags;
        inode.*.pgid = pgid;
        inode.*.key = key;
        inode.*.value = value;
        return inode;
    }

    /// Frees an inode.
    fn destroy(self: *Self, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }
};

const INodes = ?[]*INode;

fn freeInodes(allocator: std.mem.Allocator, inodes: INodes) void {
    for (inodes.?) |inode| {
        allocator.free(inode.key.?);
        if (inode.value != null) {
            allocator.free(inode.value.?);
        }
        inode.destroy(allocator);
    }
}

fn sortINodes(inodes: INodes) void {
    std.mem.sort(*INode, inodes.?, {}, sortFn);
}

fn sortFn(_: void, a: *INode, b: *INode) bool {
    const order = util.cmpBytes(a.key.?, b.key.?);
    return order == std.math.Order.lt or order == std.math.Order.eq;
}

fn findFn(_: void, a: *INode, b: *INode) std.math.Order {
    return util.cmpBytes(a.key.?, b.key.?);
}

test "node" {
    const n: usize = 14;
    var inodes = std.testing.allocator.alloc(*INode, n) catch unreachable;
    defer std.testing.allocator.free(inodes);
    defer freeInodes(std.testing.allocator, inodes);
    // random a number
    var rng = std.rand.DefaultPrng.init(10);
    for (0..n) |i| {
        const key = std.testing.allocator.alloc(u8, 10) catch unreachable;
        rng.fill(key);
        const inode = INode.init(std.testing.allocator, 0x10, 0x20, key, null);
        inodes[i] = inode;
    }
    sortINodes(inodes);

    for (inodes) |inode| {
        std.debug.print("\n{any}\n", .{inode.key.?});
    }
}
