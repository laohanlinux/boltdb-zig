const std = @import("std");
const page = @import("./page.zig");
const bucket = @import("./bucket.zig");
const tx = @import("./tx.zig");
const util = @import("./util.zig");
const assert = @import("./assert.zig").assert;

/// Represents an in-memory, deserialized page.
pub const Node = struct {
    bucket: ?*bucket.Bucket,
    isLeaf: bool,
    unbalance: bool,
    spilled: bool,
    key: ?[]u8, // The key is reference to the key in the inodes that bytes slice is reference to the key in the page. It is the first key (min)
    pgid: page.pgid_type,
    parent: ?*Node, // At memory
    children: Nodes, // the is a soft reference to the children of the node, so the children should not be free.
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
        const data = p.getDataSlice();
        const index: usize = 0;
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
                assert(inode.pgid != elem.?.pgid, "write: circulay dependency occuerd", {});
            }

            const kLen = inode.key.?.len;
            // Write data for the element to the end of the page.
            std.mem.copyForwards(u8, data[index..], inode.key.?);
            index += kLen;
            if (inode.value) |value| {
                std.mem.copyForwards(u8, data[index], value);
                index += value.len;
            }
        }

        // DEBUG ONLY: n.deump()
    }

    /// Attempts to combine the node with sibling nodes if the node fill
    /// size is below a threshold or if there are not enough keys.
    fn rebalance(self: *Self) void {
        if (!self.unbalance) {
            return;
        }
        self.unbalance = false;

        // Update statistics.
        self.bucket.?.tx.?.stats.rebalance += 1;

        // Ignore if node is above threshold (25%) and has enough keys.
        const threshold = self.bucket.?.tx.?.db.pageSize / 4;
        if (self.size() > threshold and self.count >= self.minKeys()) {
            return;
        }

        // Root node has special handling.
        if (self.parent == null) {
            // If root node is a branch and only has one node then collapse it.
            if (!self.isLeaf and self.inodes.?.len == 1) {
                // Move root's child up.
                const child: *Self = self.bucket.?.node(self.inodes.?[0].pgid, self);
                self.isLeaf = child.isLeaf;
                self.inodes = child.inodes;
                self.children = child.children;

                // Reparent all child nodes being moved.
                // TODO why not skip the first key
                for (self.inodes.?) |inode| {
                    if (self.bucket.?.nodes.get(inode.pgid)) |_child| {
                        _child.parent = self;
                    }
                }

                // Remove old child. because the node also be stored in the node's children,
                // so we should remove the child directly and recycle it.
                child.parent = null;
                _ = self.bucket.?.nodes.remove(child.pgid);
                child.free();
                return;
            }
            std.debug.print("nothing need to rebalance at root: {d}\n", .{self.pgid});
            return;
        }

        // If node has no keys then just remove it.
        if (self.numChildren() == 0) {
            self.parent.?.del(self.key);
            self.parent.?.removeChild(self);
            const exists = self.bucket.?.nodes.remove(self.pgid);
            assert(exists, "rebalance: node({d}) not found in nodes map", .{self.pgid});
            self.free();
            self.parent.rebalance();
            return;
        }

        assert(self.parent.?.numChildren() > 1, "parent must have at least 2 children", .{});

        // Destination node is right sibling if idx == 0, otherwise left sibling.
        var target: *Node = undefined;
        const useNextSlibling = (self.parent.?.children(self) == 0);
        if (useNextSlibling) {
            target = self.nextSlibling();
        } else {
            target = self.preSlibling();
        }

        // If both this node and the target node are too small then merge them.
        if (useNextSlibling) {
            // Reparent all child nodes being moved.
            for (self.inodes.?) |inode| {
                // 难道有些数据没在bucket.nodes里面？
                if (self.bucket.?.nodes.get(inode.pgid)) |_child| {
                    _child.parent.removeChild(_child);
                    _child.parent = self;
                    //_child.parent.?.children = append(_child.parent.?.children, _child);
                }
            }
        } else {}
    }

    // fn removeChild(self: *Self, target: *Node) void {
    //     for (self.children.?, 0..) |child, i| {
    //         if (child == target) {
    //             self.children.?[i] = null;
    //             return;
    //         }
    //     }
    // }

    // Causes the node to copy all its inode key/value references to heap memory.
    // This is required when `mmap` is reallocated so *inodes* are not pointing to stale data.
    fn dereference(self: *Self) void {
        // TODO: meybe we should not free the key, because it was referennce same to first inode.
        if (self.key != null) {
            const _key = self.allocator.alloc(u8, self.key.?.len) catch unreachable;
            std.mem.copyForwards(u8, _key, self.key.?);
            self.key = _key;
            assert(self.pgid == 0 or self.key != null and self.key.?.len > 0, "deference: zero-length node key on existing node", .{});
        }

        for (self.inodes.?) |inode| {
            const _key = self.allocator.alloc(u8, inode.key.?.len) catch unreachable;
            std.mem.copyForwards(u8, _key, inode.key.?);
            inode.key = _key;
            assert(inode.key != null and inode.key.?.len > 0, "deference: zero-length inode key on existing node", {});
            // If the value is not null
            if (inode.value) |value| {
                const _value = self.allocator.alloc(u8, value.len) catch unreachable;
                std.mem.copyForwards(u8, _value, value);
                inode.value = _value;
                assert(inode.value != null and inode.value.?.len > 0, "deference: zero-length inode value on existing node", {});
            }
        }

        // Recursively dereference children.
        for (self.children.items) |child| {
            child.dereference();
        }

        // Update statistics.
        self.bucket.?.tx.?.stats.nodeDeref += 1;
    }

    /// adds the node's underlying page to the freelist.
    fn free(self: *Self) void {
        if (self.pgid != 0) {
            self.bucket.?.tx.?.db.freelist.free(self.bucket.?.tx.?.meta.txid, self.bucket.?.tx.?.page(self.pgid));
            // TODO why reset the node
            self.pgid = 0;
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

const Nodes = std.ArrayList(*Node);

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
