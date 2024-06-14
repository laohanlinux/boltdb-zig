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
    pgid: page.PgidType,
    parent: ?*Node, // At memory
    children: Nodes, // the is a soft reference to the children of the node, so the children should not be free.
    // The inodes for this node. If the node is a leaf, the inodes are key/value pairs.
    // If the node is a branch, the inodes are child page ids. The inodes are kept in sorted order.
    // The inodes are reference to the inodes in the page, so the inodes should not be free.
    inodes: INodes,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) *Self {
        var self: *Self = allocator.create(Self) catch unreachable;
        self.allocator = allocator;
        self.inodes = std.ArrayList(INode).init(self.allocator);
        self.children = std.ArrayList(*Node).init(self.allocator);
        self.bucket = null;
        self.isLeaf = false;
        self.unbalance = false;
        self.spilled = false;
        self.key = null;
        self.pgid = 0;
        self.parent = null;
        return self;
    }

    // free the node memory
    pub fn deinit(self: *Self) void {
        // Just free the inodes, the inode are reference of page, so the inode should not be free.
        self.inodes.deinit();
        self.children.deinit();
        self.allocator.destroy(self);
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
        var sz = page.Page.headerSize();
        const elsz = self.pageElementSize();
        for (self.inodes.items) |item| {
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
        var sz = page.Page.headerSize();
        // page element size
        const elsz = self.pageElementSize();
        for (self.inodes.items) |item| {
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
        if (self.isLeaf) {
            return page.LeafPageElement.headerSize();
        } else {
            return page.BranchPageElement.headerSize();
        }
    }

    /// Returns the child node at a given index.
    fn childAt(self: *Self, index: usize) ?*Node {
        if (self.isLeaf) {
            @panic("invalid childAt call on a leaf node");
        }
        return self.bucket.?.node(self.inodes.items[index].pgid, self);
    }

    // Returns the index of a given child node.
    fn childIndex(self: *Self, child: *Node) usize {
        const keyINode = INode.init(0, 0, child.key.?, null);
        const index = std.sort.lowerBound(INode, keyINode, self.inodes.items, {}, lessThanFn);
        return index;
    }

    // Returns the number of children.
    // TODO ?
    fn numChildren(self: *Self) usize {
        return self.inodes.items.len;
    }

    /// Returns the next node with the same parent.
    fn nextSlibling(self: *Self) ?*Self {
        if (self.parent == null) {
            return null;
        }
        const index = self.parent.?.childIndex(self);
        const right = self.parent.?.numChildren() - 1;
        if (index >= right) { // If the node is parent's righest child, can not find a right brother
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
    pub fn put(self: *Self, oldKey: []u8, newKey: []u8, value: []u8, pgid: page.PgidType, flags: u32) void {
        if (pgid > self.bucket.?.tx.?.meta.pgid) {
            assert(false, "pgid ({}) above hight water mark ({})", .{ pgid, self.bucket.?.tx.?.meta.pgid });
        } else if (oldKey.len <= 0) {
            assert(false, "put: zero-length old key", .{});
        } else if (newKey.len <= 0) {
            assert(false, "put: zero-length new key", .{});
        }
        // Find insertion index.
        const keyINode = INode.init(0, 0, oldKey, null);
        const index = std.sort.lowerBound(INode, keyINode, self.inodes.items, {}, lessThanFn);
        const insertINode = INode.init(flags, pgid, newKey, value);
        self.inodes.insert(index, insertINode) catch unreachable;
        assert(insertINode.key.?.len > 0, "put: zero-length inode key", .{});
    }

    // Removes a key from the node.
    pub fn del(self: *Self, key: []u8) void {
        // Find index of key.
        const keyINode = INode.init(0, 0, key, null);
        const index = std.sort.binarySearch(INode, keyINode, self.inodes.items, {}, findFn) orelse return;
        _ = self.inodes.orderedRemove(index);
        // Mark the node as needing rebalancing.
        self.unbalance = true;
    }

    // For binary search
    fn createKeyINode(self: *const Self) INode {
        return INode.init(0, 0, self.key.?, null);
    }

    /// Read initializes the node from a page.
    pub fn read(self: *Self, p: *page.Page) void {
        self.pgid = p.id;
        self.isLeaf = p.isLeaf();
        self.inodes.resize(@intCast(p.count)) catch unreachable;
        for (0..@as(usize, p.count)) |i| {
            var inode = INode.init(0, 0, null, null);
            if (self.isLeaf) {
                const elem = p.leafPageElementPtr(i);
                inode.flags = elem.flags;
                inode.key = elem.key();
                inode.value = elem.value();
            } else {
                const elem = p.branchPageElementPtr(i);
                inode.pgid = elem.pgid;
                inode.key = elem.key();
            }
            std.debug.assert(inode.key.?.len > 0);
            self.inodes.append(inode) catch unreachable;
        }

        // TODO
        // Save first key so we can find the node in the parent when we spill.
        if (self.inodes.items.len > 0) {
            self.key = self.inodes.items[0].key.?;
            std.debug.assert(self.key.?.len > 0);
        } else {
            self.key = null;
            @panic("It should be not hanppen");
        }
    }

    /// Writes the items into one or more pages.
    pub fn write(self: *Self, p: *page.Page) void {
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
    fn init(flags: u32, pgid: page.PgidType, key: ?[]u8, value: ?[]u8) Self {
        var inode: Self = undefined;
        inode.flags = flags;
        inode.pgid = pgid;
        inode.key = key;
        inode.value = value;
        return inode;
    }
};

const INodes = std.ArrayList(INode);

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

fn findFn(_: void, a: INode, b: INode) std.math.Order {
    return util.cmpBytes(a.key.?, b.key.?);
}

fn lessThanFn(_: void, a: INode, b: INode) bool {
    const order = util.cmpBytes(a.key.?, b.key.?);
    return order == std.math.Order.lt or order == std.math.Order.eq;
}

test "node" {
    const node = Node.init(std.testing.allocator);
    defer node.deinit();
    _ = node.root();
    _ = node.minKeys();
    const nodeSize = node.size();
    const lessThan = node.sizeLessThan(20);
    //_ = node.childAt(0);
    //_ = node.childIndex(node);
    //_ = node.numChildren();
    _ = node.nextSlibling();
    _ = node.preSlibling();
    //  var oldKey = [_]u8{0};
    //  var newKey = [_]u8{0};
    //   var value = [_]u8{ 1, 2, 3 };
    //   node.put(oldKey[0..], newKey[0..], value[0..], 29, 0);
    // node.del("");
    std.debug.print("node size: {}, less: {}\n", .{ nodeSize, lessThan });

    //   const n: usize = 14;
    //   var inodes = std.testing.allocator.alloc(*INode, n) catch unreachable;
    //   defer std.testing.allocator.free(inodes);
    //   defer freeInodes(std.testing.allocator, inodes);
    //   // random a number
    //   var rng = std.rand.DefaultPrng.init(10);
    //   for (0..n) |i| {
    //       const key = std.testing.allocator.alloc(u8, 10) catch unreachable;
    //       rng.fill(key);
    //       const inode = INode.init(0x10, 0x20, key, null);
    //       inodes[i] = inode;
    //   }
    //   sortINodes(inodes);
    //
    //   for (inodes) |inode| {
    //       std.debug.print("\n{any}\n", .{inode.key.?});
    //   }
}
