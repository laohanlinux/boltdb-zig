const std = @import("std");
const page = @import("page.zig");
const bucket = @import("bucket.zig");
const tx = @import("tx.zig");
const util = @import("util.zig");
const consts = @import("consts.zig");
const PgidType = consts.PgidType;
const assert = @import("assert.zig").assert;

/// Represents an in-memory, deserialized page.
pub const Node = struct {
    bucket: ?*bucket.Bucket, // If the node is top root node, the key is null, but here ?
    isLeaf: bool,
    unbalance: bool,
    spilled: bool,
    key: ?[]const u8, // The key is reference to the key in the inodes that bytes slice is reference to the key in the page. It is the first key (min)
    pgid: PgidType, // The node's page id
    parent: ?*Node, // At memory
    children: Nodes, // the is a soft reference to the children of the node, so the children should not be free.
    // The inodes for this node. If the node is a leaf, the inodes are key/value pairs.
    // If the node is a branch, the inodes are child page ids. The inodes are kept in sorted order.
    // The inodes are reference to the inodes in the page, so the inodes should not be free.
    inodes: INodes,
    allocator: std.mem.Allocator,

    const Self = @This();

    /// init a node with allocator.
    pub fn init(allocator: std.mem.Allocator) *Self {
        const self = allocator.create(Self) catch unreachable;
        self.allocator = allocator;
        self.bucket = null;
        self.isLeaf = false;
        self.unbalance = false;
        self.spilled = false;
        self.key = null;
        self.pgid = 0;
        self.parent = null;
        self.children = std.ArrayList(*Node).init(self.allocator);
        self.inodes = std.ArrayList(INode).init(self.allocator);
        return self;
    }

    /// free the node memory
    pub fn deinit(self: *Self) void {
        // Just free the inodes, the inode are reference of page, so the inode should not be free.
        for (self.inodes.items) |inode| {
            inode.deinit(self.allocator);
        }
        self.inodes.deinit();
        self.children.deinit();
        self.allocator.destroy(self);
    }

    /// Returns the top-level node this node is attached to.
    pub fn root(self: *Self) ?*Node {
        if (self.parent) |parent| {
            return parent.*.root();
        } else {
            return self;
        }
    }

    /// Returns the minimum number of inodes this node should have.
    fn minKeys(self: *const Self) usize {
        if (self.isLeaf) {
            return 1;
        }
        return 2;
    }

    /// Returns the size of the node after serialization.
    pub fn size(self: *const Self) usize {
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
    pub fn childAt(self: *Self, index: usize) ?*Node {
        assert(!self.isLeaf, "invalid childAt call on a leaf", .{});
        return self.bucket.?.node(self.inodes.items[index].pgid, self);
    }

    // Returns the index of a given child node.
    fn childIndex(self: *Self, child: *Node) usize {
        const index = std.sort.lowerBound(INode, self.inodes.items, child.key.?, INode.lowerBoundFn);
        return index;
    }

    // Returns the number of children.
    fn numChildren(self: *Self) usize {
        return self.inodes.items.len;
    }

    // Returns the next node with the same parent.
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

    // Returns the previous node with the same parent.
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

    /// Inserts a key/value.
    pub fn put(self: *Self, oldKey: []const u8, newKey: []const u8, value: ?[]u8, pgid: PgidType, flags: u32) void {
        if (pgid > self.bucket.?.tx.?.meta.pgid) {
            assert(false, "pgid ({}) above hight water mark ({})", .{ pgid, self.bucket.?.tx.?.meta.pgid });
        } else if (oldKey.len <= 0) {
            assert(false, "put: zero-length old key", .{});
        } else if (newKey.len <= 0) {
            assert(false, "put: zero-length new key", .{});
        }
        // Find insertion index.
        const index = std.sort.lowerBound(INode, self.inodes.items, oldKey, INode.lowerBoundFn);
        const exact = (index < self.inodes.items.len and std.mem.eql(u8, oldKey, self.inodes.items[index].key.?));
        if (!exact) {
            // not found, allocate previous a new memory
            const insertINode = INode.init(0, 0, null, null);
            self.inodes.insert(index, insertINode) catch unreachable;
        }
        const inodeRef = &self.inodes.items[index];
        inodeRef.*.flags = flags;
        inodeRef.*.pgid = pgid;
        if (!exact) {
            inodeRef.key = newKey;
        } else {
            if (!std.mem.eql(u8, newKey, inodeRef.key.?)) {
                // free
                self.allocator.free(inodeRef.key.?);
                inodeRef.key = newKey;
            }
            // Free old value.
            if (inodeRef.isNew and inodeRef.value != null) {
                self.allocator.free(inodeRef.value.?);
            }
        }
        inodeRef.value = value;
        inodeRef.isNew = true;
        assert(inodeRef.key.?.len > 0, "put: zero-length inode key", .{});
    }

    /// Removes a key from the node.
    pub fn del(self: *Self, key: []const u8) void {
        // Find index of key.
        const index = std.sort.binarySearch(INode, self.inodes.items, key, INode.lowerBoundFn) orelse return;
        _ = self.inodes.orderedRemove(index);
        // Mark the node as needing rebalancing.
        self.unbalance = true;
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
            assert(inode.key.?.len > 0, "key is null", .{});
            self.inodes.append(inode) catch unreachable;
        }

        // Save first key so we can find the node in the parent when we spill.
        if (self.inodes.items.len > 0) {
            self.key = self.inodes.items[0].key.?;
            assert(self.key.?.len > 0, "key is null", .{});
        } else {
            // Note: if the node is the top node, it is a empty bucket without name, so it key is empty
            self.key = null;
        }
    }

    /// Writes the items into one or more pages.
    pub fn write(self: *Self, p: *page.Page) void {
        defer std.log.info("succeed to write node into page({})", .{p.id});
        // Initialize page.
        if (self.isLeaf) {
            p.flags |= consts.intFromFlags(.leaf);
        } else {
            p.flags |= consts.intFromFlags(.branch);
        }

        assert(self.inodes.items.len < 0xFFFF, "inode overflow: {} (pgid={})", .{ self.inodes.items.len, p.id });
        p.count = @as(u16, @intCast(self.inodes.items.len));
        // Stop here if there are no items to write.
        if (p.count == 0) {
            std.log.info("no inode need write, pid={}", .{p.id});
            return;
        }
        // |e1|e2|e3|b1|b2|b3|
        // Loop over each item and write it to the page.
        var b = p.getDataSlice()[self.pageElementSize() * self.inodes.items.len ..];
        // Loop pver each inode and write it to the page.
        for (self.inodes.items, 0..) |inode, i| {
            // std.log.debug("read element: {}, {}", .{ i, @intFromPtr(b.ptr) });
            assert(inode.key.?.len > 0, "write: zero-length inode key", .{});
            // Write the page element.
            if (self.isLeaf) {
                const elem = p.leafPageElement(i).?;
                elem.pos = @as(u32, @intCast(@intFromPtr(b.ptr) - @intFromPtr(elem)));
                elem.flags = inode.flags;
                elem.kSize = @as(u32, @intCast(inode.key.?.len));
                elem.vSize = @as(u32, @intCast(inode.value.?.len));
            } else {
                const elem = p.branchPageElement(i).?;
                elem.pos = @as(u32, @intCast(@intFromPtr(b.ptr) - @intFromPtr(elem)));
                elem.kSize = @as(u32, @intCast(inode.key.?.len));
                elem.pgid = inode.pgid;
                assert(inode.pgid == elem.pgid, "write: circulay dependency occuerd", .{});
            }
            // If the length of key+value is larger than the max allocation size
            // then we need to reallocate the byte array pointer
            //
            // See: https://github.com/boltdb/bolt/pull/335
            const kLen = inode.key.?.len;
            const vLen: usize = if (inode.value) |value| value.len else 0;
            assert(b.len >= (kLen + vLen), "it should be not happen!", .{});

            // Write data for the element to the end of the page.
            std.mem.copyForwards(u8, b[0..kLen], inode.key.?);
            b = b[kLen..];
            if (inode.value) |value| {
                std.mem.copyForwards(u8, b[0..vLen], value);
                b = b[vLen..];
            }
            //std.log.info("inode: {s}, key: {s}", .{ inode.key.?, inode.value.? });
        }

        // DEBUG ONLY: n.deump()
    }

    /// Split breaks up a node into multiple smaller nodes, If appropriate.
    /// This should only be called from the spill() function.
    fn split(self: *Self, _pageSize: usize) []*Node {
        var nodes = std.ArrayList(*Node).init(self.allocator);
        var curNode = self;
        while (true) {
            // Split node into two.
            const a, const b = self.splitTwo(_pageSize);
            nodes.append(a.?) catch unreachable;

            // If we can't split then exit the loop.
            if (b == null) {
                std.log.info("the node is not need to split", .{});
                break;
            } else {
                std.log.info("the node[{any}] is need to split", .{b});
            }

            // Set node to be so it gets split on the next function.
            curNode = b.?;
        }

        return nodes.toOwnedSlice() catch unreachable;
    }

    /// Breaks up a node into two smaller nodes, if approprivate.
    /// This should only be called from the split() function.
    fn splitTwo(self: *Self, _pageSize: usize) [2]?*Node {
        // Ignore the split if the page doesn't have a least enough nodes for
        // two pages or if the nodes can fit in a single page.
        if (self.inodes.items.len <= consts.MinKeysPage * 2 or self.sizeLessThan(_pageSize)) {
            return [2]?*Node{ self, null };
        }

        // Determine the threshold before starting a new node.
        var fillPercent = self.bucket.?.fillPercent;
        if (fillPercent < consts.MinFillPercent) {
            fillPercent = consts.MinFillPercent;
        } else if (fillPercent > consts.MaxFillPercent) {
            fillPercent = consts.MaxFillPercent;
        }

        const fPageSize: f64 = @floatFromInt(_pageSize);
        const threshold = @as(usize, @intFromFloat(fPageSize * fillPercent));

        // Determin split position and sizes of the two pages.
        const _splitIndex, _ = self.splitIndex(threshold);

        // Split node into two separate nodes.
        if (self.parent == null) {
            self.parent = Node.init(self.allocator);
            self.parent.?.bucket = self.bucket;
            self.children.append(self) catch unreachable; // children also is you!
        }

        // Create a new node and add it to the parent.
        const next = Node.init(self.allocator);
        next.bucket = self.bucket;
        next.isLeaf = self.isLeaf;
        next.parent = next.parent;

        // Split inodes across two nodes.
        next.inodes.appendSlice(self.inodes.items[_splitIndex..]) catch unreachable;
        self.inodes.resize(_splitIndex) catch unreachable;

        // Update the statistics.
        self.bucket.?.tx.?.stats.split += 1;

        return [2]?*Node{ self, next };
    }

    /// Finds the position where a page will fill a given threshold.
    /// It returns the index as well as the size of the first page.
    /// This is only be called from split().
    fn splitIndex(self: *Self, threshold: usize) [2]usize {
        var sz = self.bucket.?.tx.?.db.?.pageSize;
        if (self.inodes.items.len <= consts.MinKeysPage) {
            return [2]usize{ 0, sz };
        }
        // Loop until we only have the minmum number of keys required for the second page.
        var inodeIndex: usize = 0;
        for (self.inodes.items, 0..) |inode, i| {
            var elsize = self.pageElementSize() + inode.key.?.len;
            if (inode.value) |value| {
                elsize += value.len;
            }
            inodeIndex = i;

            // If we have at least the minimum number of keys and adding another
            // node would put us over the threshold then exit and return
            if (inodeIndex >= self.inodes.items.len and sz + elsize > threshold) {
                break;
            }

            // Add the element size the total size.
            sz += elsize;
        }

        return [2]usize{ inodeIndex, sz };
    }

    /// Writes the nodes to dirty pages and splits nodes as it goes.
    /// Returns and error if dirty pages cannot be allocated
    pub fn spill(self: *Self) !void {
        if (self.spilled) {
            return;
        }
        const _tx = self.bucket.?.tx.?;
        const _db = _tx.getDB();

        // Spill child nodes first. Child nodes can materialize sibling nodes in
        // the case of split-merge so we cannot use a range loop. We have to check
        // the children size on every loop iteration.
        const lessFn = struct {
            fn less(_: void, a: *Node, b: *Node) bool {
                return std.mem.order(u8, a.key.?, b.key.?) == .lt;
            }
        }.less;
        std.mem.sort(
            *Node,
            self.children.items,
            {},
            lessFn,
        );
        for (self.children.items) |child| {
            try child.spill();
        }
        // We no longer need the children list because it's only used for spilling tracking.
        self.children.clearAndFree();

        // Split nodes into approprivate sizes, The first node will always be n.
        const nodes = self.split(_db.pageSize);
        defer self.allocator.free(nodes);

        for (nodes) |node| {
            // Add node's page to the freelist if it's not new.
            // (it is the first one, because split node from left to right!)
            if (node.pgid > 0) {
                try _db.freelist.free(_tx.meta.txid, _tx.getPage(node.pgid));
                node.pgid = 0;
            }

            // Allocate contiguous space for the node.(COW: Copy on Write)
            const allocateSize: usize = node.size() / _db.pageSize + 1;
            const p = try _tx.allocate(allocateSize);
            assert(p.id < _tx.meta.pgid, "pgid ({}) above high water mark ({})", .{ p.id, _tx.meta.pgid });
            node.pgid = p.id;
            node.write(p);
            node.spilled = true;

            // Insert into parent inodes.
            if (node.parent) |parent| {
                const key = node.key orelse node.inodes.items[0].key.?;
                parent.put(key, node.inodes.items[0].key.?, null, node.pgid, 0);
                node.key = node.inodes.items[0].key;
                assert(node.key.?.len > 0, "spill: zero-length node key", .{});
                std.log.debug("spill a node from parent, pgid: {d}, key: {s}", .{ node.pgid, node.key.? });
            } // so, if the node is the first node, then the node will be the root node, and the node's parent will be null, the node's key also be null>>>

            // Update the statistics.
            _tx.stats.spill += 1;
        }

        // If the root node split and created a new root then we need to spill that
        // as well. We'll clear out the children to make sure it doesn't try to respill.
        if (self.parent != null and self.parent.?.pgid == 0) {
            self.children.clearAndFree();
            return self.parent.?.spill();
        }
    }

    /// Attempts to combine the node with sibling nodes if the node fill
    /// size is below a threshold or if there are not enough keys.
    pub fn rebalance(self: *Self) void {
        std.log.debug("rebalance node: {d}", .{self.pgid});
        if (!self.unbalance) {
            return;
        }
        self.unbalance = false;

        // Update statistics.
        self.bucket.?.tx.?.stats.rebalance += 1;

        // Ignore if node is above threshold (25%) and has enough keys.
        const threshold = self.bucket.?.tx.?.db.?.pageSize / 4;
        if (self.size() > threshold and self.inodes.items.len > self.minKeys()) {
            std.log.debug("the node size is too large, so don't rebalance: {d}", .{self.pgid});
            return;
        }

        // Root node has special handling.
        if (self.parent == null) {
            std.log.debug("the node parent is null, so rebalance root node: {d}\n", .{self.pgid});
            // If root node is a branch and only has one node then collapse it.
            if (!self.isLeaf and self.inodes.items.len == 1) {
                // Move root's child up.
                const child: *Self = self.bucket.?.node(self.inodes.items[0].pgid, self);
                self.isLeaf = child.isLeaf;
                self.inodes = child.inodes;
                self.children = child.children;

                // Reparent all child nodes being moved.
                // TODO why not skip the first key
                for (self.inodes.items) |inode| {
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
        } else {
            std.log.debug("the node parent is not null, so rebalance: {d}, parent: {d}", .{ self.pgid, self.parent.?.pgid });
        }

        // If node has no keys then just remove it.
        if (self.numChildren() == 0) {
            self.parent.?.del(self.key.?);
            self.parent.?.removeChild(self);
            const exists = self.bucket.?.nodes.remove(self.pgid);
            assert(exists, "rebalance: node({d}) not found in nodes map", .{self.pgid});
            self.free();
            self.parent.?.rebalance();
            return;
        }

        assert(self.parent.?.numChildren() > 1, "parent must have at least 2 children", .{});

        // Destination node is right sibling if idx == 0, otherwise left sibling.
        var target: ?*Node = null;
        const useNextSlibling = (self.parent.?.childIndex(self) == 0);
        if (useNextSlibling) {
            target = self.nextSlibling();
        } else {
            target = self.preSlibling();
        }

        // If both this node and the target node are too small then merge them.
        if (useNextSlibling) {
            // Reparent all child nodes being moved.
            for (self.inodes.items) |inode| {
                // 难道有些数据没在bucket.nodes里面？
                if (self.bucket.?.nodes.get(inode.pgid)) |_child| {
                    _child.parent.?.removeChild(_child);
                    _child.parent = self;
                    _child.parent.?.children.append(_child) catch unreachable;
                }
            }

            // Copy over inodes from target and remove target.
            self.inodes.appendSlice(target.?.inodes.items) catch unreachable;
            self.parent.?.del(target.?.key.?);
            _ = self.bucket.?.nodes.remove(target.?.pgid);
            target.?.free();
        } else {
            // Reparent all child nodes being moved.
            for (self.inodes.items) |inode| {
                if (self.bucket.?.nodes.get(inode.pgid)) |_child| {
                    _child.parent.?.removeChild(_child);
                    _child.parent = target;
                    _child.parent.?.children.append(_child) catch unreachable;
                }
            }

            // Copy over inodes to target and remove node.
            target.?.inodes.appendSlice(self.inodes.items) catch unreachable;
            self.parent.?.del(self.key.?);
            self.parent.?.removeChild(self);
            _ = self.bucket.?.nodes.remove(self.pgid);
            self.free();
        }

        // Either this node or the target node was deleted from the parent so rebalance it.
        self.parent.?.rebalance();
    }

    /// Removes a node from the list of in-memory children.
    /// This does not affect the inodes.
    fn removeChild(self: *Self, target: *Node) void {
        for (self.children.items, 0..) |child, i| {
            if (child == target) {
                _ = self.children.orderedRemove(i);
                return;
            }
        }
    }

    // Causes the node to copy all its inode key/value references to heap memory.
    // This is required when `mmap` is reallocated so *inodes* are not pointing to stale data.
    pub fn dereference(self: *Self) void {
        // TODO: meybe we should not free the key, because it was referennce same to first inode.
        if (self.key != null) {
            const _key = self.allocator.alloc(u8, self.key.?.len) catch unreachable;
            std.mem.copyForwards(u8, _key, self.key.?);
            self.key = _key;
            assert(self.pgid == 0 or self.key != null and self.key.?.len > 0, "deference: zero-length node key on existing node", .{});
        }

        for (self.inodes.items) |*inode| {
            var _key = self.allocator.alloc(u8, inode.key.?.len) catch unreachable;
            std.mem.copyForwards(u8, _key, inode.key.?);
            inode.key = _key[0..];
            assert(inode.key != null and inode.key.?.len > 0, "deference: zero-length inode key on existing node", .{});
            // If the value is not null
            if (inode.value) |value| {
                const _value = self.allocator.alloc(u8, value.len) catch unreachable;
                std.mem.copyForwards(u8, _value, value);
                inode.value = _value;
                assert(inode.value != null and inode.value.?.len > 0, "deference: zero-length inode value on existing node", .{});
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
    pub fn free(self: *Self) void {
        if (self.pgid != 0) {
            self.bucket.?.tx.?.db.?.freelist.free(self.bucket.?.tx.?.meta.txid, self.bucket.?.tx.?.getPage(self.pgid)) catch unreachable;
            // TODO why reset the node
            self.pgid = 0;
        }
    }
};

/// Represents a node on a page.
pub const INode = struct {
    flags: u32 = 0,
    // If the pgid is 0 then it's a leaf node, if it's greater than 0 then it's a branch node, and the value is the pgid of the child.
    pgid: PgidType = 0,
    // The key is the first key in the inodes. the key is reference to the key in the inodes that bytes slice is reference to the key in the page.
    // so the key should not be free. it will be free when the page is free.
    key: ?[]const u8 = null,
    // If the value is nil then it's a branch node.
    // same as key, the value is reference to the value in the inodes that bytes slice is reference to the value in the page.
    value: ?[]u8 = null,
    // if the inode is new, then the inode will be free when the page is free.
    // TODO use BufStr instead of the isNew flag
    isNew: bool = false,
    const Self = @This();

    /// Initializes a node.
    pub fn init(flags: u32, pgid: PgidType, key: ?[]const u8, value: ?[]u8) Self {
        return .{ .flags = flags, .pgid = pgid, .key = key, .value = value };
    }

    /// deinit the inode
    pub fn deinit(self: Self, allocator: std.mem.Allocator) void {
        if (self.isNew) { //
            allocator.free(self.key.?);
            allocator.free(self.value.?);
        }
    }

    /// binary search function
    pub fn binarySearchFn(context: []const u8, item: @This()) std.math.Order {
        return std.mem.order(u8, item.key.?, context);
    }

    /// lower bound of the key
    pub fn lowerBoundFn(context: []const u8, item: @This()) std.math.Order {
        return std.mem.order(u8, item.key.?, context);
    }
};

const INodes = std.ArrayList(INode);

const Nodes = std.ArrayList(*Node);

//
// test "node" {
//     const node = Node.init(std.testing.allocator);
//     defer node.deinit();
//     _ = node.root();
//     _ = node.minKeys();
//     const nodeSize = node.size();
//     const lessThan = node.sizeLessThan(20);
//     //_ = node.childAt(0);
//     //_ = node.childIndex(node);
//     //_ = node.numChildren();
//     _ = node.nextSlibling();
//     _ = node.preSlibling();
//
//     const pageSlice = try std.testing.allocator.alloc(u8, page.page_size);
//     defer std.testing.allocator.free(pageSlice);
//     // const pagePtr = page.Page.init(pageSlice);
//     // @memset(pageSlice, 0);
//     //node.read(pagePtr);
//     // node.write(pagePtr);
//     //  var oldKey = [_]u8{0};
//     //  var newKey = [_]u8{0};
//     //   var value = [_]u8{ 1, 2, 3 };
//     //   node.put(oldKey[0..], newKey[0..], value[0..], 29, 0);
//     // node.del("");
//     std.debug.print("node size: {}, less: {}\n", .{ nodeSize, lessThan });
//
//     //   const n: usize = 14;
//     //   var inodes = std.testing.allocator.alloc(*INode, n) catch unreachable;
//     //   defer std.testing.allocator.free(inodes);
//     //   defer freeInodes(std.testing.allocator, inodes);
//     //   // random a number
//     //   var rng = std.rand.DefaultPrng.init(10);
//     //   for (0..n) |i| {
//     //       const key = std.testing.allocator.alloc(u8, 10) catch unreachable;
//     //       rng.fill(key);
//     //       const inode = INode.init(0x10, 0x20, key, null);
//     //       inodes[i] = inode;
//     //   }
//     //   sortINodes(inodes);
//     //
//     //   for (inodes) |inode| {
//     //       std.debug.print("\n{any}\n", .{inode.key.?});
//     //   }
// }
