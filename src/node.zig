const std = @import("std");
const page = @import("page.zig");
const bucket = @import("bucket.zig");
const tx = @import("tx.zig");
const util = @import("util.zig");
const consts = @import("consts.zig");
const PgidType = consts.PgidType;
const Page = page.Page;
const assert = @import("assert.zig").assert;
const log = std.log.scoped(.BoltNode);
// const LoggerContext = struct {
//     id: u64,
//     pgid: u64,
//     isLeaf: bool,
//     ptr: u64,
// };
// const Logger = @import("log.zig").Logger(LoggerContext).init(.BoltNode);

/// Represents an in-memory, deserialized page.
pub const Node = struct {
    bucket: ?*bucket.Bucket = null, // If the node is top root node, the key is null, but here ?
    isLeaf: bool = false,
    unbalance: bool = false,
    spilled: bool = false,
    key: ?[]const u8 = null, // The key is reference to the key in the inodes that bytes slice is reference to the key in the page. It is the first key (min)
    pgid: PgidType = 0, // The node's page id
    parent: ?*Node = null, // At memory
    children: Nodes, // the is a soft reference to the children of the node, so the children should not be free.
    // The inodes for this node. If the node is a leaf, the inodes are key/value pairs.
    // If the node is a branch, the inodes are child page ids. The inodes are kept in sorted order.
    // The inodes are reference to the inodes in the page, so the inodes should not be free.
    inodes: INodes,
    isFreed: bool = false,
    // The id of the node.
    id: u64 = 0,

    // allocator: std.mem.Allocator,
    arenaAllocator: std.heap.ArenaAllocator,

    const Self = @This();

    /// init a node with allocator.
    pub fn init(allocator: std.mem.Allocator) *Self {
        var arenaAllocator = std.heap.ArenaAllocator.init(allocator);
        const self = arenaAllocator.allocator().create(Self) catch unreachable;
        const id = std.crypto.random.int(u64);
        self.* = .{
            .arenaAllocator = arenaAllocator,
            .children = std.array_list.Managed(*Node).init(allocator),
            .inodes = std.array_list.Managed(INode).init(allocator),
            .id = id,
        };
        return self;
    }

    /// free the node memory and destroy the node
    pub fn deinitAndDestroy(self: *Self) void {
        self.deinit();
    }

    /// free the node memory when the node is removed
    pub fn deinitWhenRemoved(self: *Self) void {
        if (self.isFreed) {
            return;
        }
        self.isFreed = true;
        // TODO, if the key is equal the first key of the node, we should be not free the key
        // because the key is reference to the key in the inodes that bytes slice is reference to the key in the page.
        if (!self.isFirstKeyReference()) {
            log.info("free key, ptr: 0x{x}", .{@intFromPtr(self.key.?.ptr)});
        }

        self.inodes.clearAndFree();
        assert(self.inodes.items.len == 0, "the inodes is not empty, id: {d}, pgid: {d}, ptr: 0x{x}", .{ self.id, self.pgid, self.nodePtrInt() });
        self.children.clearAndFree();
        assert(self.children.items.len == 0, "the children is not empty, id: {d}, pgid: {d}, ptr: 0x{x}", .{ self.id, self.pgid, self.nodePtrInt() });
    }

    /// free the node memory
    pub fn deinit(self: *Self) void {
        if (self.isFreed) {
            return;
        }
        assert(self.isFreed == false, "the node is already freed", .{});
        self.isFreed = true;
        // The is a inline node, so we should free the inline node memory
        // Just free the inodes, the inode are reference of page, so the inode should not be free.
        for (0..self.inodes.items.len) |i| {
            self.inodes.items[i].deinit(null);
        }

        // if (self.key) |key| {
        //     std.log.info("free key, ptr: 0x{x}", .{@intFromPtr(key.ptr)});
        //     self.allocator.free(key);
        // }
        self.key = null;

        self.inodes.clearAndFree();
        assert(self.inodes.items.len == 0, "the inodes is not empty, id: {d}, pgid: {d}, ptr: 0x{x}", .{ self.id, self.pgid, self.nodePtrInt() });
        self.children.clearAndFree();
        assert(self.children.items.len == 0, "the children is not empty, id: {d}, pgid: {d}, ptr: 0x{x}", .{ self.id, self.pgid, self.nodePtrInt() });
        self.arenaAllocator.deinit();
    }

    /// Returns the top-level node this node is attached to.
    pub fn root(self: *Self) ?*Node {
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
    /// *Note*: the oldKey, newKey will be stored in the node, so the oldKey, newKey will not be freed
    /// the pgid is the page id of the new node, the new node is the child node of the current node
    /// *Note*: the oldKey, newKey life is move to the node, so we should manage the life by ourselves
    pub fn put(self: *Self, oldKey: []const u8, newKey: []const u8, value: ?[]u8, pgid: PgidType, flags: u32) *INode {
        if (pgid > self.bucket.?.tx.?.meta.pgid) {
            assert(false, "pgid ({}) above hight water mark ({})", .{ pgid, self.bucket.?.tx.?.meta.pgid });
        } else if (oldKey.len <= 0) {
            assert(false, "put: zero-length old key", .{});
        } else if (newKey.len <= 0) {
            assert(false, "put: zero-length new key", .{});
        }
        // Find insertion index.
        const index = std.sort.lowerBound(INode, self.inodes.items, oldKey, INode.lowerBoundFn);
        const exact = (index < self.inodes.items.len and std.mem.eql(u8, oldKey, self.inodes.items[index].getKey().?));
        if (!exact) {
            const insertINode = INode.init(0, 0, null, null);
            self.inodes.insert(index, insertINode) catch unreachable;
        }
        const inodeRef = &self.inodes.items[index];
        inodeRef.*.flags = flags;
        inodeRef.*.pgid = pgid;
        if (!exact) {
            inodeRef.key = newKey;
        } else {
            if (inodeRef.key != null and inodeRef.isNew) {
                // std.log.info("free old key, id: {d}, key: 0x{x}", .{ inodeRef.id, @intFromPtr(inodeRef.key.?.ptr) });
                inodeRef.key = null;
            }
            inodeRef.key = newKey;

            // Free old value.
            if (inodeRef.value != null and inodeRef.isNew) {
                if (value != null) {
                    assert(inodeRef.value.?.ptr != value.?.ptr, "the value is null", .{});
                }
                inodeRef.value = null;
            }
        }
        inodeRef.value = value;
        inodeRef.isNew = true; // the inode is new inserted
        assert(inodeRef.key.?.len > 0, "put: zero-length inode key", .{});
        // std.log.info("ptr: 0x{x}, id: {d}, succeed to put key: {s}, len: {d}, vLen:{d}, before count: {d}", .{ self.nodePtrInt(), self.id, inodeRef.key.?, inodeRef.key.?.len, vLen, self.inodes.items.len });
        return inodeRef;
    }

    /// Removes a key from the node.
    pub fn del(self: *Self, key: []const u8) ?usize {
        // std.log.debug("del key: {any} at node: {d}", .{ key, self.pgid });
        // Find index of key.
        const indexRef = self.searchInodes(key);
        var inode = self.inodes.orderedRemove(indexRef.index);
        assert(indexRef.exact, "the key is not found, key: {s}, index: {d}, node len: {d}, node key: {s}", .{ key, indexRef.index, self.inodes.items.len, inode.key.? });
        // free the inode
        inode.deinit(self.arenaAllocator.allocator());
        if (self.inodes.items.len == 0) {
            const keyPtr = if (self.key) |k| @intFromPtr(k.ptr) else 0;
            std.log.info("the node is empty, id: {d}, pgid: {d}, ptr: 0x{x}, keyPtr: 0x{x}", .{ self.id, self.pgid, self.nodePtrInt(), keyPtr });
        }
        // Mark the node as needing rebalancing.
        self.unbalance = true;
        // self.printKeysString();
        return indexRef.index;
    }

    /// Read initializes the node from a page.
    /// *Note*: here, we set the node is old node.
    pub fn read(self: *Self, p: *page.Page) void {
        self.pgid = p.id;
        self.isLeaf = p.isLeaf();
        self.inodes.resize(0) catch unreachable;
        // std.log.info("read page, pgid: {}, isLeaf: {}, count:{}, overflow:{}", .{ p.id, self.isLeaf, p.count, p.overflow });
        for (0..@as(usize, p.count)) |i| {
            var inode = INode.init(0, 0, null, null);
            if (self.isLeaf) {
                const elem = p.leafPageElementRef(i).?;
                inode.flags = elem.flags;
                inode.isNew = false;
                inode.key = elem.key();
                inode.value = elem.value();
                // std.log.info("read leaf element: {any}", .{elem.*});
            } else {
                const elem = p.branchPageElementRef(i).?;
                inode.pgid = elem.pgid;
                inode.isNew = false;
                inode.key = elem.key();
            }
            //std.log.info("read element, index: {d}, inode.id:{d}, inode.flags:{any}, inode.pgid:{d}, isLeaf: {}, key: {s}", .{ i, inode.id, inode.flags, inode.pgid, self.isLeaf, inode.key orelse "empty" });
            assert(inode.key.?.len > 0, "key is null", .{});
            self.inodes.append(inode) catch unreachable;
        }

        // Save first key so we can find the node in the parent when we spill.
        if (self.inodes.items.len > 0) {
            self.key = self.arenaAllocator.allocator().dupe(u8, self.inodes.items[0].key.?) catch unreachable;
            assert(self.key.?.len > 0, "key is null, id: {d}, ptr: 0x{x}", .{ self.id, self.nodePtrInt() });
        } else {
            // Note: if the node is the top node, it is a empty bucket without name, so it key is empty
            self.key = null;
        }
        if (@import("builtin").is_test) {
            const firstKey = if (self.inodes.items.len > 0) self.inodes.items[0].key.? else "";
            const lastkey = if (self.inodes.items.len > 0) self.inodes.getLast().key.? else "";
            std.log.info("read node from page(ptr: 0x{x}, id: {d}, flags: {any}), key[{any}->{any}] countElement: {d}, overflow: {d}, pageSize: {d}", .{ @intFromPtr(p), p.id, consts.toFlags(p.flags), firstKey, lastkey, p.count, p.overflow, p.asSlice().len });
        }
    }

    /// Writes the items into one or more pages.
    /// return the number of bytes written (not include the page header)
    pub fn write(self: *Self, p: *page.Page) usize {
        // Initialize page.
        if (self.isLeaf) {
            p.flags |= consts.intFromFlags(.leaf);
        } else {
            p.flags |= consts.intFromFlags(.branch);
        }
        assert(p.count == 0, "the page count is not zero, pgid: {d}, count: {d}", .{ p.id, p.count });
        assert(self.inodes.items.len < 0xFFFF, "inode({}) overflow: {} > {}", .{ p.id, self.inodes.items.len, 0xFFFF });
        p.count = @as(u16, @intCast(self.inodes.items.len));
        // Stop here if there are no items to write.
        if (p.count == 0) {
            // std.log.info("no inode need write, pgid={}, flags: {any}", .{ p.id, consts.toFlags(p.flags) });
            return 0;
        }
        // |e1|e2|e3|b1|b2|b3|
        // Loop over each item and write it to the page.
        // cals the data start position, the data start position is the page header size + the page element size * the number of inodes
        const dataStart = Page.headerSize() + self.pageElementSize() * self.inodes.items.len;
        var b = p.asSlice()[dataStart..];
        // assert(b.len >= (self.pageElementSize() * self.inodes.items.len), "the page({d}) is too small to write all inodes, data size: {d}, need size: {d}", .{ p.id, dataSlice.len, self.pageElementSize() * self.inodes.items.len });
        var written: usize = 0;
        if (@import("builtin").is_test) {
            const firstKey = if (self.inodes.items.len > 0) self.inodes.items[0].key.? else "";
            const lastkey = if (self.inodes.items.len > 0) self.inodes.getLast().key.? else "";
            std.log.info("write node(nodeid: {d}) into page(ptr: 0x{x}, id: {d}, flags: {any}), key[{any}->{any}] countElement: {d}, overflow: {d}, pageSize: {d}, bSize: {d}", .{ self.pgid, @intFromPtr(p), p.id, consts.toFlags(p.flags), firstKey, lastkey, p.count, p.overflow, p.asSlice().len, b.len });
        }
        // Loop pver each inode and write it to the page.
        for (self.inodes.items, 0..) |inode, i| {
            assert(inode.key.?.len > 0, "write: zero-length inode key", .{});
            // Write the page element.
            if (self.isLeaf) {
                const elem = p.leafPageElement(i).?;
                elem.pos = @as(u32, @intCast(@intFromPtr(b.ptr) - @intFromPtr(elem)));
                elem.flags = inode.flags;
                elem.kSize = @as(u32, @intCast(inode.key.?.len));
                elem.vSize = @as(u32, @intCast(inode.value.?.len));
                written += page.LeafPageElement.headerSize();
            } else {
                const elem = p.branchPageElement(i).?;
                elem.pos = @as(u32, @intCast(@intFromPtr(b.ptr) - @intFromPtr(elem)));
                elem.kSize = @as(u32, @intCast(inode.key.?.len));
                elem.pgid = inode.pgid;
                written += page.BranchPageElement.headerSize();
                assert(inode.pgid == elem.pgid, "write: circulay dependency occuerd", .{});
            }
            // If the length of key+value is larger than the max allocation size
            // then we need to reallocate the byte array pointer
            //
            // See: https://github.com/boltdb/bolt/pull/335
            const kLen = inode.key.?.len;
            const vLen: usize = if (inode.value) |value| value.len else 0;
            written += kLen + vLen;
            // Write data for the element to the end of the page.
            std.mem.copyForwards(u8, b[0..kLen], inode.key.?);
            b = b[kLen..];
            if (inode.value) |value| {
                std.mem.copyForwards(u8, b[0..vLen], value);
                b = b[vLen..];
            }
        }
        // const deump = p.asSlice();
        // std.log.info("deump: {any}", .{deump});
        // DEBUG ONLY: n.deump()
        return written;
    }

    /// Split breaks up a node into multiple smaller nodes, If appropriate.
    /// This should only be called from the spill() function.
    pub fn split(self: *Self, _pageSize: usize) std.array_list.Managed(*Node) {
        var nodes = std.array_list.Managed(*Node).init(self.arenaAllocator.allocator());
        var curNode = self;
        while (true) {
            // Split node into two.
            const a, const b = curNode.splitTwo(_pageSize);
            nodes.append(a.?) catch unreachable;
            // a.?.printKeysString();
            // If we can't split then exit the loop.
            if (b == null) {
                // log.info("the node is not need to split, id: {d}, key: {s}, hasParent: {}", .{ curNode.pgid, curNode.key orelse "empty", a.?.parent != null });
                break;
            } else {
                // const aFirstKey = a.?.inodes.items[0].key.?;
                // const aLastKey = a.?.inodes.items[a.?.inodes.items.len - 1].key.?;
                // const bFirstKey = b.?.inodes.items[0].key.?;
                // const bLastKey = b.?.inodes.items[b.?.inodes.items.len - 1].key.?;
                // log.info("the node[ a=>[len:{d}, key:{any}-{any}], b=>[len:{d}, key:{any}-{any}]] is need to split, isLeaf: {}", .{ a.?.inodes.items.len, aFirstKey, aLastKey, b.?.inodes.items.len, bFirstKey, bLastKey, a.?.isLeaf });
            }

            // Set node to be so it gets split on the next function.
            curNode = b.?;
        }

        return nodes;
    }

    // Breaks up a node into two smaller nodes, if approprivate.
    // This should only be called from the split() function.
    fn splitTwo(self: *Self, _pageSize: usize) [2]?*Node {
        // Ignore the split if the page doesn't have a least enough nodes for
        // two pages or if the nodes can fit in a single page.
        if (self.inodes.items.len <= consts.MinKeysPage * 2 or self.sizeLessThan(_pageSize)) {
            return [2]?*Node{ self, null };
        }

        // Determine the threshold before starting a new node.
        const fillPercent = self.bucket.?.fillPercent;
        const threshold = consts.calThreshold(fillPercent, _pageSize);
        // Determin split position and sizes of the two pages.
        const _splitIndex, _ = self.splitIndex(threshold);
        assert(_splitIndex < self.inodes.items.len, "the split index is out of range, index: {}, inodes len: {}", .{ _splitIndex, self.inodes.items.len });
        // std.log.err("split index: {}, threshold: {}, fillPercent: {d}, inodes len: {}", .{ _splitIndex, threshold, fillPercent, self.inodes.items.len });
        // Split node into two separate nodes.
        // if the node is the root node, then create a new node as the parent node
        // and set the current node as the child node
        if (self.parent == null) {
            self.parent = Node.init(self.getAllocator());
            self.parent.?.bucket = self.bucket;
            self.parent.?.children.append(self) catch unreachable; // children also is you!
            // self.bucket.?.tx.?.autoFreeNodes.?.addNode(self.parent.?);
        }

        // Create a new node and add it to the parent.
        const next = Node.init(self.bucket.?.getAllocator());
        next.bucket = self.bucket;
        next.isLeaf = self.isLeaf;
        next.parent = self.parent;
        self.parent.?.children.append(next) catch unreachable;
        next.inodes.ensureTotalCapacity(self.inodes.items.len - _splitIndex) catch unreachable;
        next.inodes.appendSlice(self.inodes.items[_splitIndex..]) catch unreachable;
        assert(next.inodes.items.len == (self.inodes.items.len - _splitIndex), "the next node's inodes length is not equal to the self node's inodes length - the split index", .{});
        // shrink self.inodes to _splitIndex
        self.inodes.resize(_splitIndex) catch unreachable;
        // const firstKey = self.inodes.items[0].key.?;
        // const lastKey = self.inodes.getLast().key.?;
        // std.log.err("split node, self: {s}-{s}, next: {s}-{s}", .{ firstKey, lastKey, next.inodes.items[0].key.?, next.inodes.items[next.inodes.items.len - 1].key.? });
        assert(self.inodes.items.len == _splitIndex, "the self node's inodes length is not equal to the split index", .{});
        // Update the statistics.
        self.bucket.?.tx.?.stats.split += 1;

        assert(self.parent.?.numChildren() == next.parent.?.numChildren(), "the parent node's children count is not equal to the next node's parent node's children count", .{});
        assert(self.parent.? == next.parent.?, "the parent node is not equal to the next node's parent node", .{});
        assert(self.parent.?.bucket.? == next.parent.?.bucket.?, "the parent node's bucket is not equal to the next node's parent node's bucket", .{});
        assert(self.parent.?.bucket.? == self.bucket.?, "the parent node's bucket is not equal to the self node's bucket", .{});

        return [2]?*Node{ self, next };
    }

    /// Finds the position where a page will fill a given threshold.
    /// It returns the index as well as the size of the first page.
    /// This is only be called from split().
    fn splitIndex(self: *Self, threshold: usize) [2]usize {
        var sz = Page.headerSize();
        // Loop until we only have the minimum number of keys required for the second page.
        var i: usize = 0;
        var index: usize = 0;
        while (i < self.inodes.items.len - consts.MinKeysPage) {
            const inode = self.inodes.items[i];
            var elsize = self.pageElementSize() + inode.key.?.len;
            if (inode.value) |value| {
                elsize += value.len;
            }
            // If we have at least the minimum number of keys and adding another
            // node would put us over the threshold then exit and return.
            if (i >= consts.MinKeysPage and (sz + elsize) > threshold) {
                break;
            }
            // Add the element size to the total size.
            index = i;
            sz += elsize;
            i += 1;
        }
        return [2]usize{ index, sz };
    }

    /// Writes the nodes to dirty pages and splits nodes as it goes.
    /// Returns and error if dirty pages cannot be allocated
    pub fn spill(self: *Self) !void {
        if (self.spilled) {
            // log.debug("the node has already spilled, pgid: {d}", .{self.pgid});
            return;
        }
        const pgid = self.pgid;
        if (@import("builtin").is_test) {
            log.info("\t.start spill node: {d}, isLeaf: {}", .{ pgid, self.isLeaf });
            defer log.info("\t.end spill node: {d}", .{pgid});
        }
        const _tx = self.bucket.?.tx.?;
        const _db = _tx.getDB();

        // Spill child nodes first. Child nodes can materialize sibling nodes in
        // the case of split-merge so we cannot use a range loop. We have to check
        // the children size on every loop iteration.
        std.mem.sort(
            *Node,
            self.children.items,
            {},
            struct {
                fn lessFn(_: void, a: *Node, b: *Node) bool {
                    return std.mem.order(u8, a.key.?, b.key.?) == .lt;
                }
            }.lessFn,
        );
        for (0..self.children.items.len) |i| {
            const child = self.children.items[i];
            try child.spill();
        }
        // We no longer need the children list because it's only used for spilling tracking.
        self.children.clearAndFree();

        // Split nodes into approprivate sizes, The first node will always be n.
        const nodes = self.split(_db.pageSize);
        defer nodes.deinit();
        // log.debug("pgid: {d}, nodeid: 0x{x}, nodes size: {d}, key: {s}", .{ self.pgid, self.nodePtrInt(), nodes.items.len, self.key orelse "empty" });
        for (nodes.items) |node| {
            // Add node's page to the freelist if it's not new.
            // (it is the first one, because split node from left to right!)
            if (node.pgid > 0) {
                try _db.freelist.free(_tx.meta.txid, _tx.getPage(node.pgid));
                // reset the pgid to 0, so the node will be a new node.
                node.pgid = 0;
            }
            // Allocate contiguous space for the node.(COW: Copy on Write)
            const allocateSize: usize = node.size() / _db.pageSize + 1;
            const p = try _tx.allocate(allocateSize);
            assert(p.id < _tx.meta.pgid, "pgid ({}) above high water mark ({})", .{ p.id, _tx.meta.pgid });
            node.pgid = p.id;
            const written = node.write(p) + Page.headerSize();
            assert(written == node.size(), "spill: wrote {d} bytes, expected {d} for node {d}", .{ written, node.size(), node.pgid });
            node.spilled = true;

            // Insert into parent inodes
            if (node.parent) |parent| {
                const key: []const u8 = node.key orelse node.inodes.items[0].key.?;
                const oldKey = parent.arenaAllocator.allocator().dupe(u8, key) catch unreachable;
                const newKey = parent.arenaAllocator.allocator().dupe(u8, node.inodes.items[0].key.?) catch unreachable;
                _ = parent.put(oldKey, newKey, null, node.pgid, 0);
                node.key = node.arenaAllocator.allocator().dupe(u8, node.inodes.items[0].key.?) catch unreachable;
                assert(node.key.?.len > 0, "spill: zero-length node key", .{});
                // log.debug("spill a node from parent, parent's pgid: {d}, parent's inodes len: {d}, children len: {d}, node's pgid: {d}, key: {s}", .{ parent.pgid, parent.inodes.items.len, parent.children.items.len, node.pgid, node.key.? });
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
        if (!self.unbalance) {
            return;
        }
        self.unbalance = false;
        // std.log.info("rebalance node: {d}", .{self.pgid});

        // Update statistics.
        self.bucket.?.tx.?.stats.rebalance += 1;

        // Ignore if node is above threshold (25%) and has enough keys.
        const threshold = self.bucket.?.tx.?.db.?.pageSize / 4;
        if (self.size() > threshold and self.inodes.items.len > self.minKeys()) {
            return;
        }

        // Root node has special handling.
        if (self.parent == null) {
            // std.log.debug("the node parent is null, so rebalance root node: {d}\n", .{self.pgid});
            // If root node is a branch and only has one node then collapse it.
            if (!self.isLeaf and self.inodes.items.len == 1) {
                // Move root's child up.
                const child: *Self = self.bucket.?.node(self.inodes.items[0].pgid, self);
                self.isLeaf = child.isLeaf;
                {
                    self.inodes.clearAndFree();
                    self.inodes.appendSlice(child.inodes.items) catch unreachable;
                    self.children.clearAndFree();
                    self.children.appendSlice(child.children.items) catch unreachable;
                    assert(self.inodes.items.len == self.inodes.items.len, "the inodes length is not equal to the child inodes length", .{});
                    assert(self.children.items.len == child.children.items.len, "the children length is not equal to the child children length", .{});
                }

                // Reparent all child nodes being moved.
                // TODO why not skip the first key
                for (self.inodes.items) |inode| {
                    if (self.bucket.?.nodes.?.get(inode.pgid)) |_child| {
                        _child.parent = self;
                    }
                }

                // Remove old child. because the node also be stored in the node's children,
                // so we should remove the child directly and recycle it.
                child.parent = null;
                const exist = self.bucket.?.nodes.?.remove(child.pgid);
                assert(exist, "rebalance: node({d}) not found in nodes map", .{child.pgid});
                child.free();
                child.deinitWhenRemoved();
                // std.log.debug("only one child, merge it, pgid: {d}, ptr: 0x{x}, key={any}, isLeaf: {}, inodes len: {d}", .{ self.pgid, self.nodePtrInt(), self.key orelse "empty", self.isLeaf, self.inodes.items.len });
                return;
            }
            // std.log.debug("nothing need to rebalance at root: {d}, key={s}, isLeaf: {}, inodes len: {d}", .{ self.pgid, self.key orelse "empty", self.isLeaf, self.inodes.items.len });
            return;
        }
        // If node has no keys then just remove it.
        if (self.numChildren() == 0) {
            // remove self from parent reference.
            const index = self.parent.?.del(self.key.?);
            assert(index != null, "the node({d}) not found in parent inodes", .{self.pgid});
            self.parent.?.removeChild(self);
            // remove self from node.
            const exists = self.bucket.?.nodes.?.remove(self.pgid);
            assert(exists, "rebalance: node({d}) not found in nodes map", .{self.pgid});
            // free reference page to db.
            self.free();
            // continue reblance parent.
            self.parent.?.rebalance();
            self.deinitAndDestroy();
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
                if (self.bucket.?.nodes.?.get(inode.pgid)) |_child| {
                    _child.parent.?.removeChild(_child);
                    _child.parent = self;
                    _child.parent.?.children.append(_child) catch unreachable;
                }
            }

            // Copy over inodes from target and remove target.
            self.inodes.appendSlice(target.?.inodes.items) catch unreachable;
            const index = self.parent.?.del(target.?.key.?);
            assert(index != null, "rebalance: node({d}) not found in parent inodes", .{target.?.pgid});
            _ = self.bucket.?.nodes.?.remove(target.?.pgid);
            target.?.free();
        } else {
            // Reparent all child nodes being moved.
            for (self.inodes.items) |inode| {
                if (self.bucket.?.nodes.?.get(inode.pgid)) |_child| {
                    _child.parent.?.removeChild(_child);
                    _child.parent = target;
                    _child.parent.?.children.append(_child) catch unreachable;
                }
            }

            // Copy over inodes to target and remove node.
            target.?.inodes.appendSlice(self.inodes.items) catch unreachable;
            const index = self.parent.?.del(self.key.?);
            assert(index != null, "rebalance: node({d}) not found in parent inodes", .{self.pgid});
            self.parent.?.removeChild(self);
            _ = self.bucket.?.nodes.?.remove(self.pgid);
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
                const childNode = self.children.orderedRemove(i);
                assert(childNode.nodePtrInt() == target.nodePtrInt(), "the child is not in the children list", .{});
                return;
            }
        }
    }

    // Causes the node to copy all its inode key/value references to heap memory.
    // This is required when `mmap` is reallocated so *inodes* are not pointing to stale data.
    pub fn dereference(self: *Self) void {
        if (self.key != null) {
            const cpKey = self.arenaAllocator.allocator().dupe(u8, self.key.?) catch unreachable;
            self.key = cpKey;
            assert(self.pgid == 0 or self.key != null and self.key.?.len > 0, "deference: zero-length node key on existing node", .{});
        }

        for (self.inodes.items) |*inode| {
            const newKey = self.arenaAllocator.allocator().dupe(u8, inode.key.?) catch unreachable;
            inode.key = newKey;
            assert(inode.key != null and inode.key.?.len > 0, "deference: zero-length inode key on existing node", .{});
            // If the value is not null
            if (inode.value) |value| {
                const newValue = self.arenaAllocator.allocator().dupe(u8, value) catch unreachable;
                inode.value = newValue;
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
            self.pgid = 0;
        }
    }

    /// get the pointer of the node
    pub fn nodePtrInt(self: *const Self) usize {
        return @intFromPtr(self);
    }

    /// get the allocator of the node
    pub fn getAllocator(self: *Self) std.mem.Allocator {
        return self.arenaAllocator.allocator();
    }

    /// search the key in the inodes, if found, return the index and exact, if not found, return the position of the first element that is greater than the key
    pub fn searchInodes(self: *const Self, key: []const u8) struct { index: usize, exact: bool } {
        var left: usize = 0;
        var right: usize = self.inodes.items.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            const cmp = std.mem.order(u8, key, self.inodes.items[mid].key.?);

            switch (cmp) {
                .eq => return .{ .index = mid, .exact = true },
                .lt => right = mid,
                .gt => left = mid + 1,
            }
        }
        return .{ .index = left, .exact = false };
    }

    fn safeCheck(self: *const Self) void {
        defer log.debug("pass safe check node: {d}, inodes len: {d}", .{ self.pgid, self.inodes.items.len });
        for (0..self.inodes.items.len) |i| {
            if (i > 0) {
                const left = self.inodes.items[i - 1].key.?;
                const right = self.inodes.items[i].key.?;
                assert(std.mem.order(u8, right, left) == .gt, "the inodes is not in order, left: {s}, right: {s}", .{ left, right });
            }
        }
        const isRoot = self.parent == null;
        if (!isRoot) {
            if (self.key) |_key| {
                const iKey = self.inodes.items[0].key.?;
                assert(std.mem.order(u8, _key, iKey) == .eq, "the key is not equal to the self key, key: {s}, iKey: {s}", .{ _key, iKey });
            }
        }
    }

    /// Returns true if the key is reference to the first key of the node.
    fn isFirstKeyReference(self: *const Self) bool {
        if (self.key == null or self.inodes.items.len == 0) {
            return false;
        }
        return self.inodes.items[0].key.?.ptr == self.key.?.ptr;
    }

    fn printKeysString(self: *Self) void {
        std.log.debug("--->>id:{}, pgid:{}, inodes len: {d}<<--", .{ self.id, self.pgid, self.inodes.items.len });
        var keyArray = std.array_list.Managed([]const u8).init(self.arenaAllocator.allocator());
        defer keyArray.deinit();
        for (self.inodes.items) |inode| {
            const key = inode.key.?;
            keyArray.append(key) catch unreachable;
        }
        std.log.debug("{any}", .{keyArray.items});
        std.log.debug("--->>end<<--", .{});
    }
};

/// Represents a node on a page.
pub const INode = struct {
    flags: u32 = 0,
    // If the pgid is 0 then it's a leaf node, if it's greater than 0 then it's a branch node, and the value is the pgid of the child.
    pgid: PgidType = 0,
    // The key is the first key in the inodes. the key is reference to the key in the inodes that bytes slice is reference to the key in the page.
    // so the key should not be free. it will be free when the page is free.
    // 1: if the node is a branch node, then the key is the first key in the inodes.
    // 2: if the node is a leaf node, then the key is null(TODO).
    // 3: if the node is root node, then the key is null.
    key: ?[]const u8 = null,
    // If the value is nil then it's a branch node.
    // same as key, the value is reference to the value in the inodes that bytes slice is reference to the value in the page.
    value: ?[]u8 = null,

    // if the inode is new, then the inode will be added to the inodes list.
    isNew: bool = true,

    /// The id of the inode.
    id: u64 = 0,

    const Self = @This();

    /// Initializes a node.
    pub fn init(flags: u32, pgid: PgidType, key: ?[]const u8, value: ?[]u8) Self {
        const id = std.crypto.random.int(u64);
        // std.log.debug("create a inode, inode id: {d}, key: {s}, value: {s}", .{ id, key orelse "empty", value orelse "empty" });
        return .{ .flags = flags, .pgid = pgid, .key = key, .value = value, .id = id };
    }

    /// keyLen returns the length of the key.
    pub fn keyLen(self: Self) usize {
        const sz = self.key orelse return 0;
        return sz.len();
    }

    /// valueLen returns the length of the value.
    pub fn valueLen(self: Self) usize {
        const sz = self.value orelse return 0;
        return sz.len();
    }

    /// setKey sets the key of the inode, and set the inode to new.
    pub fn setKey(self: *Self, key: []const u8) void {
        self.key = key;
        self.isNew = true;
    }

    /// getKey returns the key of the inode.
    pub fn getKey(self: Self) ?[]const u8 {
        return self.key;
    }

    /// getValue returns the value of the inode.
    pub fn getValue(self: Self) ?[]u8 {
        return self.value;
    }

    /// deinit the inode
    pub fn deinit(self: *Self, _: ?std.mem.Allocator) void {
        if (!self.isNew) {
            return;
        }
        // if (self.key) |key| {
        //     _ = key; // autofix
        //     // allocator.free(key);
        //     // self.key = null;
        // }
        // // TODO: Print the value address.(Eg: the value is a inline bucket value)
        // if (self.value) |value| {
        //     _ = value; // autofix
        //     //std.log.debug("free value: 0x{x}", .{@intFromPtr(value.ptr)});
        //     // allocator.free(value);
        //     self.value = null;
        // }
    }

    /// lower bound of the key
    pub fn lowerBoundFn(context: []const u8, item: @This()) std.math.Order {
        return std.mem.order(u8, context, item.getKey().?);
    }
};

const INodes = std.array_list.Managed(INode);

const Nodes = std.array_list.Managed(*Node);
