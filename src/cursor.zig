const std = @import("std");
const Bucket = @import("bucket.zig").Bucket;
const Node = @import("node.zig").Node;
const INode = @import("node.zig").INode;
const findINodeFn = @import("node.zig").findFn;
const lessThanFn = @import("node.zig").lessThanFn;
const page = @import("page.zig");
const util = @import("util.zig");
const assert = util.assert;
const consts = @import("consts.zig");
const PgidType = consts.PgidType;
const Tuple = consts.Tuple;
const KeyPair = consts.KeyPair;
const KeyValueRef = consts.KeyValueRef;
const Error = @import("error.zig").Error;

/// Cursor represents an iterator that can traverse over all key/value pairs in a bucket in sorted order.
/// Cursors see nested buckets with value == nil.
/// Cursors can be obtained from a transaction and are valid as long as the transaction is open.
///
/// Keys and values returned from the cursor are only valid for the life of the transaction.
///
/// Changing data while traversing with a cursor may cause it to be invalidated
/// and return unexpected keys and/or values. You must reposition your cursor
/// after mutating data.
pub const Cursor = struct {
    _bucket: *Bucket,
    stack: std.ArrayList(ElementRef),

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, _bt: *Bucket) Self {
        return Cursor{
            ._bucket = _bt,
            .stack = std.ArrayList(ElementRef).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.stack.deinit();
    }

    /// Returns the bucket that this cursor was created from.
    pub fn bucket(self: *Self) *Bucket {
        return self._bucket;
    }

    /// Moves the cursor to the first item in the bucket and returns its key and value.
    /// If the bucket is empty then a nil key and value are returned.
    /// The returned key and value are only valid for the life of the transaction
    pub fn first(self: *Self) KeyPair {
        assert(self._bucket.tx.?.db != null, "tx closed", .{});
        self.stack.resize(0) catch unreachable;
        const pNode = self._bucket.pageNode(self._bucket._b.?.root);

        const ref = ElementRef{ .p = pNode.page, .node = pNode.node, .index = 0 };
        self.stack.append(ref) catch unreachable;
        _ = self._first();
        // If we land on an empty page then move to the next value.
        // https://github.com/boltdb/bolt/issues/450
        if (self.stack.getLast().count() == 0) {
            std.log.info("the last element count is 0, try to move to the next", .{});
            _ = self._next();
        }
        const keyValueRet = self.keyValue();
        if (keyValueRet.key == null) {
            return KeyPair.init(null, null);
        }
        // Return an error if current value is a bucket.
        if (keyValueRet.flag & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.key.?, null);
        }
        return KeyPair.init(keyValueRet.key.?, keyValueRet.value);
    }

    /// Moves the cursor to the last item in the bucket and returns its key and value.
    /// If the bucket is empty then a nil key and value are returned.
    pub fn last(self: *Self) KeyPair {
        assert(self._bucket.tx.?.db != null, "tx closed", .{});
        self.stack.resize(0) catch unreachable;
        const pNode = self._bucket.pageNode(self._bucket._b.?.root);
        var ref = ElementRef{ .p = pNode.page, .node = pNode.node, .index = 0 };
        if (ref.count() > 0) {
            ref.index = ref.count() - 1;
        }
        self.stack.append(ref) catch unreachable;
        self._last();
        const keyValueRet = self.keyValue();
        if (keyValueRet.key == null) {
            return KeyPair.init(null, null);
        }
        // Return an error if current value is a bucket.
        if (keyValueRet.flag & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.key, null);
        }
        return KeyPair.init(keyValueRet.key, keyValueRet.value);
    }

    /// Moves the cursor to the next item in the bucket and returns its key and value.
    /// If the cursor is at the end of the bucket then a nil key and value are returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn next(
        self: *Self,
    ) KeyPair {
        assert(self._bucket.tx.?.db != null, "tx closed", .{});
        const keyValueRet = self._next();
        if (keyValueRet.key == null) {
            return KeyPair.init(null, null);
        }
        // Return an error if current value is a bucket.
        if (keyValueRet.flag & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.key, null);
        }
        return KeyPair.init(keyValueRet.key, keyValueRet.value);
    }

    /// Moves the cursor to the previous item in the bucket and returns its key and value.
    /// If the cursor is at the beginning of the bucket then a nil key and value are returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn prev(self: *Self) KeyPair {
        assert(self._bucket.tx.?.db != null, "tx closed", .{});
        // Attempt to move back one element until we're successful.
        // Move up the stack as we hit the beginning of each page in our stack.
        var i: isize = @as(isize, @intCast(self.stack.items.len)) - 1;
        while (i >= 0) : (i -= 1) {
            const elem = &self.stack.items[@as(usize, @intCast(i))];
            if (elem.index > 0) {
                elem.index -= 1;
                break;
            }
            self.stack.resize(@as(usize, @intCast(i))) catch unreachable;
        }

        // If we've hit the end then return nil.
        if (self.stack.items.len == 0) {
            return KeyPair.init(null, null);
        }
        // Move down the stack to find the last element of the last leaf under this branch.
        self._last();

        const keyValueRet = self.keyValue();
        if (keyValueRet.key == null) {
            return KeyPair.init(null, null);
        }
        if (keyValueRet.flag & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.key, null);
        }
        return KeyPair.init(keyValueRet.key, keyValueRet.value);
    }

    /// Moves the cursor to a given key and returns it.
    /// If the key does not exist then the next key is used. If no keys
    /// follow, a nil key is returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn seek(self: *Self, seekKey: []const u8) KeyPair {
        var keyValueRet = self._seek(seekKey);
        // If we ended up after the last element of a page then move to the next one.
        const ref = self.stack.getLast();
        if (ref.index >= ref.count()) {
            // the level page has remove all key?
            keyValueRet = self._next();
        }
        if (keyValueRet.key == null) {
            return KeyPair.init(null, null);
        } else if (keyValueRet.flag & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.key, null);
        }
        return KeyPair.init(keyValueRet.key, keyValueRet.value);
    }

    /// Removes the current key/value under the cursor from the bucket.
    /// Delete fails if current key/value is a bucket or if the transaction is not writable.
    pub fn delete(self: *Self) Error!void {
        assert(self._bucket.tx.?.db != null, "tx closed", .{});
        if (!self._bucket.tx.?.writable) {
            return Error.TxNotWriteable;
        }
        const keyValueRet = self.keyValue();
        // Return an error if current value is a bucket.
        if (keyValueRet.flag & consts.BucketLeafFlag != 0) {
            return Error.IncompactibleValue;
        }

        self.getNode().?.del(keyValueRet.key.?);
    }

    // Moves the cursor to a given key and returns it.
    // If the key does not exist then the next key is used.
    pub fn _seek(self: *Self, seekKey: []const u8) KeyValueRef {
        assert(self._bucket.tx.?.db != null, "tx closed", .{});
        // Start from root page/node and traverse to correct page.
        self.stack.resize(0) catch unreachable;
        // std.log.info("seekKey: {s}, root: {}\n", .{ seekKey, self._bucket._b.?.root });
        self.search(seekKey, self._bucket._b.?.root);
        const ref = self.getLastElementRef().?;
        // If the cursor is pointing to the end of page/node then return nil.
        // TODO, if not found the key, the index should be 0, but the count maybe > 0
        if (ref.index >= ref.count()) {
            return KeyValueRef{ .key = null, .value = null, .flag = 0 };
        }
        // If this is a bucket then return a nil value.
        return self.keyValue();
    }

    // Moves the cursor to the first leaf element under that last page in the stack.
    fn _first(self: *Self) void {
        while (true) {
            // std.log.info("the stack is {}", .{self.stack.items.len});
            // Exit when we hit a leaf page.
            const ref = self.stack.getLast();
            if (ref.isLeaf()) {
                // had move to the first element that first leaf's key.
                break;
            }
            // Keep adding pages pointing to the first element to the stack.
            var pgid: PgidType = 0;
            if (ref.node) |n| {
                pgid = n.inodes.items[ref.index].pgid;
            } else {
                assert(ref.index < ref.p.?.count, "the index is out of range, index: {}, count: {}", .{ ref.index, ref.p.?.count });
                pgid = ref.p.?.branchPageElementRef(ref.index).?.pgid;
            }
            const pNode = self._bucket.pageNode(pgid);
            // assert(self.stack.items.len < 3, "the stack is too long, stack: {any}", .{self.stack.items});
            // std.log.info("the pNode is {any}", .{pNode});
            self.stack.append(ElementRef{ .p = pNode.page, .node = pNode.node, .index = 0 }) catch unreachable;
            assert(self.stack.getLast().index == 0, "the index is not 0, index: {}", .{self.stack.getLast().index});
        }

        // std.log.info("now, the stack len is {}, the last element ref is {any}", .{ self.stack.items.len, self.stack.getLast() });
    }

    // Moves the cursor to the last leaf element under that last page in the stack.
    fn _last(self: *Self) void {
        while (true) {
            // Exit when we hit a leaf page.
            const ref = self.stack.getLast();
            if (ref.isLeaf()) {
                break;
            }

            // Keep adding pages pointing to the last element in the stack.
            var pgid: PgidType = 0;
            if (ref.node) |_node| {
                pgid = _node.pgid;
            } else {
                pgid = ref.p.?.branchPageElementRef(ref.index).?.pgid;
            }

            const pNode = self._bucket.pageNode(pgid);
            var nextRef = ElementRef{ .p = pNode.page, .node = pNode.node, .index = 0 };
            nextRef.index = nextRef.count() - 1;
            self.stack.append(nextRef) catch unreachable;
        }
    }

    /// Moves to the next leaf element and returns the key and value.
    /// If the cursor is at the last leaf element then it stays there and return null.
    pub fn _next(self: *Self) KeyValueRef {
        // defer std.log.info("the stack is {}", .{self.stack.items.len});
        while (true) {
            // assert(self.stack.items.len > 0, "the stack is empty", .{});
            // Attempt to move over one element until we're successful.
            // Move up the stack as we hit the end of each page in our stack.
            var i: isize = @as(isize, @intCast(self.stack.items.len - 1));
            // std.log.info("the i is {}", .{i});
            while (i >= 0) : (i -= 1) {
                const elem = &self.stack.items[@as(usize, @intCast(i))];
                if ((elem.index + 1) < elem.count()) { // iterate the current inode elements
                    elem.index += 1;
                    break;
                }
                // pop the current page by index that same to pop the current inode from the stack.
                // _ = self.stack.pop();
            }

            // If we've hit the root page then stop and return. This will leave the
            // cursor on the last element of the past page.
            if (i == -1) {
                return KeyValueRef{ .key = null, .value = null, .flag = 0 };
            }

            // Otherwise start from where we left off in the stack and find the
            // first element of the first leaf page.
            self.stack.resize(@as(usize, @intCast(i + 1))) catch unreachable; // TODO
            assert(self.stack.items.len == (i + 1), "the stack is empty", .{});
            // Fix location
            self._first();

            // If this is an empty page then restart and move back up the stack.
            if (self.getLastElementRef().?.count() == 0) {
                continue;
            }
            return self.keyValue();
        }
    }

    /// Search recursively performs a binary search against a given page/node until it finds a given key.
    pub fn search(self: *Self, key: []const u8, pgid: PgidType) void {
        const pNode = self._bucket.pageNode(pgid);
        const p = pNode.page;
        const n = pNode.node;
        if (p != null and (p.?.flags & (consts.intFromFlags(.branch) | consts.intFromFlags(.leaf)) == 0)) {
            assert(false, "invalid page type, pgid: {}, flags: {}, page: {any}\n", .{ pgid, p.?.flags, p.? });
        }

        const e = ElementRef{ .p = p, .node = n };
        self.stack.append(e) catch unreachable;
        // If we're on a leaf page/node then find the specific node.
        if (e.isLeaf()) {
            // return a equal or greater than key?
            self.nsearch(key);
            return;
        }
        if (n) |_node| {
            self.searchNode(key, _node);
            return;
        }
        assert(p.?.id == pgid, "the page id is not equal to the pgid, page id: {}, pgid: {}", .{ p.?.id, pgid });
        self.searchPage(key, p.?);
    }

    /// Returns the node that then cursor is currently positioned on.
    pub fn node(self: *Self) ?*Node {
        assert(self.stack.items.len > 0, "accessing a node with a zero-length cursor stack", .{});

        // If the top of the stack is a leaf node then just return it.
        const topRef = self.getLastElementRef().?;
        if (topRef.node != null and topRef.node.?.isLeaf) {
            std.log.debug("return a topRef node", .{});
            return topRef.node;
        }
        // Start from root and traveerse down the hierarchy.
        var n = self.stack.items[0].node orelse self._bucket.node(self.stack.items[0].p.?.id, null);
        for (self.stack.items[0 .. self.stack.items.len - 1]) |ref| {
            assert(!n.isLeaf, "expected branch node", .{});
            n = n.childAt(ref.index).?;
        }

        assert(n.isLeaf, "expect leaf node", .{});
        std.log.debug("return a node, pgid: {}", .{n.pgid});
        return n;
    }

    // Search key from nodes.
    fn searchNode(self: *Self, key: []const u8, n: *const Node) void {
        const printNodes = struct {
            fn print(curNode: *const Node) void {
                for (curNode.inodes.items, 0..) |iNode, i| {
                    const iKey = iNode.getKey().?;
                    std.log.debug("i={}, key={any}, len={}, iKey = {any}, len={}", .{ i, curNode.key.?, curNode.key.?.len, iKey, iKey.len });
                }
            }
        }.print;
        // _ = printNodes;
        printNodes(n);
        var index = n.upperBoundInodes(key);
        if (index > 0) {
            index -= 1;
        }
        std.log.debug("find index: {}", .{index});
        // Recursively search to the next node.
        var lastEntry = self.stack.getLast();
        lastEntry.index = index;
        self.search(key, n.inodes.items[index].pgid);
    }

    // Search key from pages
    fn searchPage(self: *Self, key: []const u8, p: *page.Page) void {
        assert(p.flags == consts.intFromFlags(.branch), "the page is not a branch page, page: {any}", .{p});
        // Binary search for the correct range.
        var elementRef = p.searchBranchElements(key);
        if (!elementRef.exact and elementRef.index > 0) {
            elementRef.index -= 1;
        }
        // if (p.id == 117) {
        //     for (0..p.count) |i| {
        //         const elem = p.branchPageElement(i);
        //         std.log.debug("branch page element: {any}, elementRef: {any}", .{ elem, elementRef });
        //     }
        // }
        self.getLastElementRef().?.index = elementRef.index;
        // Recursively search to the next page.
        const nextPgid = p.branchPageElementRef(elementRef.index).?.pgid;
        self.search(key, nextPgid);
    }

    // Searches the leaf node on the top of the stack for a key
    fn nsearch(self: *Self, key: []const u8) void {
        const e = self.getLastElementRef().?;
        const p = e.p;
        const n = e.node;

        // If we have a node then search its inodes.
        if (n) |_node| {
            const index = std.sort.lowerBound(INode, _node.inodes.items, key, INode.lowerBoundFn);
            e.index = index;
            return;
        }

        // If we have a page then search its leaf elements.
        const index = p.?.searchLeafElements(key).index;
        e.index = index;
    }

    // get the key and value of the cursor.
    fn keyValue(self: *Self) KeyValueRef {
        const ref = self.stack.getLast();
        if (ref.count() == 0 or ref.index >= ref.count()) {
            // 1: all key remove of tx, the page's keys are 0,
            // 2: index == count indicate not found the key.
            return KeyValueRef{ .key = null, .value = null, .flag = 0 };
        }

        // Retrieve value from node.
        if (ref.node) |refNode| {
            const inode = &refNode.inodes.items[ref.index];
            return KeyValueRef{ .key = inode.getKey(), .value = inode.getValue(), .flag = inode.flags };
        }

        // Or retrieve value from page.
        const elem = ref.p.?.leafPageElement(ref.index).?;
        return KeyValueRef{ .key = elem.key(), .value = elem.value(), .flag = elem.flags };
    }

    /// Returns the node that the cursor is currently positioned on.
    fn getNode(self: *Self) ?*Node {
        assert(self.stack.items.len > 0, "accessing a node with a zero-length cursor stack", .{});

        // If the top of the stack is a leaf node then just return it.
        const latestElementRef = self.getLastElementRef().?;
        if (latestElementRef.node != null and latestElementRef.node.?.isLeaf) {
            return latestElementRef.node;
        }
        // Start from root and traverse down the lierarchy.
        var n = self.stack.items[0].node;
        if (n == null) {
            // assert(self.stack.items[0].p.?.id > 1, "the page id is not valid, id: {}", .{self.stack.items[0].p.?.id});
            n = self._bucket.node(self.stack.items[0].p.?.id, null);
            std.log.warn("the node is null, so it is the root node at this bucket, pgid: {}", .{self.stack.items[0].p.?.id});
        }
        // find the node from the stack from the top to the bottom.
        for (self.stack.items[0..(self.stack.items.len - 1)]) |ref| {
            assert(!n.?.isLeaf, "expected branch node", .{});
            n = n.?.childAt(ref.index);
        }

        assert(n.?.isLeaf, "expect leaf node", .{});
        return n;
    }

    // get the last element reference of the stack.
    fn getLastElementRef(self: *Self) ?*ElementRef {
        if (self.stack.items.len == 0) {
            return null;
        }
        return &self.stack.items[self.stack.items.len - 1];
    }
};

// Represents a reference to an element on a given page/node.
const ElementRef = struct {
    // page
    p: ?*page.Page = null,
    // node, Thinking: if the transaction is read-only, the node is null. don't you know?
    node: ?*Node = null,
    index: usize = 0,

    // Create a new element reference.
    fn init(allocator: std.mem.Allocator, index: usize, p: ?*page.Page, node: ?*Node) *ElementRef {
        const self = allocator.create(ElementRef) catch unreachable;
        self.* = .{
            .p = p,
            .node = node,
            .index = index,
        };
        return self;
    }

    // Returns true if the element is a leaf element.
    fn isLeaf(self: *const ElementRef) bool {
        if (self.node) |node| {
            return node.isLeaf;
        }
        return self.p.?.flags & consts.intFromFlags(.leaf) != 0;
    }

    // returns the number of inodes or page elements.
    fn count(self: *const ElementRef) usize {
        if (self.node) |node| {
            return node.inodes.items.len;
        }
        return @as(usize, self.p.?.count);
    }
};
