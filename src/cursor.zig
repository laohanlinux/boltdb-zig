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
const Tuple = consts.Tuple;
const KeyPair = consts.KeyPair;
const KeyValueRef = consts.Tuple.t3(?[]const u8, ?[]u8, u32);
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

        const ref = ElementRef{ .p = pNode.first, .node = pNode.second, .index = 0 };
        self.stack.append(ref) catch unreachable;
        _ = self._first();
        // If we land on an empty page then move to the next value.
        // https://github.com/boltdb/bolt/issues/450
        if (self.stack.getLast().count() == 0) {
            std.log.info("next key {any}", .{pNode.first});
            _ = self._next();
        }
        const keyValueRet = self.keyValue();
        if (keyValueRet.first == null) {
            return KeyPair.init(null, null);
        }
        // Return an error if current value is a bucket.
        if (keyValueRet.third & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.first.?, null);
        }
        return KeyPair.init(keyValueRet.first.?, keyValueRet.second);
    }

    /// Moves the cursor to the last item in the bucket and returns its key and value.
    /// If the bucket is empty then a nil key and value are returned.
    pub fn last(self: *Self) [2]?[]u8 {
        assert(self._bucket.tx.?.db == null, "tx closed", .{});
        self.stack.resize(0) catch unreachable;
        const pNode = self._bucket.?.pageNode(self._bucket.?._b.root);
        var ref = ElementRef{ .p = pNode.first, .node = pNode.second, .index = 0 };
        ref.index = ref.count() - 1;
        self.stack.append(ref) catch unreachable;
        self._last();
        const keyValueRet = self.keyValue();
        // Return an error if current value is a bucket.
        if (keyValueRet.third & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.third, null);
        }
        return KeyPair.init(keyValueRet.first, keyValueRet.second);
    }

    /// Moves the cursor to the next item in the bucket and returns its key and value.
    /// If the cursor is at the end of the bucket then a nil key and value are returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn next(
        self: *Self,
    ) KeyPair {
        assert(self._bucket.tx.?.db == null, "tx closed", .{});
        const keyValueRet = self._next();
        // Return an error if current value is a bucket.
        if (keyValueRet.third & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.first, null);
        }
        return KeyPair.init(keyValueRet.first, keyValueRet.second);
    }

    /// Moves the cursor to the previous item in the bucket and returns its key and value.
    /// If the cursor is at the beginning of the bucket then a nil key and value are returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn prev(self: *Self) KeyPair {
        assert(self._bucket.?.tx.?.db != null, "tx closed", .{});
        // Attempt to move back one element until we're successful.
        // Move up the stack as we hit the beginning of each page in our stack.
        var i: isize = self.stack.items.len - 1;
        while (i >= 0) : (i -= 1) {
            const elem = &self.stack.items[i];
            if (elem.index > 0) {
                elem.index -= 1;
                break;
            }
            self.stack.resize(i);
        }

        // If we've hit the end then return nil.
        if (self.stack.items.len == 0) {
            return KeyPair.init(null, null);
        }
        // Move down the stack to find the last element of the last leaf under this branch.
        self._last();

        const keyValueRet = self.keyValue();
        if (keyValueRet.third & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.first, null);
        }
        return KeyPair.init(keyValueRet.first, keyValueRet.second);
    }

    /// Moves the cursor to a given key and returns it.
    /// If the key does not exist then the next key is used. If no keys
    /// follow, a nil key is returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn seek(self: *Self, seekKey: []u8) KeyPair {
        var keyValueRet = self._seek(seekKey);
        // If we ended up after the last element of a page then move to the next one.
        const ref = self.stack.getLast();
        if (ref.index >= ref.count()) {
            // the level page has remove all key?
            keyValueRet = self._next();
        }
        if (keyValueRet.first == null) {
            return KeyPair.init(null, null);
        } else if (keyValueRet.third & consts.BucketLeafFlag != 0) {
            return KeyPair.init(keyValueRet.first, null);
        }
        return KeyPair.init(keyValueRet.first, keyValueRet.second);
    }

    /// Removes the current key/value under the cursor from the bucket.
    /// Delete fails if current key/value is a bucket or if the transaction is not writable.
    pub fn delete(self: *Self) Error!void {
        if (self._bucket.tx.?.db == null) {
            return Error.ErrTxClosed;
        } else if (!self._bucket.tx.?.writable) {
            return Error.TxNotWriteable;
        }
        const keyValueRet = self.keyValue();
        // Return an error if current value is a bucket.
        if (keyValueRet.third & consts.BucketLeafFlag != 0) {
            return Error.IncompactibleValue;
        }

        self.getNode().?.del(keyValueRet.first);
    }

    // Moves the cursor to a given key and returns it.
    // If the key does not exist then the next key is used.
    pub fn _seek(self: *Self, seekKey: []const u8) KeyValueRef {
        assert(self._bucket.tx.?.db != null, "tx closed", .{});
        // Start from root page/node and traverse to correct page.
        self.stack.resize(0) catch unreachable;
        self.search(seekKey, self._bucket._b.?.root);
        const ref = self.getLastElementRef().?;
        // If the cursor is pointing to the end of page/node then return nil.
        // TODO, if not found the key, the index should be 0, but the count maybe > 0
        if (ref.index >= ref.count()) {
            return KeyValueRef{ .first = null, .second = null, .third = 0 };
        }
        // If this is a bucket then return a nil value.
        return self.keyValue();
    }

    // Moves the cursor to the first leaf element under that last page in the stack.
    fn _first(self: *Self) void {
        while (true) {
            // Exit when we hit a leaf page.
            const ref = self.stack.getLast();
            if (ref.isLeaf()) {
                break;
            }
            // Keep adding pages pointing to the first element to the stack.
            var pgid: page.PgidType = 0;
            if (ref.node) |n| {
                pgid = n.pgid;
            } else {
                pgid = ref.p.?.branchPageElementPtr(ref.index).pgid;
            }
            const pNode = self._bucket.pageNode(pgid);
            self.stack.append(ElementRef{ .p = pNode.first, .node = pNode.second, .index = 0 }) catch unreachable;
        }
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
            var pgid: page.PgidType = 0;
            if (ref.node) |_node| {
                pgid = _node.pgid;
            } else {
                pgid = ref.p.?.branchPageElementPtr(ref.index).pgid;
            }

            const pNode = self._bucket.pageNode(pgid);
            var nextRef = ElementRef{ .p = pNode.first, .node = pNode.second, .index = 0 };
            nextRef.index = nextRef.count() - 1;
            self.stack.append(nextRef) catch unreachable;
        }
    }

    /// Moves to the next leaf element and returns the key and value.
    /// If the cursor is at the last leaf element then it stays there and return null.
    pub fn _next(self: *Self) KeyValueRef {
        while (true) {
            // Attempt to move over one element until we're successful.
            // Move up the stack as we hit the end of each page in our stack.
            var i: isize = @as(isize, @intCast(self.stack.items.len - 1));
            std.log.info("the i is {}", .{i});
            while (i >= 0) : (i -= 1) {
                const elem = &self.stack.items[@as(usize, @intCast(i))];
                if ((elem.index + 1) < elem.count()) {
                    elem.index += 1;
                    break;
                }
            }

            // If we've hit the root page then stop and return. This will leave the
            // cursor on the last element of the past page.
            if (i == -1) {
                return KeyValueRef{ .first = null, .second = null, .third = 0 };
            }

            // Otherwise start from where we left off in the stack and find the
            // first element of the first leaf page.
            self.stack.resize(@as(usize, @intCast(i + 1))) catch unreachable; // TODO
            // Fix location
            self._first();

            // If this is an empty page then restart and move back up the stack.
            if (self.stack.getLast().count() == 0) {
                continue;
            }

            return self.keyValue();
        }
    }

    /// Search recursively performs a binary search against a given page/node until it finds a given key.
    pub fn search(self: *Self, key: []const u8, pgid: page.PgidType) void {
        const pNode = self._bucket.pageNode(pgid);
        const p = pNode.first;
        const n = pNode.second;
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
        return n;
    }

    // Search key from nodes.
    fn searchNode(self: *Self, key: []const u8, n: *const Node) void {
        const f = struct {
            fn searchFn(_key: []const u8, inode: INode) std.math.Order {
                return util.cmpBytes(_key, inode.key.?);
            }
        };
        const index = std.sort.binarySearch(INode, n.inodes.items, key, f.searchFn) orelse (self.stack.items.len - 1);
        // Recursively search to the next node.
        var lastEntry = self.stack.getLast();
        lastEntry.index = index;
        self.search(key, self.stack.items[index].p.?.id);
    }

    // Search key from pages
    fn searchPage(self: *Self, key: []const u8, p: *page.Page) void {
        // Binary search for the correct range.
        const inodes = p.branchPageElements().?;
        const index = std.sort.binarySearch(page.BranchPageElement, inodes, key, cmpBranchElementFn) orelse self.stack.items.len - 1;
        self.getLastElementRef().?.index = index;
        // Recursively search to the next page.
        self.search(key, inodes[index].pgid);
    }

    // Searches the leaf node on the top of the stack for a key
    fn nsearch(self: *Self, key: []const u8) void {
        const e = self.getLastElementRef().?;
        const p = e.p;
        const n = e.node;

        // If we have a node then search its inodes.
        if (n) |_node| {
            const index = std.sort.lowerBound(INode, _node.inodes.items, key, INode.cmp);
            e.index = index;
            // std.log.debug("nsearch: {s}, inodes: {s}", .{ key, _node.inodes.items[index].key.? });
            return;
        }

        // If we have a page then search its leaf elements.
        e.index = 0;
        const inodes = p.?.leafPageElements() orelse return;
        const index = std.sort.lowerBound(page.LeafPageElement, inodes, key, page.LeafPageElement.cmp);
        e.index = index;
    }

    // get the key and value of the cursor.
    fn keyValue(self: *Self) KeyValueRef {
        const ref = self.stack.getLast();
        if (ref.count() == 0 or ref.index >= ref.count()) {
            // 1: all key remove of tx, the page's keys are 0,
            // 2: index == count indicate not found the key.
            return KeyValueRef{ .first = null, .second = null, .third = 0 };
        }

        // Retrieve value from node.
        if (ref.node) |refNode| {
            const inode = refNode.inodes.items[ref.index];
            return KeyValueRef{ .first = inode.key, .second = inode.value, .third = inode.flags };
        }

        // Or retrieve value from page.
        const elem = ref.p.?.leafPageElement(ref.index);
        return KeyValueRef{ .first = elem.?.key(), .second = elem.?.value(), .third = elem.?.flags };
    }

    /// Returns the node that the cursor is currently positioned on.
    fn getNode(self: *Self) ?*Node {
        assert(self.stack.items.len > 0, "accessing a node with a zero-length cursor stack", .{});

        // If the top of the stack is a leaf node then just return it.
        if (self.stack.getLastOrNull()) |ref| {
            if (ref.node != null and ref.node.?.isLeaf) {
                return ref.node;
            }
        }
        // Start from root and traverse down the lierarchy.
        var n = self.stack.items[0].node;
        if (n == null) {
            n = self._bucket.node(self.stack.items[0].p.?.id, null);
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

// find the key in the branch page.
fn findEqualBranchElementFn(findKey: []const u8, a: page.BranchPageElement, b: page.BranchPageElement) bool {
    const order = cmpBranchElementFn(findKey, a, b);
    return order == std.math.Order.eq;
}

// find the key in the branch page.
fn cmpBranchElementFn(findKey: []const u8, elementRef: page.BranchPageElement) std.math.Order {
    const order = util.cmpBytes(findKey, elementRef.key());
    return order;
}

test "cursor" {}
