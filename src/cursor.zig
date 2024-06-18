const std = @import("std");
const Bucket = @import("./bucket.zig").Bucket;
const Node = @import("./node.zig").Node;
const INode = @import("./node.zig").INode;
const findINodeFn = @import("./node.zig").findFn;
const lessThanFn = @import("./node.zig").lessThanFn;
const page = @import("./page.zig");
const util = @import("./util.zig");
const assert = util.assert;
const consts = @import("./consts.zig");
const Tuple = consts.Tuple;
const KeyValueRet = consts.Tuple.t3(?[]u8, ?[]u8, u32);

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
    _bucket: ?*Bucket,
    stack: std.ArrayList(ElementRef),

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, bucket: *Bucket) Self {
        return Cursor{ ._bucket = bucket, .stack = std.ArrayList(ElementRef).init(allocator), .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        self.stack.deinit();
    }

    /// First moves the cursor to the first item in the bucket and returns its key and value.
    /// If the bucket is empty then a nil key and value are returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn first(self: *Self) ?[2][]u8 {
        assert(self._bucket.?.tx.?.db != null, "tx closed", .{});
        self.stack.resize(0) catch unreachable;
        const pacheCache = self._bucket.?.pageNode();
        try self.stack.append(ElementRef{ .p = pacheCache[0], .node = pacheCache[1] });
        self._first();

        // If we land on an empty page then move th the next value.
        // https://github.com/boltdb/bolt/issues/450
        if (self.stack.getLast().count() == 0) {
            self.next();
        }
    }

    /// Moves to the next leaf element and returns the key and value.
    /// If the cursor is at the last leaf element then it stays there and return null.
    pub fn next(self: *Self) KeyValueRet {
        while (true) {
            // Attempt to move over one element until we're successful.
            // Move up the stack as we hit the end of each page in our stack.
            var i: usize = self.stack.items.len;
            while (i > 0) : (i -= 1) {
                const index = i - 1;
                const elem = &self.stack.items[index];
                if (elem.index < elem.count() - 1) {
                    elem.index -= 1;
                    break;
                }
            }

            // If we've hit the root page then stop and return. This will leave the
            // cursor on the last element of the past page.
            if (i == 0) {
                return KeyValueRet{ .first = null, .second = null, .third = 0 };
            }

            // Otherwise start from where we left off in the stack and find the
            // first element of the first leaf page.
            self.stack.resize(i) catch unreachable; // TODO
            _ = self.first();

            // If this is an empty page then restart and move back up the stack.
            if (self.stack.getLast().count() == 0) {
                continue;
            }

            return self.keyValue();
        }
    }

    /// Search recursively performs a binary search against a given page/node until it finds a given key.
    pub fn search(self: *Self, key: []u8, pgid: page.PgidType) void {
        const p: ?*page.Page, const n: ?*Node = self._bucket.?.pageNode(pgid);
        assert(p != null and p.?.flags & (page.intFromFlags(page.PageFlage.branch) | page.intFromFlags(page.PageFlage.branch)) != 0, "invalid page type: {d}: {x}", .{ p.?.id, p.?.flags });
        const e = ElementRef{ .p = p, .node = n };
        self.stack.append(e) catch unreachable;

        // If we're on a leaf page/node then find the specific node.
        if (e.isLeaf()) {
            self.nsearch(key);
            return;
        }

        if (n) |_node| {
            self.searchNode(key, _node);
            return;
        }

        self.searchPage(key, p);
    }

    fn searchNode(self: *Self, key: []u8, n: *const Node) void {
        const index = std.sort.binarySearch(*INode, INode.init(0, 0, key, null), n.inodes.items, {}, findINodeFn);
        // Recursively search to the next node.
        self.stack.getLast().index = index orelse (self.stack.items.len - 1);
        self.search(key, self.stack.items[index].p.?.id);
    }

    fn searchPage(self: *Self, key: []u8, p: *page.Page) void {
        // Binary search for the correct range.
        const inodes = p.branchPageElements().?;
        var keyEl: page.BranchPageElement = undefined;
        keyEl.pos = 0;
        const index = std.sort.binarySearch(page.BranchPageElement, keyEl, inodes, key, findEqualBranchElementFn);
        self.stack.getLast().index = index orelse (self.stack.items.len - 1);
        // Recursively search to the next page.
        self.search(key, inodes[index].pgid);
    }

    /// Searches the leaf node on the top of the stack for a key
    fn nsearch(self: *Self, key: []u8) void {
        const e = &self.stack.items[self.stack.items.len - 1];
        const p = e.p;
        const n = e.node;

        // If we have a node then search its inodes.
        if (n) |node| {
            const index = std.sort.lowerBound(INode, INode.init(0, 0, key, null), node.inodes.items, {}, lessThanFn);
            e.index = index;
            return;
        }

        // If we have a page then search its leaf elements.
        const inodes = p.?.leafPageElements().?;
        var keyEl: page.LeafPageElement = undefined;
        keyEl.pos = 0;
        const index = std.sort.lowerBound(page.LeafPageElement, keyEl, inodes, key, lessThanLeafElementFn);
        e.index = index;
    }

    fn keyValue(self: *Self) KeyValueRet(?[]u8, ?[]u8, u32) {
        const ref = self.stack.getLast();
        if (ref.count() == 0 or ref.index >= ref.count()) {
            return KeyValueRet{ .first = null, .second = null, .third = 0 };
        }

        // Retrieve value from node.
        if (ref.node) |refNode| {
            const inode = refNode.inodes.items[ref.index];
            return KeyValueRet{ .first = inode.key, .second = inode.value, .third = inode.flags };
        }

        // Or retrieve value from page.
        const elem = ref.p.?.leafPageElement(ref.index);
        return KeyValueRet{ .first = elem.?.key(), .second = elem.?.value(), .third = elem.?.flags };
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
            n = self._bucket.?.node(self.stack.items[0].p.?.id, null);
        }

        for (self.stack.items[0..(self.stack.items.len - 1)]) |ref| {
            assert(!n.?.isLeaf, "expected branch node", .{});
            n = n.?.childAt(ref.index);
        }

        assert(n.?.isLeaf, "expect leaf node", .{});
        return n;
    }
};

// Represents a reference to an element on a given page/node.
const ElementRef = struct {
    p: ?*page.Page = null,
    node: ?*Node = null,
    index: usize = 0,

    fn isLeaf(self: *const ElementRef) bool {
        if (self.node) |node| {
            return node.isLeaf;
        }
        return self.p.?.flags & page.intFromFlags(page.PageFlage.leaf) != 0;
    }

    // returns the number of inodes or page elements.
    fn count(self: *const ElementRef) usize {
        if (self.node) |node| {
            return node.inodes.items.len;
        }
        return @as(usize, self.p.?.count);
    }
};

pub fn findEqualBranchElementFn(findKey: []u8, a: page.BranchPageElement, b: page.BranchPageElement) bool {
    var aKey: []u8 = undefined;
    if (a.pos == 0) {
        aKey = findKey;
    } else {
        aKey = a.key();
    }
    var bKey: []u8 = undefined;
    if (b.pos == 0) {
        bKey = findKey;
    } else {
        bKey = b.key();
    }
    const order = util.cmpBytes(aKey, bKey);
    return order == std.math.Order.eq;
}

pub fn lessThanLeafElementFn(findKey: []u8, a: page.LeafPageElement, b: page.LeafPageElement) bool {
    var aKey: []u8 = undefined;
    if (a.pos == 0) {
        aKey = findKey;
    } else {
        aKey = a.key();
    }
    var bKey: []u8 = undefined;
    if (b.pos == 0) {
        bKey = findKey;
    } else {
        bKey = b.key();
    }
    const order = util.cmpBytes(aKey, bKey);
    return order == std.math.Order.lt or order == std.math.Order.eq;
}
