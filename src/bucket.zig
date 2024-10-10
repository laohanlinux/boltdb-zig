const page = @import("page.zig");
const tx = @import("tx.zig");
const std = @import("std");
const Node = @import("node.zig").Node;
const assert = @import("util.zig").assert;
const Tuple = @import("consts.zig").Tuple;
const Tuple2 = Tuple.t2(?*page.Page, *Node);
const Cursor = @import("cursor.zig").Cursor;
const consts = @import("consts.zig");
const util = @import("util.zig");
const Error = @import("error.zig").Error;
const PageOrNode = consts.PageOrNode;
const BufStr = consts.BufStr;
const PgidType = consts.PgidType;
const Page = page.Page;
// A set of nodes that will be freed by the bucket.
const NodeSet = std.AutoHashMap(*Node, void);

// A set of aligned values that will be freed by the bucket.
pub const AutoFreeObject = struct {
    isFreed: bool = false,
    // A set of aligned values that will be freed by the bucket.
    alignedValue: std.ArrayList([]u8),
    // A set of nodes that will be freed by the bucket.
    // 1: Note, the bucket.nodes is not in the autoFreeObject, so we need to destroy it manually.
    // But the bucket.rootNode is in the autoFreeObject, so we don't need to destroy it manually.
    // Because the bucket.rootNode is a special node, it is the root node of the bucket.
    // So, we need to destroy it manually.
    // 2: the nodes of autoFreeNodes is a new node that created after tx.commit(Copy on Write), their are is a spill node, a snapshot node, a new node.
    autoFreeNodes: NodeSet,

    freePtrs: std.AutoArrayHashMap(u64, isize),

    allocator: std.mem.Allocator,
    /// Init the auto free object.
    pub fn init(allocator: std.mem.Allocator) AutoFreeObject {
        return .{
            .alignedValue = std.ArrayList([]u8).init(allocator),
            .autoFreeNodes = NodeSet.init(allocator),
            .allocator = allocator,
            .freePtrs = std.AutoArrayHashMap(u64, isize).init(allocator),
        };
    }

    /// Add a node to the auto free object.
    pub fn addNode(self: *AutoFreeObject, node: *Node) void {
        const key = node.key orelse "";
        const gop = self.autoFreeNodes.getOrPut(node) catch unreachable;
        const ptr = @intFromPtr(node);
        // assert(gop.found_existing == false, "the node({}: 0x{x}, {d}) is already in the auto free nodes", .{ node.pgid, ptr, node.id });
        std.log.debug("add node to the auto free nodes, key: {s}, pgid: {d}, ptr: 0x{x}, id: {d}", .{ key, node.pgid, ptr, node.id });
        if (gop.found_existing) {
            std.log.debug("the node({s}, {}: 0x{x}, {d}) is already in the auto free nodes", .{ key, node.pgid, ptr, node.id });
        }
    }

    /// Add an aligned value to the auto free object.
    pub fn addAlignedValue(self: *AutoFreeObject, value: []u8) void {
        self.alignedValue.append(value) catch unreachable;
    }

    /// Deinit the auto free object.
    pub fn deinit(self: *AutoFreeObject, allocator: std.mem.Allocator) void {
        assert(self.isFreed == false, "the auto free object is already freed", .{});
        self.isFreed = true;
        var it = self.autoFreeNodes.keyIterator();
        while (it.next()) |node| {
            const ptr = @intFromPtr(node);
            const key = node.*.key orelse "";
            _ = key; // autofix
            // std.log.debug("deinit the auto free node: {d}, key: {s}, ptr: 0x{x}", .{ node.*.id, key, ptr });
            node.*.deinit();
            self.allocator.destroy(node.*);
            self.freePtrs.put(ptr, 1) catch unreachable;
        }
        self.autoFreeNodes.deinit();

        for (0..self.alignedValue.items.len) |i| {
            allocator.free(self.alignedValue.items[i]);
            const ptr = @intFromPtr(self.alignedValue.items[i].ptr);
            self.freePtrs.put(ptr, 1) catch unreachable;
        }
        self.alignedValue.deinit();
        self.freePtrs.deinit();
    }
};

/// Represents a collection of key/value pairs inside the database.
pub const Bucket = struct {
    isInittialized: bool = false,
    //_b: ?_Bucket = null, // the bucket struct, it is a pointer to the underlying bucket page.
    _b: ?_Bucket align(@alignOf(_Bucket)) = null,
    tx: ?*tx.TX, // the associated transaction
    buckets: std.StringHashMap(*Bucket), // subbucket cache
    nodes: std.AutoHashMap(PgidType, *Node), // node cache
    // materialized node for the root page. Same to nodes.
    // 1: If the transaction is onlyRead, it is null, because readonly transaction just use UnderlyingPage.
    rootNode: ?*Node = null,
    page: ?*page.Page = null, // inline page reference

    // Sets the thredshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amout if you know that your write workloads are mostly append-only.
    //
    // This is non-presisted across transactions so it must be set in every TX.
    fillPercent: f64 = consts.DefaultFillPercent,

    allocator: std.mem.Allocator,

    const Self = @This();

    /// Initializes a new bucket.
    pub fn init(_tx: *tx.TX) *Bucket {
        const b = _tx.db.?.allocator.create(Self) catch unreachable;
        b.isInittialized = true;
        b._b = _Bucket{};
        b.tx = _tx;
        util.assert(b.tx != null, "tx has closed", .{});
        b.allocator = _tx.db.?.allocator;
        // Note:
        // If the transaction is writable, then the b.buckets and b.nodes will be initialized.
        // If the transaction is readonly, then the b.buckets and b.nodes will not be initialized.
        // But for write better code, we need to initialize the b.buckets and b.nodes.
        // So, if the transaction is readonly, travel all the bucket and nodes by underlaying page.
        // don't load the bucket and node into memory.
        b.buckets = std.StringHashMap(*Bucket).init(b.allocator);
        b.nodes = std.AutoHashMap(PgidType, *Node).init(b.allocator);
        // init the rootNode and page to null.
        b.rootNode = null;
        b.page = null;
        // set the fill percent
        b.fillPercent = consts.DefaultFillPercent;
        return b;
    }

    /// Deallocates a bucket and all of its nested buckets and nodes.
    pub fn deinit(self: *Self) void {
        const rootId = self._b.?.root;
        _ = rootId; // autofix
        // defer std.log.debug("finish deinit bucket, rid: {}", .{rootId});
        // std.log.debug("start deinit bucket, rid: {}, root: {}", .{ rootId, self.rootNode == null });
        assert(self.isInittialized, "the bucket is not initialized", .{});
        self.isInittialized = false;
        var btIter = self.buckets.iterator();
        while (btIter.next()) |nextBucket| {
            self.allocator.free(nextBucket.key_ptr.*);
            nextBucket.value_ptr.*.deinit();
        }
        self.buckets.deinit();
        if (self.tx.?.writable) {
            if (self._b) |iBucket| {
                iBucket.deinit(self.allocator);
                self._b = null;
            }
        }
        {
            // Note, the nodes does not exist in the autoFreeObject, so we need to destroy it manually.
            var nodeIter = self.nodes.iterator();
            while (nodeIter.next()) |nextNode| {
                // std.log.debug("--> {}, {}, {d}", .{ rootId, nextNode.key_ptr.*, self.nodes.count() });
                nextNode.value_ptr.*.deinit();
                self.allocator.destroy(nextNode.value_ptr.*);
            }
            self.nodes.deinit();
        }

        self.rootNode = null; // just set the rootNode to null, don't destroy it(it will be destroyed by the autoFreeObject)
        self.allocator.destroy(self);
    }

    /// Destroy the bucket.
    pub fn destroy(self: *Self) void {
        self.allocator.destroy(self);
    }

    /// Create a cursor associated with the bucket.
    /// The cursor is only valid as long as the transaction is open.
    /// Do not use a cursor after the transaction is closed.
    pub fn cursor(self: *Self) Cursor {
        // Update transaction statistics.
        self.tx.?.stats.cursor_count += 1;
        // Allocate and return a cursor.
        return Cursor.init(self.allocator, self);
    }

    /// Retrives a nested bucket by name.
    /// Returns nil if the bucket does not exits.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn getBucket(self: *Self, name: []const u8) ?*Bucket {
        if (self.buckets.get(name)) |_bucket| {
            return _bucket;
        }

        // Move cursor to key.
        var _cursor = self.cursor();
        defer _cursor.deinit();
        const keyPairRef = _cursor._seek(name);
        if (keyPairRef.key == null) {
            return null;
        }

        // Return nil if the key dosn't exist or it is not a bucket.
        if (!std.mem.eql(u8, name, keyPairRef.key.?) or keyPairRef.flag & consts.BucketLeafFlag == 0) {
            return null;
        }

        // because the keyPairRef.second is a bucket value, so we need to open it.
        const child = self.openBucket(keyPairRef.value.?);
        // cache the bucket
        const cpName = self.allocator.dupe(u8, name) catch unreachable;
        self.buckets.put(cpName, child) catch unreachable;
        return child;
    }

    /// Helper method that re-interprets a sub-bucket value
    /// from a parent into a Bucket
    pub fn openBucket(self: *Self, value: []u8) *Bucket {
        // std.log.info("openBucket, value: {any}", .{value});
        const child = Bucket.init(self.tx.?);
        // TODO
        // If unaligned load/stores are broken on this arch and value is
        // unaligned simply clone to an aligned byte array.
        const alignment = std.math.ceilPowerOfTwo(usize, @alignOf(_Bucket)) catch @alignOf(usize);
        var alignedValue: []u8 = undefined;
        const isAligned = @intFromPtr(value.ptr) % alignment == 0;
        if (!isAligned) {
            alignedValue = self.allocator.dupe(u8, value) catch unreachable;
            self.tx.?.autoFreeNodes.alignedValue.append(alignedValue) catch unreachable;
            // self.stats().unAlignValueN += 1;
            // std.log.warn("unaligned memory, align size: {}", .{alignedValue.len});
        } else {
            alignedValue = value;
            // self.stats().alignValueN += 1;
            // std.log.warn("aligned memory, align size: {}, value: {any}", .{ alignedValue.len, alignedValue });
        }
        // If this is a writable transaction then we need to copy the bucket entry.
        // Read-Only transactions can point directly at the mmap entry.
        // TODO Opz the code.
        if (self.tx.?.writable) {
            child._b = _Bucket.init(alignedValue).*;
        } else {
            child._b = _Bucket.init(alignedValue).*;
        }

        // Save a reference to the inline page if the bucket is inline.
        if (child._b.?.root == 0) {
            // Note:
            // The value is a pointer to the underlying bucket page.
            // The bucket page is a 16-byte header followed by a 12-byte page body.
            // So, the bucket page is 28 bytes.
            // The page is a 12-byte body.
            child.page = page.Page.init(alignedValue[Bucket.bucketHeaderSize()..]);
            assert(child.page.?.id == 0, "the page({}) is not inline", .{child.page.?.id});
            assert(child.page.?.flags == consts.intFromFlags(.leaf), "the page({}) is a leaf page", .{child.page.?.id});
            std.log.info("Save a reference to the inline page if the bucket is inline", .{});
            // std.log.debug("inline align value: {any}", .{alignedValue});
        }
        return child;
    }

    /// Creates a new bucket at the given key and returns the new bucket.
    /// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucket(self: *Self, key: []const u8) Error!*Bucket {
        if (self.tx.?.db == null) {
            return Error.TxClosed;
        } else if (!self.tx.?.writable) {
            return Error.TxNotWriteable;
        } else if (key.len == 0) {
            return Error.BucketNameRequired;
        }
        // copy the key, avoid the key be freed by the caller.
        // Move cursor to correct position.
        var c = self.cursor();
        defer c.deinit();
        const keyPairRef = c._seek(key);

        // Return an error if there is an existing key.
        if (keyPairRef.key != null and std.mem.eql(u8, key, keyPairRef.key.?)) {
            if (keyPairRef.flag & consts.BucketLeafFlag != 0) {
                return Error.BucketExists;
            }
            return Error.IncompactibleValue;
        }

        const cpKey = self.allocator.dupe(u8, key) catch unreachable;
        errdefer self.allocator.free(cpKey);

        // Create empty, inline bucket.
        const newBucket = Bucket.init(self.tx.?);
        defer newBucket.deinit();
        newBucket.rootNode = Node.init(self.allocator);
        newBucket.rootNode.?.isLeaf = true;
        self.tx.?.autoFreeNodes.addNode(newBucket.rootNode.?);
        const value = newBucket.write();

        // Insert into node
        _ = c.node().?.put(cpKey, cpKey, value, 0, consts.BucketLeafFlag);
        std.log.info("create a new bucket: {s}", .{cpKey});
        // Since subbuckets are not allowed on inline buckets, we need to
        // dereference the inline page, if it exists. This will cause the bucket
        // to be treated as regular, non-inline bucket for the rest of the tx.
        // FIXME: why
        self.page = null;
        return self.getBucket(key) orelse return Error.BucketNotFound;
    }

    /// Creates a new bucket if it doesn't already exist and returns a reference to it.
    /// Returns an error if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucketIfNotExists(self: *Self, key: []const u8) Error!*Bucket {
        const child = self.createBucket(key) catch |err| switch (err) {
            Error.BucketExists => {
                return self.getBucket(key);
            },
            else => {
                return err;
            },
        };
        return child;
    }

    /// Deletes a bucket at the give key.
    /// Returns an error if the bucket does not exists, or if the key represents a non-bucket value.
    pub fn deleteBucket(self: *Self, key: []const u8) Error!void {
        if (self.tx.db == null) {
            return Error.TxClosed;
        } else if (!self.tx.?.writable) {
            return Error.TxNotWriteable;
        }

        // Move cursor to correct position.
        var c = self.cursor();
        defer c.deinit();
        const keyPairRef = c._seek(key);

        // Return an error if the bucket dosn't exist or is not a bucket.
        if (!std.mem.eql(u8, key, keyPairRef.first)) {
            return Error.BucketNotFound;
        } else if (keyPairRef.third & consts.BucketLeafFlag == 0) {
            return Error.IncompactibleValue;
        }

        // Returnsively delete all child buckets.
        const child = self.getBucket(key).?;
        try child.forEach(traveBucket);
        // Remove cached copy. TODO memory leak
        _ = self.buckets.remove(key);

        // Delete the node if we have a matching key.
        c.node().?.del(key);
    }

    /// Retrives the value for a key in the bucket.
    /// Return a nil value if the key does not exist or if the key is a nested bucket.
    /// The returned value is only valid for the life of the transaction.
    pub fn get(self: *Self, key: []const u8) ?[]u8 {
        var _cursor = self.cursor();
        defer _cursor.deinit();
        const keyPairRef = _cursor._seek(key);
        if (keyPairRef.first == null) {
            return null;
        }
        // Return nil if this is a bucket.
        if (keyPairRef.third & consts.BucketLeafFlag != 0) {
            return null;
        }

        // If our target node isn't the same key as what's passed in then return nil.
        if (!std.mem.eql(u8, key, keyPairRef.first.?)) {
            return null;
        }

        return keyPairRef.second;
    }

    /// Sets the value for a key in the bucket.
    /// If the key exist then its previous value will be overwritten.
    /// Supplied value must remain valid for the life of the transaction.
    /// Returns an error if the bucket was created from a read-only transaction, if the key is bucket, if the key is too large, or
    /// of if the value is too large.
    pub fn put(self: *Self, keyPair: consts.KeyPair) !void {
        if (self.tx.?.db == null) {
            return Error.TxClosed;
        } else if (!self.tx.?.writable) {
            return Error.TxNotWriteable;
        } else if (keyPair.key.?.len == 0) {
            return Error.KeyRequired;
        } else if (keyPair.key.?.len > consts.MaxKeySize) {
            return Error.KeyTooLarge;
        } else if (keyPair.value.?.len > consts.MaxValueSize) {
            return Error.ValueTooLarge;
        }

        // Move cursor to correct position.
        var c = self.cursor();
        defer c.deinit();

        const keyPairRef = c._seek(keyPair.key.?);

        // Return an error if there is an existing key with a bucket value.
        if (keyPairRef.key != null and std.mem.eql(u8, keyPair.key.?, keyPairRef.key.?) and keyPairRef.flag & consts.BucketLeafFlag != 0) {
            return Error.IncompactibleValue;
        }

        // Insert into node.
        const cpKey = self.allocator.dupe(u8, keyPair.key.?) catch unreachable;
        const cpValue = self.allocator.dupe(u8, keyPair.value.?) catch unreachable;
        _ = c.node().?.put(cpKey, cpKey, cpValue, 0, 0);
    }

    /// Removes a key from the bucket.
    /// If the key does not exist then nothing is done and a nil error is returned.
    /// Returns an error if the bucket was created from a read-only transaction.
    pub fn delete(self: *Self, key: []const u8) Error!void {
        if (self.tx.db == null) {
            return Error.TxClosed;
        } else if (!self.tx.writable) {
            return Error.TxNotWriteable;
        }

        // Move cursor to correct position.
        var c = self.cursor();
        defer c.deinit();
        const keyPairRef = c._seek(key);

        // Return on error if there is already existing bucket value.
        if (keyPairRef.third & consts.BucketLeafFlag != 0) {
            return Error.IncompactibleValue;
        }

        // Delete the node if we have a matching key.
        c.node().?.del(key);
    }

    /// Returns the current integer for the bucket without incrementing it.
    pub fn sequence(self: *const Self) u64 {
        return self._b.?.sequence;
    }

    /// Updates the sequence number for the bucket.
    pub fn setSequence(self: *Self, v: u64) Error!void {
        if (self.tx.?.db == null) {
            return Error.TxClosed;
        } else if (!self.tx.?.writable) {
            return Error.TxNotWriteable;
        }

        // Materialize the root node if it hasn't been already so that the
        // bucket will be saved during commit.
        if (self.rootNode == null) {
            _ = self.node(self._b.?.root, null);
        }

        // Increment and return the sequence.
        self._b.?.sequence = v;
        return null;
    }

    /// Returns an autoincrementing integer for the bucket.
    pub fn nextSequence(self: *Self) Error!u64 {
        if (self.tx.?.db == null) {
            return Error.TxClosed;
        } else if (!self.tx.?.writable) {
            return Error.TxNotWriteable;
        }

        // Materialize the root node if it hasn't been already so that the
        // bucket will be saved during commit.
        if (self.rootNode == null) {
            _ = self.node(self._b.?.root, null);
        }

        // Increment and return the sequence
        self._b.?.sequence += 1;
        return self._b.?.sequence;
    }

    /// Executes a function for each key/value pair in a bucket.
    /// If the provided function returns an error then the iteration is stopped and
    /// the error is returned to the caller. The provided function must not modify
    /// the bucket; this will result in undefined behavior.
    pub fn forEach(self: *Self, travel: fn (bt: *Bucket, keyPairRef: *const consts.KeyPair) Error!void) Error!void {
        if (self.tx.?.db == null) {
            return Error.TxClosed;
        }
        const c = self.cursor();
        var keyPairRef = c.first();
        while (keyPairRef.key != null) {
            try travel(self, &keyPairRef);
            keyPairRef = c.next();
        }
        return;
    }

    pub fn forEachKeyValue(self: *Self, context: anytype, comptime travel: fn (@TypeOf(context), key: []const u8, value: ?[]const u8) Error!void) Error!void {
        if (self.tx.?.db == null) {
            return Error.TxClosed;
        }
        var c = self.cursor();
        defer c.deinit();
        var keyPairRef = c.first();
        while (keyPairRef.key != null) {
            try travel(context, keyPairRef.key.?, keyPairRef.value);
            std.log.debug("forEachKeyValue: {any}", .{c.stack.items});
            keyPairRef = c.next();
        }
        return;
    }

    /// Return stats on a bucket.
    pub fn stats(self: *Self) BucketStats {
        var s = BucketStats.init();
        var subStats = BucketStats.init();
        assert(self.tx != null, "tx closed", .{});
        const pageSize = self.tx.?.db.?.pageSize;
        s.BucketN += 1;
        if (self._b.?.root == 0) {
            s.InlineBucketN += 1;
        }
        const tuple3 = Tuple.t3(?*Bucket, ?*BucketStats, ?*BucketStats);
        const tuple3Arg = tuple3{ .first = self, .second = &s, .third = &subStats };
        self.forEachPage(tuple3Arg, travelStats);

        // Alloc stats can be computed from page counts and pageSize.
        s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize;
        s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize;

        // Add the max depth of sub-buckets to get total nested depth.
        s.depth += subStats.depth;
        // Add the stats for all sub-buckets.
        s.add(&subStats);
        return s;
    }

    // Travel the bucket and its sub-buckets to collect stats.
    fn travelStats(context: Tuple.t3(?*Bucket, ?*BucketStats, ?*BucketStats), p: *const page.Page, depth: usize) void {
        assert(context.first != null, "bucket is null", .{});
        assert(context.second != null, "bucket stats is null", .{});
        const b = context.first.?;
        const s = context.second.?;
        if (p.flags & consts.intFromFlags(consts.PageFlag.leaf) != 0) {
            s.keyN += @as(usize, p.count);

            // used totals the used bytes for the page.
            var used = page.Page.headerSize();
            std.log.debug("travelStats: depth: {d}, page: {any}", .{ depth, p });
            if (p.count != 0) {
                // If page has any elements, add all element headers.
                used += page.LeafPageElement.headerSize() * @as(usize, p.count - 1); // TODO why -1.

                // Add all element key, value sizes.
                // The computation takes advantages of the fact that the position
                // of the last element's key/value equals to the total of the sizes.
                // of all previous elements' keys and values.
                // It also includes the last element's header.
                const lastElement = p.leafPageElementRef(@as(usize, p.count - 1));
                used += @as(usize, lastElement.?.pos + lastElement.?.kSize + lastElement.?.vSize);
            }

            if (b._b.?.root == 0) {
                // For inlined bucket just update the inline stats
                s.InlineBucketInuse += used;
            } else {
                // For non-inlined bucket update all the leaf stats.
                s.LeafPageN += 1;
                s.LeafInuse += used;
                s.LeafOverflowN += @as(usize, p.overflow);

                // Collect stats from sub-buckets.
                // Do that by iterating over all element headers
                // looking for the ones with the bucketLeafFlag.
                for (0..p.count) |i| {
                    const elem = p.leafPageElementRef(i).?;
                    if (elem.flags & consts.BucketLeafFlag != 0) {
                        // For any bucket elements. open the element value
                        // and recursively call Stats on the contained bucket.
                        s.add(&b.openBucket(elem.value()).stats());
                    }
                }
            }
        } else if (p.flags & consts.intFromFlags(consts.PageFlag.branch) != 0) {
            s.BranchPageN += 1;
            const lastElement = p.branchPageElementRef(p.count - 1).?;

            // used totals the used bytes for the page.
            // Add header and all element header.
            var used = page.Page.headerSize() + (page.BranchPageElement.headerSize() * @as(usize, p.count - 1));

            // Add size of all keys and values.
            // Again, use the fact that last element's position euqals to
            // the total of key, value sizes of all previous elements.
            used += @as(usize, lastElement.pos + lastElement.kSize);
            s.BranchInuse += used;
            s.BranchOverflowN += @as(usize, p.overflow);
        }

        // Keep track of maximum page depth.
        if (depth + 1 > context.second.?.depth) {
            s.depth = (depth + 1);
        }
    }

    // Iterates over every page in a bucket, including inline pages.
    fn forEachPage(self: *Self, context: anytype, travel: fn (@TypeOf(context), p: *const page.Page, depth: usize) void) void {
        // If we have an inline page then just use that.
        if (self.page) |_p| {
            std.log.debug("forEachPage: depth: {d}, root: {d}", .{ 0, self._b.?.root });
            travel(context, _p, 0);
            return;
        }

        assert(self.tx != null, "tx closed", .{});
        // Otherwise traverse the page hierarchy.
        self.tx.?.forEachPage(self._b.?.root, 0, context, travel);
    }

    /// Iterators over every page （or node) in a bucket.
    /// This also include inline pages.
    pub fn forEachPageNode(self: *Self, context: anytype, travel: fn (@TypeOf(context), p: ?*const page.Page, n: ?*Node, depth: usize) void) void {
        // If we have an inline page or root node then just user that.
        if (self.page) |p| {
            travel(context, p, null, 0);
            return;
        }

        self._forEachPageNode(context, self._b.?.root, 0, travel);
    }

    // Recursively iterates over every page or node in a bucket and its nested buckets.
    fn _forEachPageNode(self: *Self, context: anytype, pgid: PgidType, depth: usize, travel: fn (@TypeOf(context), p: ?*const page.Page, n: ?*Node, depth: usize) void) void {
        const pNode = self.pageNode(pgid);

        // Execute function.
        travel(context, pNode.page, pNode.node, depth);

        // Recursively loop over children.
        if (pNode.page) |p| {
            if (p.flags & consts.intFromFlags(consts.PageFlag.branch) != 0) {
                for (0..p.count) |i| {
                    const elem = p.branchPageElementPtr(i);
                    self._forEachPageNode(context, elem.pgid, depth + 1, travel);
                }
            }
        } else if (!pNode.node.?.isLeaf) {
            for (pNode.node.?.inodes.items) |iNode| {
                self._forEachPageNode(context, iNode.pgid, depth + 1, travel);
            }
        }
    }

    /// Writes all the nodes for this bucket to dirty pages.
    pub fn spill(self: *Self) Error!void {
        // Spill all child buckets first.
        var itr = self.buckets.iterator();
        var value = std.ArrayList(u8).init(self.allocator);
        defer value.deinit();
        while (itr.next()) |entry| {
            value.resize(0) catch unreachable;
            std.log.info("Run at bucket({s}) spill!", .{entry.key_ptr.*});
            // If the child bucket is small enough and it has no child buckets then
            // write it inline into the parent bucket's page. Otherwise spill it
            // like a normal bucket and make the parent value a pointer to the page.
            if (entry.value_ptr.*.inlineable()) {
                const valBuffer = entry.value_ptr.*.write();
                value.appendSlice(valBuffer) catch unreachable;
                self.allocator.free(valBuffer);
                std.log.info("spill a inlineable bucket({s}) done!", .{entry.key_ptr.*});
            } else {
                try entry.value_ptr.*.spill(); // TODO Opz code
                // Update the child bucket header in this bucket.
                value.appendNTimes(0, _Bucket.size()) catch unreachable;
                const bt = _Bucket.init(value.items[0..]);
                bt.* = entry.value_ptr.*._b.?;
            }

            // Skip writing the bucket if there are no matterialized nodes.
            // If we delete a bucket ?
            if (entry.value_ptr.*.rootNode == null) {
                std.log.debug("the root node is null, skip it.", .{});
                continue;
            }

            // Update parent node.
            var c = self.cursor();
            const keyPairRef = c._seek(entry.key_ptr.*);
            assert(std.mem.eql(u8, entry.key_ptr.*, keyPairRef.key.?), "misplaced bucket header: {s} -> {s}", .{ std.fmt.fmtSliceHexLower(entry.key_ptr.*), std.fmt.fmtSliceHexLower(keyPairRef.key.?) });
            assert(keyPairRef.flag & consts.BucketLeafFlag != 0, "unexpeced bucket header flag: 0x{x}", .{keyPairRef.flag});
            _ = c.node().?.put(entry.key_ptr.*[0..], entry.key_ptr.*[0..], value.toOwnedSlice() catch unreachable, 0, consts.BucketLeafFlag);
            c.deinit();
        }

        // Ignore if there's not a materialized root node.
        if (self.rootNode == null) {
            std.log.debug("the rootNode is null", .{});
            return;
        }
        // Spill nodes.
        self.rootNode.?.spill() catch unreachable;
        self.rootNode = self.rootNode.?.root();
        // Update the root node for this bucket.
        assert(self.rootNode.?.pgid < self.tx.?.meta.pgid, "pgid ({}) above high water mark ({})", .{ self.rootNode.?.pgid, self.tx.?.meta.pgid });
        self._b.?.root = self.rootNode.?.pgid;
        std.log.info("the rootNode update to {d}, isLeaf:{}, inodes: {any}\n", .{ self._b.?.root, self.rootNode.?.isLeaf, self.rootNode.?.inodes.items.len });
    }

    // Returns true if a bucket is small enough to be written inline
    // and if it contains no subbuckets. Otherwise returns false.
    fn inlineable(self: *const Self) bool {
        const n = self.rootNode;
        // Bucket must only contain a single leaf node.
        if (n == null or !n.?.isLeaf) { // the inline node has not parent rootNode, because it inline.
            std.log.debug("the rootNode is null or not a leaf node: {d}", .{self._b.?.root});
            return false;
        }

        // Bucket is not inlineable if it contains subbuckets or if it goes beyond
        // our threshold for inline bucket size.
        var size = page.Page.headerSize();
        for (n.?.inodes.items) |inode| {
            size += page.LeafPageElement.headerSize() + inode.key.?.len;
            if (inode.value) |value| {
                size += value.len;
            }
            // include the bucket leaf flag
            if (inode.flags & consts.BucketLeafFlag != 0) {
                return false;
            } else if (size > self.maxInlineBucketSize()) {
                // the size of the bucket is greater than the maximum inline bucket size
                return false;
            }
        }

        return true;
    }

    // Returns the maximum total size of a bucket to make it a candidate for inlining.
    fn maxInlineBucketSize(self: *const Self) usize {
        return self.tx.?.getDB().pageSize / 4;
    }

    // Allocates and writes a bucket to a byte slice, *Note*! remember to free the memory
    fn write(self: *Self) []u8 {
        // Allocate the approprivate size.
        const n = self.rootNode.?;
        const value = self.allocator.alloc(u8, Bucket.bucketHeaderSize() + n.size()) catch unreachable;
        @memset(value, 0);

        // Write a bucket header.
        const _bt = _Bucket.init(value);
        _bt.* = self._b.?;
        assert(_bt.root == 0, "the bucket root is not eq 0", .{});

        // TODO may sure no children at the node and more check!.
        // Convert byte slice to a fake page and write the roor node.
        const p = Page.init(value[Bucket.bucketHeaderSize()..]);
        const written = n.write(p) + Page.headerSize();
        assert(written == n.size(), "the written size is not equal to the node size, written: {d}, need size: {d}", .{ written, n.size() });
        std.log.info("after write page, the value is {any}", .{value});
        return value;
    }

    /// Attemps to balance all nodes
    pub fn rebalance(self: *Self) void {
        var valueItr = self.nodes.valueIterator();
        while (valueItr.next()) |n| {
            n.*.rebalance();
        }
        var itr = self.buckets.valueIterator();
        while (itr.next()) |child| {
            child.*.rebalance();
        }
    }

    /// Returns the size of the bucket header.
    fn bucketHeaderSize() usize {
        return @sizeOf(_Bucket);
    }

    /// Recursively frees all pages in the bucket.
    pub fn free(self: *Self) void {
        if (self._b == null or self._b.?.root == 0) {
            return;
        }

        const trx = self.tx.?;
        self.forEachPageNode(trx, freeTravel);
        self._b.?.root = 0;
    }

    // Recursively frees all pages in the bucket.
    fn freeTravel(trx: *tx.TX, p: ?*const page.Page, n: ?*Node, _: usize) void {
        if (p) |_p| {
            // free the page
            trx.db.?.freelist.free(trx.meta.txid, _p) catch unreachable;
        } else {
            // free the node
            n.?.free();
        }
    }

    /// Removes all references to the old mmap.
    pub fn dereference(self: *Bucket) void {
        if (self.rootNode) |rNode| {
            rNode.root().?.dereference();
        }
        defer self.buckets.deinit();
        var itr = self.buckets.iterator();
        while (itr.next()) |entry| {
            _ = entry; // autofix
            // entry.value_ptr.*.dereference();
        }
    }

    /// Returns the in-memory node, if it exists.
    /// Otherwise returns the underlying page.
    pub fn pageNode(self: *Self, id: PgidType) PageOrNode {
        // Inline buckets have a fake page embedded in their value so treat them
        // differently. We'll return the rootNode (if available) or the fake page.
        if (self._b == null or self._b.?.root == 0) {
            std.log.info("this is a inline bucket, be embedded at page, pgid: {d}", .{id});
            assert(id == 0, "inline bucket non-zero page access(2): {} != 0", .{id});
            if (self.rootNode) |rNode| {
                return PageOrNode{ .page = null, .node = rNode };
            }
            return PageOrNode{ .page = self.page, .node = null };
        }

        // Check the node cache for non-inline buckets.
        if (self.nodes.get(id)) |cacheNode| {
            return PageOrNode{ .page = null, .node = cacheNode };
        }
        // Finally lookup the page from the transaction if the id's node is not materialized.
        return PageOrNode{ .page = self.tx.?.getPage(id), .node = null };
    }

    /// Creates a node from a page and associates it with a given parent.
    pub fn node(self: *Self, pgid: PgidType, parentNode: ?*Node) *Node {
        // Retrive node if it's already been created.
        if (self.nodes.get(pgid)) |_node| {
            return _node;
        }

        // Otherwise create a node and cache it.
        const n = Node.init(self.allocator);
        n.bucket = self;
        if (parentNode != null) {
            // the node is not the root node, so add it to the parent node's children that contact with it.
            parentNode.?.children.append(n) catch unreachable;
        } else {
            self.rootNode = n; // the node is the root node
        }
        // Use the page into the node and cache it.
        var p = self.page;
        if (p == null) {
            // if is inline bucket, the page is not null ???
            p = self.tx.?.getPage(pgid);
        }
        // Read the page into the node and cacht it.
        n.read(p.?);
        std.log.info("read node, pgid: {d}, ptr: 0x{x}, isTop: {}", .{ pgid, n.nodePtrInt(), parentNode == null });
        const entry = self.nodes.getOrPut(pgid) catch unreachable;
        assert(!entry.found_existing, "the node is already exist, pgid: {d}, ptr: 0x{x}", .{ pgid, n.nodePtrInt() });
        entry.value_ptr.* = n;
        // Update statistic.
        self.tx.?.stats.nodeCount += 1;
        return n;
    }

    /// #TODO
    pub fn print(self: *Self) void {
        const printStruct = .{
            .root = self._b.?.root,
            .sequence = self._b.?.sequence,
        };
        std.log.info("root: {}, sequence: {}, isInline bucket: {}", .{ printStruct.root, printStruct.sequence, self.page != null });
    }
};

// Represents the on-file represesntation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header, In the case of inline buckets, the "root" will be 0.
pub const _Bucket = packed struct {
    root: PgidType = 0, // page id of the bucket's root-level page, if the Bucket is embedded，it's root is zero.
    sequence: u64 = 0, // montotically incrementing. used by next_sequence().
    /// Init _Bucket with a given slice
    pub fn init(slice: []u8) *_Bucket {
        util.assert(slice.len >= _Bucket.size(), "slice is too short to init _Bucket", .{});
        const aligned_slice: []align(@alignOf(_Bucket)) u8 = @alignCast(slice);
        const ptr: *_Bucket = @ptrCast(aligned_slice.ptr);
        return ptr;
    }

    fn size() usize {
        return @sizeOf(_Bucket);
    }

    /// Init _Bucket with a given allocator. This is used for writable transaction.
    pub fn initWithAllocator(allocator: std.mem.Allocator) !*_Bucket {
        const self = try allocator.create(_Bucket);
        self.root = 0;
        self.sequence = 0;
        return self;
    }

    pub fn mustAligned(value: []const u8) void {
        const alignment = std.math.ceilPowerOfTwo(usize, @alignOf(_Bucket)) catch @alignOf(usize);
        const isAligned = @intFromPtr(value.ptr) % alignment == 0;
        util.assert(isAligned, "the value buffer is not aligned", .{});
    }

    /// Deinit _Bucket with a given allocator. This is used for writable transaction.
    pub fn deinit(_: _Bucket, _: std.mem.Allocator) void {
        // allocator.destroy(self);
        //self.* = undefined;
    }
};

/// Records statistics about resoureces used by a bucket.
pub const BucketStats = struct {
    // Page count statistics.
    BranchPageN: usize = 0, // number of logical branch pages.
    BranchOverflowN: usize = 0, // number of physical branch overflow pages
    LeafPageN: usize = 0, // number of logical leaf pages
    LeafOverflowN: usize = 0, // number of physical leaf overflow pages

    // Tree statistics.
    keyN: usize = 0, // number of keys/value pairs
    depth: usize = 0, // number of levels in B+tree

    // Page size utilization.
    BranchAlloc: usize = 0, // bytes allocated for physical branch pages
    BranchInuse: usize = 0, // bytes actually used for branch data
    LeafAlloc: usize = 0, // bytes allocated for physical leaf pages
    LeafInuse: usize = 0, // bytes actually used for leaf data

    // Bucket statistics
    BucketN: usize = 0, // total number of buckets including the top bucket
    InlineBucketN: usize = 0, // total number on inlined buckets
    InlineBucketInuse: usize = 0, // bytes used for inlined buckets (also accouted for in LeafInuse)

    // align value operation statistics
    alignValueN: usize = 0,
    // unalign value operation statistics
    unAlignValueN: usize = 0,

    /// Initializes a new bucket statistics.
    pub fn init() BucketStats {
        return BucketStats{};
    }

    /// Adds the statistics from another bucket to the current bucket.
    pub fn add(self: *BucketStats, other: *const BucketStats) void {
        self.BranchPageN += other.BranchPageN;
        self.BranchOverflowN += other.BranchOverflowN;
        self.LeafPageN += other.LeafPageN;
        self.LeafOverflowN += other.LeafOverflowN;
        self.keyN += other.keyN;
        if (self.depth < other.depth) {
            self.depth = other.depth;
        }
        self.BranchAlloc += other.BranchAlloc;
        self.BranchInuse += other.BranchInuse;
        self.LeafAlloc += other.LeafAlloc;
        self.LeafInuse += other.LeafInuse;

        self.BucketN += other.BucketN;
        self.InlineBucketN += other.InlineBucketN;
        self.InlineBucketInuse += other.InlineBucketInuse;
    }
};

// Recursively deletes all child buckets of a bucket.
fn traveBucket(bucket: *Bucket, keyPair: *const consts.KeyPair) Error!void {
    if (keyPair.value == null) {
        try bucket.deleteBucket(keyPair.key);
    }
    return null;
}
