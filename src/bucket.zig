const page = @import("./page.zig");
const tx = @import("./tx.zig");
const std = @import("std");
const Node = @import("./node.zig").Node;
const assert = @import("util.zig").assert;
const Tuple = @import("./consts.zig").Tuple;
const Tuple2 = Tuple.t2(?*page.Page, *Node);
const Cursor = @import("./cursor.zig").Cursor;
const consts = @import("./consts.zig");
const util = @import("./util.zig");
const Error = @import("./error.zig").Error;

// DefaultFilterPersent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
const DefaultFillPercent = 0.5;

pub const minFillPercent: f64 = 0.1;
pub const maxFillPercent: f64 = 1.0;

fn forEachPageNodeInner(_: anytype, _: *page.Page, _: *Node, _: usize) void {}

// Represents a collection of key/value pairs inside the database.
pub const Bucket = struct {
    _b: ?*_Bucket = null,
    tx: ?*tx.TX, // the associated transaction
    buckets: std.AutoHashMap([]u8, *Bucket), // subbucket cache
    nodes: std.AutoHashMap(page.PgidType, *Node), // node cache
    rootNode: ?*Node = null, // materialized node for the root page.
    page: ?*page.Page = null, // inline page reference

    // Sets the thredshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amout if you know that your write workloads are mostly append-only.
    //
    // This is non-presisted across transactions so it must be set in every TX.
    fillPercent: f64 = DefaultFillPercent,

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(_tx: *tx.TX) *Bucket {
        const b = _tx.db.?.allocator.create(Self) catch unreachable;
        b._b = _Bucket{};
        b.tx = _tx;
        b.allocator = _tx.db.?.allocate;
        if (_tx.writable) { // TODO ?
            b.buckets = std.AutoHashMap([]u8, *Bucket).init(b.allocator);
            b.nodes = std.AutoHashMap(page.PgidType, *Node).init(b.allocator);
        }
        return b;
    }

    pub fn deinit(self: *Self) void {
        self.buckets.deinit();
        self.nodes.deinit();
        self.allocator.free(self.name);
        if (self.tx.?.writable) {
            self.tx.?.getDB().allocator.destroy(self._b);
        }
    }

    /// Create a cursor associated with the bucket.
    /// The cursor is only valid as long as the transaction is open.
    /// Do not use a cursor after the transaction is closed.
    pub fn cursor(self: *Self) *Cursor {
        // Update transaction statistics.
        self.tx.?.stats.cursor_count += 1;
        // Allocate and return a cursor.
        return Cursor.init(self.allocator, self);
    }

    /// Retrives a nested bucket by name.
    /// Returns nil if the bucket does not exits.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn getBucket(self: *Self, name: []u8) ?*Bucket {
        if (self.buckets.get(name)) |_bucket| {
            return _bucket;
        }

        // Move cursor to key.
        const _cursor = self.cursor();
        const keyPairRef = _cursor._seek(name);
        if (keyPairRef.key == null) {
            return null;
        }

        // Return nil if the key dosn't exist or it is not a bucket.
        if (std.mem.eql(u8, name, keyPairRef.key.?) or keyPairRef.first & consts.BucketLeafFlag == 0) {
            return null;
        }

        std.log.info("get a new bucket: {}, current page: {}", .{ name, self.page.?.id });
        const child = self.openBucket(keyPairRef.second);

        self.buckets.put(util.cloneBytes(self.allocator, name), child);
        return child;
    }

    /// Helper method that re-interprets a sub-bucket value
    /// from a parent into a Bucket
    pub fn openBucket(self: *Self, value: []u8) *Bucket {
        var child = Bucket.init(self.tx.?);
        // TODO
        // If unaligned load/stores are broken on this arch and value is
        // unaligned simply clone to an aligned byte array.

        // If this is a writable transaction then we need to copy the bucket entry.
        // Read-Only transactions can point directly at the mmap entry.
        if (self.tx.?.writable) {
            self._b = _Bucket.init(util.cloneBytes(self.tx.?.db.?.allocator, value));
        } else {
            self._b = _Bucket.init(value);
        }

        // Save a reference to the inline page if the bucket is inline.
        if (self._b.root == 0) {
            child.page = page.Page.init(value); // TODO
            std.log.info("Save a reference to the inline page if the bucket is inline, the page is {}", .{child.page.?.id});
        }

        return child;
    }

    /// Creates a new bucket at the given key and returns the new bucket.
    /// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucket(self: *Self, key: []u8) Error!*Bucket {
        if (self.tx.?.db == null) {
            return Error.TxClosed;
        } else if (!self.tx.?.writable) {
            return Error.TxNotWriteable;
        } else if (key.len == 0) {
            return Error.BucketNameRequired;
        }

        // Move cursor to correct position.
        const c = self.cursor();
        std.log.info("first levels: {}", .{c._bucket._b.?.root});
        const keyPairRef = c._seek(key);

        // Return an error if there is an existing key.
        if (keyPairRef.first != null and std.mem.eql(u8, key, keyPairRef.first.?)) {
            if (keyPairRef.third & consts.BucketLeafFlag != 0) {
                return Error.BucketExists;
            }
            return Error.IncompactibleValue;
        }

        // Create empty, inline bucket.
        const newBucket = Bucket.init(self.tx);
        newBucket.rootNode = Node.init(self.allocator);
        newBucket.rootNode.?.isLeaf = true;

        const value = newBucket.write();
        // Insert into node
        const cpKey = util.cloneBytes(self.allocator, key);
        c.node().?.put(cpKey, cpKey, value, 0, consts.BucketLeafFlag);

        // Since subbuckets are not allowed on inline buckets, we need to
        // dereference the inline page, if it exists. This will cause the bucket
        // to be treated as regular, non-inline bucket for the rest of the tx.
        // FIXME: why
        self.page = null;

        return self.getBucket(key);
    }

    /// Creates a new bucket if it doesn't already exist and returns a reference to it.
    /// Returns an error if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucketIfNotExists(self: *Self, key: []u8) Error!*Bucket {
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
    pub fn deleteBucket(self: *Self, key: []u8) Error!void {
        if (self.tx.db == null) {
            return Error.TxClosed;
        } else if (!self.tx.?.writable) {
            return Error.TxNotWriteable;
        }

        // Move cursor to correct position.
        const c = self.cursor();
        const keyPairRef = c._seek(key);

        // Returnsively delete all child buckets.
        const child = self.getBucket(key).?;
        child.forEach();
    }

    // Returns the maximum total size of a bucket to make it a candidate for inlining.
    fn maxInlineBucketSize(self: *const Self) usize {
        return self.tx.?.getDB().pageSize / 4;
    }

    // Allocates and writes a bucket to a byte slice.
    fn write(self: *Self) []u8 {
        // Allocate the approprivate size.
        const n = self.rootNode;
        const value = self.allocator.alloc(u8, Bucket.bucketHeaderSize()) catch unreachable;
        const _bt = _Bucket.init(value);
        _bt.* = self._b.?.*;
        const p = page.Page.init(value[Bucket.bucketHeaderSize()..]);
        n.?.write(p);

        return value;
    }

    fn bucketHeaderSize() usize {
        return @sizeOf(_Bucket);
    }

    // Recursively frees all pages in the bucket.
    pub fn free(self: *Self) void {
        if (self._b.root == 0) {
            return;
        }

        //const trx = self.tx.?;
        //self.forEachPageNode()
    }

    /// Removes all references to the old mmap.
    pub fn dereference(self: *Self) void {
        if (self.rootNode) |rNode| {
            rNode.root().?.dereference();
        }
        var itr = self.buckets.iterator();
        while (itr.next()) |entry| {
            entry.value_ptr.dereference();
        }
    }

    /// Returns the in-memory node, if it exists.
    pub fn pageNode(self: *Self, id: page.PgidType) Tuple2 {
        // Inline buckets have a fake page embedded in their value so treat them
        // differently. We'll return the rootNode (if available) or the fake page.
        if (self._b.root == 0) {
            std.log.info("this is a inline bucket, embed at page({})", .{id});
            assert(id == 0, "inline bucket non-zero page access(2): {} != 0", .{id});
            if (self.rootNode) |rNode| {
                return Tuple2{ .first = null, .second = rNode };
            }
            return Tuple2{ .first = self.page, .second = null };
        }

        // Check the node cache for non-inline buckets.
        if (self.nodes.get(id)) |cacheNode| {
            return Tuple2{ .first = null, .second = cacheNode };
        }
        // Finally lookup the page from the transaction if no node is materialized.
        return Tuple2{ .first = self.tx.?.getPage(id), .second = null };
    }

    // Creates a node from a page and associates it with a given parent.
    pub fn node(self: *Self, pgid: page.PgidType, parentNode: ?*Node) *Node {
        assert(self.nodes.count() > 0, "nodes map expected!", .{});

        // Retrive node if it's already been created.
        if (self.nodes.get(pgid)) |_node| {
            return _node;
        }

        // Otherwise create a node and cache it.
        const n = Node.init(self.allocator);
        if (parentNode != null) {
            parentNode.?.children.append(n) catch unreachable;
        } else {
            self.rootNode = n;
        }
        // Use the page into the node and cache it.
        var p = self.page;
        if (p == null) {
            // if is inline bucket, the page is not null ???
            p = self.tx.?.getPage(pgid);
        }

        // Read the page into the node and cacht it.
        n.read(p.?);
        self.nodes.put(pgid, n) catch unreachable;

        // Update statistic.
        self.tx.?.stats.nodeCount += 1;
        return n;
    }
};

// Represents the on-file represesntation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header, In the case of inline buckets, the "root" will be 0.
pub const _Bucket = packed struct {
    root: page.PgidType = 0, // page id of the bucket's root-level page
    sequence: u64 = 0, // montotically incrementing. used by next_sequence().

    pub fn init(slice: []u8) *_Bucket {
        const ptr: *_Bucket = @ptrCast(@alignCast(slice));
        return ptr;
    }
};
