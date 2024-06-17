const page = @import("./page.zig");
const tx = @import("./tx.zig");
const std = @import("std");
const Node = @import("./node.zig").Node;
const assert = @import("util.zig").assert;

// DefaultFilterPersent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
const DefaultFillPercent = 0.5;

pub const minFillPercent: f64 = 0.1;
pub const maxFillPercent: f64 = 1.0;

fn forEachPageNodeInner(_: anytype, _: *page.Page, _: *Node, _: usize) void {}

// Represents a collection of key/value pairs inside the database.
pub const Bucket = struct {
    _b: _Bucket = _Bucket{},
    tx: ?*tx.TX, // the associated transaction
    buckets: std.AutoHashMap([]u8, *Bucket), // subbucket cache
    nodes: std.AutoHashMap(page.PgidType, *Node), // node cache
    rootNode: ?*Node, // materialized node for the root page.
    page: ?*page.Page, // inline page reference

    // Sets the thredshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amout if you know that your write workloads are mostly append-only.
    //
    // This is non-presisted across transactions so it must be set in every TX.
    fillPercent: f64 = 0.50,

    allocator: std.mem.Allocator,

    const Self = @This();

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

    pub fn pageNode(self: *Self, id: page.PgidType) std.meta.Tuple(&.{ ?*page.Page, ?*Node }) {
        // Inline buckets have a fake page embedded in their value so treat them
        // differently. We'll return the rootNode (if available) or the fake page.
        if (self._b.root == 0) {
            assert(id != 0, "inline bucket non-zero page access(2): {} != 0", .{id});
            if (self.rootNode) |rNode| {
                return .{ null, rNode };
            }
            return .{ self.page, null };
        }

        // Check the node cache for non-inline buckets.
        if (self.nodes.get(id)) |cacheNode| {
            return .{ null, cacheNode };
        }
        // Finally lookup the page from the transaction if no node is materialized.
        return .{ self.tx.?.getPage(id), null };
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
};
