const std = @import("std");
const db = @import("./db.zig");
const bucket = @import("./bucket.zig");
const page = @import("./page.zig");
const Cursor = @import("./cursor.zig").Cursor;
const consts = @import("./consts.zig");
// Represents the internal transaction indentifier.
pub const TxId = u64;

// Represent a read-only or read/write transaction on the database.
// Read-only transactions can be used for retriving values for keys and creating cursors.
pub const TX = struct {
    writable: bool,
    managed: bool,
    db: ?*db.DB,
    meta: *db.Meta,
    pages: std.AutoHashMap(page.PgidType, *page.Page),

    stats: TxStats,

    root: bucket.Bucket,

    const Self = @This();

    pub fn init(_db: *db.DB) *Self {
        var self = _db.allocator.create(Self) catch unreachable;
        self.db = _db;
        self.pages = std.AutoHashMap(page.PgidType, *page.Page).init(_db.allocate);

        // Copy the meta page since it can be changed by the writer.
        self.meta = _db.allocate.create(db.Meta) catch unreachable;
        _db.getMeta().copy(self.meta);

        // Copy over the root bucket.
        self.root = bucket.Bucket.init();
        self.root._b.root = self.meta.root;
        std.log.info("tx's root bucket {}", .{self.root._b.root});

        // Increment the transaction id and add a page cache for writable transactions.
        if (self.writable) {
            self.pages = std.AutoHashMap(page.PgidType, *page.Page).init(self.db.?.allocate);
            self.meta.txid += 1;
        }
        return self;
    }

    /// Returns the current database size in bytes as seen by this transaction.
    pub fn size(self: *const Self) u64 {
        return self.meta.pgid * @as(u64, self.db.?.pageSize);
    }

    /// Creates a cursor assosicated with the root bucket.
    /// All items in the cursor will return a nil value because all root bucket keys point to buckets.
    /// The cursor is only valid as long as the transaction is open.
    /// Do not use a cursor after the transaction is closed.
    pub fn cursor(self: *Self) *Cursor {
        return self.root.cursor();
    }

    /// Iterates over every page within a given page and executes a function.
    pub fn forEach(self: *Self, comptime CTX: type, c: CTX, id: page.PgidType, depth: usize, f: fn (c: CTX, *page.Page, usize) void) void {
        const p = self.getPage(id);
        // Execute function
        f(c, p, depth);
        // recursively loop over children
        if (p.flags & page.intFromFlags(page.PageFlage.branch) != 0) {
            for (0..p.count) |i| {
                const elem = p.branchPageElementPtr(i);
                self.forEach(CTX, c, elem.pgid, depth + 1, f);
            }
        }
    }

    /// Iterates over every page within a given page and executes a function.
    pub fn forEachPage(self: *Self, pgid: page.PgidType, depth: usize, f: fn (p: *const page.Page, n: usize) void) void {
        const p = self.getPage(pgid);

        // Execute function.
        f(p, depth);

        // Recursively loop over children.
        if (p.flags & consts.intFromFlags(consts.PageFlage.branch) != 0) {
            for (p.branchPageElements().?) |elem| {
                self.forEachPage(elem.pgid, depth + 1, f);
            }
        }
    }

    /// Returns a reference to the page with a given id.
    /// If page has been written to then a temporary buffered page is returned.
    pub fn getPage(self: *Self, id: page.PgidType) *page.Page {
        // Check the dirty pages first.
        if (self.pages.get(id)) |p| {
            return p;
        }
        // Otherwise return directly form the mmap.
        return self.db.?.pageById(id);
    }

    pub fn getID(self: *const Self) u64 {
        self.meta.txid;
    }

    pub fn getDB(self: *Self) *db.DB {
        return self.db;
    }

    /// Returns current database size in bytes as seen by this transaction.
    pub fn getSize(self: *const Self) usize {
        self.db.pageSize * @as(usize, self.meta.txid);
    }

    pub fn writable(self: *const Self) bool {
        return self.writable;
    }

    pub fn allocate(self: *Self, count: usize) !*page.Page {
        const p = self.db.allocatePage(count);

        return p;
    }
};

// Represents statusctics about the actiosn performed by the transaction
pub const TxStats = packed struct {
    // Page statistics.
    page_count: usize, // number of page allocations
    page_alloc: usize, // total bytes allocated

    // Cursor statistics
    cursor_count: usize, // number of cursors created

    // Node statistics.
    nodeCount: usize, // number of node allocations
    nodeDeref: usize, // number of node dereferences

    // Rebalance statstics
    rebalance: usize, // number of node rebalances
    rebalance_time: u64,

    // Split/Spill statistics
    split: usize, // number of nodes split
    spill: usize, // number of nodes spilled
    spill_time: i64, // total time spent spilling

    // Write statistics
    write: usize, // number of writes performed
    write_time: usize, // total time spent writing to disk

    const Self = @This();
    pub fn add(self: *Self, other: *TxStats) void {
        self.page_count += other.page_count;
        self.page_alloc += other.page_alloc;
        self.cursor_count += other.cursor_count;
        self.nodeCount += other.nodeCount;
        self.nodeDeref += other.nodeDeref;
        self.rebalance += other.rebalance;
        self.rebalance_time += other.rebalance_time;
        self.split += other.split;
        self.spill += other.spill;
        self.spill_time += other.spill_time;
        self.write += other.write;
        self.write_time += other.write_time;
    }

    // Calculates and returns the difference between two sets of transaction stats.
    // This is useful when obtaning stats at two different points and time and
    // you need the performance counters that occurred within that time span.
    pub fn sub(self: *Self, other: *TxStats) Self {
        return TxStats{
            .page_count = self.page_count - other.page_count,
            .page_alloc = self.page_alloc - other.page_alloc,
            .cursor_count = self.cursor_count - other.cursor_count,
            .nodeCount = self.nodeCount - other.nodeCount,
            .nodeDeref = self.nodeDeref - other.nodeDeref,
            .rebalance = self.rebalance - other.rebalance,
            .rebalance_time = self.rebalance_time - other.rebalance_time,
            .split = self.split - other.split,
            .spill = self.spill - other.spill,
            .spill_time = self.spill_time - other.spill_time,
            .write = self.write - other.write,
            .write_time = self.write_time - other.write_time,
        };
    }
};

fn tEach(_: void, _: *page.Page, depth: usize) void {
    std.debug.print("Hello Word: {}\n", .{depth});
}

test "forEach" {
    const tx = std.testing.allocator.create(TX) catch unreachable;
    defer std.testing.allocator.destroy(tx);
    tx.forEach(void, {}, 1, 100, tEach);
}
