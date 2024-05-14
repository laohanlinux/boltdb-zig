const std = @import("std");
const db = @import("./db.zig");
const bucket = @import("./bucket.zig");
// Represents the internal transaction indentifier.
pub const TxId = u64;

// Represent a read-only or read/write transaction on the database.
// Read-only transactions can be used for retriving values for keys and creating cursors.
pub const TX = struct {
    writable: bool,
    managed: bool,
    db: *db.DB,
    meta: *db.Meta,

    stats: TxStats,

    root: bucket.Bucket,
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
    rebalance_time: std.time.Instant,

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
