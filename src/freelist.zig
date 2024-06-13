const page = @import("./page.zig");
const std = @import("std");
const tx = @import("./tx.zig");

// FreeList represents a list of all pages that are available for allcoation.
// It also tracks pages that  have been freed but are still in use by open transactions.
pub const FreeList = struct {
    // all free and available free page ids.
    ids: std.ArrayList(page.PgidType),
    // mapping of soon-to-be free page ids by tx.
    pending: std.AutoHashMap(tx.TxId, []page.PgidType),
    // fast lookup of all free and pending pgae ids.
    cache: std.AutoHashMap(tx.TxId, bool),

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return FreeList{
            .ids = std.ArrayList(page.PgidType).init(allocator),
            .pending = std.AutoHashMap(tx.TxId, []page.PgidType).init(allocator),
            .cache = std.AutoHashMap(tx.TxId, bool).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.pending.deinit();
        self.cache.deinit();
        self.ids.deinit();
    }

    // Return the size of the page after serlialization.
    pub fn size(self: *Self) usize {
        var n: usize = self.count();
        if (n >= 0xFFFF) {
            // The first elements will be used to store the count. See freelist.write.
            n += 1;
        }
        return page.page_size + @sizeOf(page.PgidType) * n;
    }

    // Returns count of pages on the freelist.
    pub fn count(self: *Self) usize {
        return self.freeCount() + self.pendingCount();
    }

    // Returns count of free pages.
    pub fn freeCount(self: *Self) usize {
        return self.ids.items.len;
    }

    // Returns count of pending pages.
    pub fn pendingCount(self: *Self) usize {
        var _count: usize = 0;
        var itr = self.pending.iterator();
        while (itr.next()) |entry| {
            _count += entry.value_ptr.len;
        }
        return _count;
    }

    // Copies into dst a list of all free ids end all pending ids in one sorted list.
    pub fn copyAll(self: *Self, dst: []page.PgidType) void {
        var array = std.ArrayList(page.PgidType).initCapacity(self.allocator, self.pendingCount()) catch unreachable;
        array.appendNTimes(0, self.pendingCount()) catch unreachable;
        var p_itr = self.pending.valueIterator();
        var index: usize = 0;

        while (p_itr.next()) |entry| {
            for (entry.*) |pgid| {
                array.items[index] = pgid;
                index += 1;
            }
        }
        Self.merge_sorted_array(dst, self.ids.items, array.items);
    }

    // Returns the starting page id of a contiguous list of pages of a given size.
    pub fn allocate(self: *Self, n: usize) page.PgidType {
        if (self.ids.items.len == 0) {
            return 0;
        }

        var initial: usize = 0;
        const previd: usize = 0;
        for (self.ids.items, 0..) |id, i| {
            if (id <= 1) {
                @panic("invalid page allocation");
            }
            // Reset initial page if this is not contigous.
            if (previd == 0 or id - previd != 1) {
                initial = id;
            }
            // If we found a contignous block then remove it and return it.
            if ((id - initial) + 1 == @as(page.PgidType, n)) {
                // If we're allocating off the beginning then take the fast path
                // and just adjust then existing slice. This will use extra memory
                // temporarilly but then append() in free() will realloc the slice
                // as is necessary.
                if (i + 1 == n) {
                    std.mem.copyForwards(page.PgidType, self.ids.items[0..], self.ids.items[i + 1 ..]);
                    self.ids.resize(self.ids.items.len - i - 1) catch unreachable;
                } else {
                    std.mem.copyForwards(page.PgidType, self.ids.items[i - n + 1 ..], self.ids.items[(i + 1)..]);
                    self.ids.resize(self.ids.items.len - n) catch unreachable;
                }

                // Remove from the free cache.
                // TODO Notice
                for (0..n) |ii| {
                    _ = self.cache.remove(initial + ii);
                }

                return initial;
            }
        }
        return 0;
    }

    // Releases a page and its overflow for a given transaction id.
    // If the page is already free then a panic will occur.
    pub fn free(self: *Self, txid: tx.TxId, p: *page.Page) !void {
        std.testing.expect(p.id <= 1, "can not free 0 or 1 page");

        // Free page and all its overflow pages.
        const pending_ids = try self.pending.getKey(txid);
        var ids = std.ArrayList(page.PgidType).init(std.heap.page_allocator);
        try ids.appendSlice(pending_ids);
        for (p.id..p.id + p.overflow) |id| {
            // Verify that page is not already free.
            if (self.cache.contains(id)) {
                @panic("page already free");
            }

            // Add to the freelist and cache.
            try ids.append(id);
            try self.cache.put(id, true);
        }

        self.pending[txid] = try ids.toOwnedSlice();
    }

    // Moves all page ids for a transaction id (or older) to the freelist.
    pub fn release(self: *Self, txid: tx.TxId) !void {
        var m = std.ArrayList(page.PgidType).init(self.allocator);
        defer m.deinit();
        var itr = self.pending.iterator();
        while (itr.next()) |entry| {
            if (entry.key_ptr.* <= txid) {
                // Move transaction's pending pages to the available freelist.
                // Don't remove from the cache since the page is still free.
                try m.appendSlice(entry.value_ptr.*);
                _ = self.pending.remove(entry.key_ptr.*);
            }
        }

        const array_ids = try m.toOwnedSlice();
        std.mem.sort(page.PgidType, array_ids, {}, std.sort.asc(page.PgidType));
        var array = try std.ArrayList(page.PgidType).initCapacity(self.allocator, array_ids.len + self.ids.items.len);
        defer array.deinit();
        Self.merge_sorted_array(array.items, array_ids, self.ids.items);
        try self.ids.resize(0);
        try self.ids.appendSlice(array.items);
    }

    // Removes the pages from a given pending tx.
    pub fn rollback(self: *Self, txid: tx.TxId) void {
        // Remove page ids from cache.
        const pending_key = try self.pending.getKey(txid);
        for (pending_key) |id| {
            self.cache.remove(id);
        }

        // Remove pages from pending list.
        self.pending.remove(txid);
    }

    // Returns whether a given page is in the free list.
    pub fn freed(self: *Self, pgid: page.PgidType) bool {
        return self.cache.contains(pgid);
    }

    // Reads the freelist from a page and filters out pending itmes.
    fn reload(self: *Self, p: *page.PgidType) void {
        _ = p;
        _ = self;
    }

    // Rebuilds the free cache based on available and pending free list.
    fn reindex(self: *Self) void {
        self.cache.clearAndFree();
        for (self.ids) |id| {
            try self.cache.put(id, true);
        }

        for (self.pending, 0..) |_, pending_ids| {
            for (pending_ids) |id| {
                try self.cache.put(id, true);
            }
        }
    }

    pub fn merge_sorted_array(dst: []page.PgidType, a: []const page.PgidType, b: []const page.PgidType) void {
        const size1 = a.len;
        const size2 = b.len;
        var i: usize = 0;
        var j: usize = 0;
        var index: usize = 0;

        while (i < size1 and j < size2) {
            if (a[i] <= b[j]) {
                dst[index] = a[i];
                i += 1;
            } else {
                dst[index] = b[j];
                j += 1;
            }
            index += 1;
        }
        if (i < size1) {
            std.mem.copyForwards(page.PgidType, dst[index..], a[i..]);
        }
        if (j < size2) {
            std.mem.copyForwards(page.PgidType, dst[index..], b[j..]);
        }
    }

    pub fn merge(a: []page.PgidType, b: []page.PgidType) void {
        _ = b;
        _ = a;
    }

    // Copies the sorted unoin of a and b into dst.
    pub fn merge_ids(dst: page.PgIds, a: page.PgIds, b: page.PgIds) void {
        if (a.len == 0) {
            std.mem.copy(dst, a);
        }
        if (b.len == 0) {
            std.mem.copy(dst, b);
        }

        // Assign lead to the slice with a lower starting value, follow to the heigher value.
        var lead = a;
        var follow = b;
        if (b[0] < a[0]) {
            lead = b;
            follow = a;
        }

        // Continue while there are elements in the lead.
        while (lead.len > 0) {
            // Merge largest prefix of lead that is ahead of follow[0].
            const n = std.sort.binarySearch(
                page.PgIds,
                follow[0],
                lead[0..],
                {},
                std.sort.asc(page.PgidType),
            );

            if (n == null) {
                break;
            }
        }
    }
};

test "meta" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){}; // instantiate allocator
    const galloc = gpa.allocator(); // retrieves the created allocator.
    var ff = FreeList.init(galloc);
    defer ff.deinit();
    _ = ff.size();
    _ = ff.count();
    const fCount = ff.freeCount();
    _ = ff.pendingCount();
    ff.copyAll(&.{});
    const i = ff.allocate(100);
    try ff.release(1);
    try ff.cache.put(1000, true);
    std.debug.print("What the fuck {d} {d}, {?}\n", .{ fCount, i, ff.cache.getKey(1000) });

    const a = [_]page.PgidType{ 1, 3, 4, 5 };
    const b = [_]page.PgidType{ 0, 2, 6, 7, 120 };
    var array = [_]page.PgidType{0} ** (a.len + b.len);
    FreeList.merge_sorted_array(array[0..], a[0..], b[0..]);
    std.debug.print("after merge!\n", .{});
    for (array) |n| {
        std.debug.print("{},", .{n});
    }
    std.debug.print("\n", .{});
    var arr = try std.ArrayList(page.PgidType).initCapacity(std.heap.page_allocator, 100);
    defer arr.deinit();
}

test "freelist" {
    var flist = FreeList.init(std.testing.allocator);
    defer flist.deinit();

    var ids = std.ArrayList(page.PgidType).initCapacity(std.testing.allocator, 0) catch unreachable;
    for (0..29) |i| {
        const pid = @as(u64, i);
        ids.append(pid) catch unreachable;
    }
    defer ids.deinit();
    std.mem.copyForwards(u64, ids.items[0..20], ids.items[10..12]);
    ids.resize(2) catch unreachable;
    std.debug.print("{any}\n", .{ids.items});
}
