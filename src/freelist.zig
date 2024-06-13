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

    const Self = @This();
    pub fn new(allocator: std.mem.Allocator) Self {
        return FreeList{
            .ids = std.ArrayList(page.PgidType).init(allocator),
            .pending = std.AutoHashMap(tx.TxId, []page.PgidType).init(allocator),
            .cache = std.AutoHashMap(tx.TxId, bool).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.pending.deinit();
        self.cache.deinit();
        self.ids.deinit();
    }

    // Return the size of the page after serlialization.
    pub fn size(self: *Self) isize {
        var n = self.count();
        if (n >= 0xFFFF) {
            // The first elements will be used to store the count. See freelist.write.
            n += 1;
        }
        return page.page_size + @sizeOf(page.pgid_type) * n;
    }

    // Returns count of pages on the freelist.
    pub fn count(self: *Self) usize {
        return self.free_count() + self.pending_count();
    }

    // Returns count of free pages.
    pub fn free_count(self: *Self) usize {
        return self.ids.len;
    }

    // Returns count of pending pages.
    pub fn pending_count(self: *Self) usize {
        var _count = 0;
        for (self.pending) |el| {
            _count += el.len;
        }
        return _count;
    }

    // Copies into dst a list of all free ids end all pending ids in one sorted list.
    pub fn copy_all(self: *Self, dst: []page.pgid_type) void {
        var array = [:self.pending_count()]page.pgid_type{};
        var p_itr = self.pending.valueIterator();
        var index = 0;

        while (p_itr.next()) |entry| {
            for (entry) |pgid| {
                array[index] = pgid;
                index += 1;
            }
        }
        Self.merge_sorted_array(dst, self.ids, array);
    }

    // Returns the starting page id of a contiguous list of pages of a given size.
    pub fn allocate(self: *Self, n: usize) page.PgidType {
        if (self.ids.len == 0) {
            return 0;
        }

        var initial: usize = 0;
        const previd: usize = 0;
        for (self.ids, 0..) |i, id| {
            if (id <= 1) {
                unreachable;
            }
            // Reset initial page if this is not contigous.
            if (previd == 0 or id - previd != 1) {
                initial = id;
            }
            // If we found a contignous block then remove it and return it.
            if ((id - initial) + 1 == @as(page.pgid_type, n)) {
                // If we're allocating off the beginning then take the fast path
                // and just adjust then existing slice. This will use extra memory
                // temporarilly but then append() in free() will realloc the slice
                // as is necessary.
                if (i + 1 == n) {
                    self.ids = self.ids[i + 1 ..];
                } else {
                    std.mem.copyForwards(page.pgid_type, self.ids[i - n + 1 ..], self.ids[i + 1 ..]);
                    self.ids = self.ids[0 .. self.ids.len - n];
                }

                // Remove from the free cache.
                // TODO Notice
                for (0..n) |ii| {
                    self.cache.remove(initial + ii);
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
        var ids = std.ArrayList(page.pgid_type).init(std.heap.page_allocator);
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
        var m = std.ArrayList(page.pgid_type).init(std.heap.page_allocator);
        for (self.pending, 0..) |tid, ids| {
            if (tid <= txid) {
                // Move transaction's pending pages to the available freelist.
                // Don't remove from the cache since the page is still free.
                try m.appendSlice(ids);
                self.pending.remove(tid);
            }
        }

        const array_ids = try m.toOwnedSlice();
        std.sort.sort(page.pgid_type, array_ids, .{}, std.sort.asc(page.pgid_type));
        const array = [_]page.pgid_type{0} ** (array_ids.len + self.ids.len);
        Self.merge_sorted_array(array, array_ids, self.ids);
        self.ids = array;
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
    pub fn freed(self: *Self, pgid: page.pgid_type) bool {
        return self.cache.contains(pgid);
    }

    // Reads the freelist from a page and filters out pending itmes.
    fn reload(self: *Self, p: *page.pgid_type) void {
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

    pub fn merge_sorted_array(dst: []page.pgid_type, a: []const page.pgid_type, b: []const page.pgid_type) void {
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
            std.mem.copyForwards(page.pgid_type, dst[index..], a[i..]);
        }
        if (j < size2) {
            std.mem.copyForwards(page.pgid_type, dst[index..], b[j..]);
        }
    }

    pub fn merge(a: []page.pgid_type, b: []page.pgid_type) void {
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
                std.sort.asc(page.pgid_type),
            );

            if (n == null) {
                break;
            }
        }
    }

    // free memory
    pub fn drop(self: *Self) void {
        self.cache.deinit();
        self.pending.deinit();
    }
};

test "meta" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){}; // instantiate allocator
    const galloc = gpa.allocator(); // retrieves the created allocator.
    var ff = FreeList.new(galloc);
    defer ff.drop();
    try ff.cache.put(1000, true);
    std.debug.print("What the fuck {?}\n", .{ff.cache.getKey(1000)});

    const a = [_]page.PgidType{ 1, 3, 4, 5 };
    const b = [_]page.PgidType{ 0, 2, 6, 7, 120 };
    var array = [_]page.PgidType{0} ** (a.len + b.len);
    FreeList.merge_sorted_array(array[0..], a[0..], b[0..]);
    std.debug.print("after merge!\n", .{});
    for (array) |n| {
        std.debug.print("{},", .{n});
    }
    std.debug.print("\n", .{});
    var arr = try std.ArrayList(page.pgid_type).initCapacity(std.heap.page_allocator, 100);
    defer arr.deinit();
}
