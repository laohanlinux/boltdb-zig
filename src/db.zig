const std = @import("std");
const page = @import("./page.zig");
const tx = @import("./tx.zig");
const errors = @import("./error.zig");
const bucket = @import("./bucket.zig");

// Represents a marker value to indicate that a file is a Bolt DB.
const Magic = 0xED0CDAED;
const Version = 1;

pub const Meta = packed struct {
    magic: u32,
    version: u32,
    page_size: u32,
    flags: u32,
    root: bucket._Bucket,
    free_list: page.pgid_type,
    pgid: page.pgid_type,
    txid: tx.TxId,
    check_sum: u64,

    const Self = @This();
    pub const header_size = @sizeOf(Meta);

    pub fn validate(self: *Self) errors.Error!void {
        if (self.magic != Magic) {
            return errors.Error.Invalid;
        } else if (self.version != Version) {
            return errors.Error.VersionMismatch;
        } else if (self.check_sum != 0 and self.check_sum != self.sum64()) {
            return errors.Error.CheckSum;
        }

        return;
    }

    pub fn sum64(self: *Self) u64 {
        const ptr = @fieldParentPtr(Meta, "check_sum", self);
        const slice = @intToPtr([ptr - self]u8, ptr);
        const crc32 = std.hash.Crc32.hash(slice);
        return @as(crc32, u64);
    }

    // Writes the meta onto a page.
    pub fn write(self: *Self, p: *page.Page) void {
        if (self.root.root >= self.pgid) {
            unreachable;
        } else if (self.free_list >= self.pgid) {
            unreachable;
        }
        // Page id is either going to be 0 or 1 which we can determine by the transaction ID.
        p.id = @as(page.pgid_type, self.txid % 2);
        p.flags |= page.PageFlage.meta;

        // Calculate the checksum.
        self.check_sum = self.sum64();
        var meta = p.meta();
        meta.* = self.*;
        return;
    }
};

test "meta" {}
