const page = @import("./page.zig");
const tx = @import("./tx.zig");

// DefaultFilterPersent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
const DefaultFillPercent = 0.5;

// Represents a collection of key/value pairs inside the database.
pub const Bucket = struct {
    _b: ?*_Bucket,
    tx: ?*tx.TX,
};

// Represents the on-file represesntation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header, In the case of inline buckets, the "root" will be 0.
pub const _Bucket = packed struct {
    root: page.PgidType, // page id of the bucket's root-level page
    sequence: u64, // montotically incrementing. used by next_sequence().
};
