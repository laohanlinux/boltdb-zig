const page = @import("./page.zig");

// Represents the on-file represesntation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header, In the case of inline buckets, the "root" will be 0.
pub const _Bucket = packed struct {
    root: page.pgid_type, // page id of the bucket's root-level page
    sequence: u64, // montotically incrementing. used by next_sequence().
};
