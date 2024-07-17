/// Error type
pub const Error = error{
    // Below are the database errors
    DatabaseNotOpen,
    DatabaseOpen,

    Invalid,
    VersionMismatch,
    CheckSum,
    Timeout,
    // Below are the transaction errors
    TxNotWriteable,
    TxClosed,
    DatabaseReadOnly,

    // Below are the bucket errors
    BucketNotFound,
    BucketExists,
    BucketNameRequired,
    KeyRequired,
    KeyTooLarge,
    ValueTooLarge,
    IncompactibleValue,

    // Below are the mmap errors
    MMapTooLarge,
};
