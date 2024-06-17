pub const Error = error{
    DatabaseNotOpen,
    DatabaseOpen,
    Invalid,
    VersionMismatch,
    CheckSum,
    Timeout,

    TxNotWriteable,
    TxClosed,
    DatabaseReadOnly,

    BucketNotFound,
    BucketExists,
    BucketNameRequired,
    KeyRequired,
    KeyTooLarge,
    ValueTooLarge,
    IncompactibleValue,

    MMapTooLarge,
};
