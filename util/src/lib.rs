
pub const fn bytes(bytes: u64) -> u64 {
    bytes
}

pub const fn kilobytes(kb: u64) -> u64 {
    bytes(kb) * 1024
}

pub const fn megabytes(mb: u64) -> u64 {
    kilobytes(mb) * 1024
}

pub const fn gigabytes(gb: u64) -> u64 {
    megabytes(gb) * 1024
}
