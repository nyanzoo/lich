pub(super) fn bytes(bytes: u64) -> u64 {
    bytes
}

pub(super) fn kilobytes(kb: u64) -> u64 {
    bytes(kb) * 1024
}

pub(super) fn megabytes(mb: u64) -> u64 {
    kilobytes(mb) * 1024
}

pub(super) fn gigabytes(gb: u64) -> u64 {
    megabytes(gb) * 1024
}
