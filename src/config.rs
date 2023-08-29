#[derive(serde::Deserialize, serde::Serialize)]
pub struct Config {
    pub path: String,

    pub meta_size: u64,

    pub node_size: u64,

    pub version: u8,
}

#[cfg(test)]
pub mod test {

    use super::Config;

    impl Config {
        pub fn test_new(path: String) -> Self {
            Self {
                path,
                meta_size: 1024,
                node_size: 1024,
                version: 1,
            }
        }
    }
}
