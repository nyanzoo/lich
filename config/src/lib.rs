#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PoolConfig {
    pub block_size: usize,
    pub capacity: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EndpointConfig {
    pub port: u16,

    pub operator_addr: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct BackendConfig {
    pub endpoints: EndpointConfig,

    pub stores: Vec<phylactery::store::Config>,

    pub incoming_pool: PoolConfig,
    pub outgoing_pool: PoolConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct FrontendConfig {
    pub endpoints: EndpointConfig,

    pub incoming_pool: PoolConfig,
    pub outgoing_pool: PoolConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct OperatorConfig {
    pub port: u16,
}

#[cfg(test)]
pub mod test {

    use phylactery::store;

    use super::{BackendConfig, EndpointConfig, OperatorConfig};

    #[test]
    fn test_backend_config() {
        let expected = BackendConfig {
            endpoints: EndpointConfig {
                port: 10001,
                operator_addr: "localhost:10000".to_string(),
            },
            stores: vec![
                store::Config {
                    dir: "./lich/".to_owned(),
                    shards: 4,
                    data_len: 1024,
                    meta_len: 1024,
                    max_disk_usage: 4096,
                    block_size: 1024,
                    capacity: 100,
                },
                store::Config {
                    dir: "./lich2/".to_owned(),
                    shards: 100,
                    data_len: 2048,
                    meta_len: 1024,
                    max_disk_usage: 8096,
                    block_size: 2048,
                    capacity: 4,
                },
            ],
            incoming_pool: crate::PoolConfig {
                block_size: 1024,
                capacity: 1024,
            },
            outgoing_pool: crate::PoolConfig {
                block_size: 1024,
                capacity: 1024,
            },
        };

        let actual = toml::from_str::<BackendConfig>(
            r"
            [endpoints]
            port = 10001
            operator_addr = 'localhost:10000'

            [[stores]]
            dir = './lich/'
            shards = 4
            data_len = 1024
            meta_len = 1024
            max_disk_usage = 4096
            block_size = 1024
            capacity = 100

            [[stores]]
            dir = './lich2/'
            shards = 100
            data_len = 2048
            meta_len = 1024
            max_disk_usage = 8096
            block_size = 2048
            capacity = 4

            [incoming_pool]
            block_size = 1024
            capacity = 1024

            [outgoing_pool]
            block_size = 1024
            capacity = 1024
        ",
        )
        .expect("valid config");
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_operator_config() {
        let config = toml::from_str::<OperatorConfig>("port = 10000").expect("valid config");
        assert_eq!(config, OperatorConfig { port: 10000 });
    }
}
