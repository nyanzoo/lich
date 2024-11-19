use phylactery::store::PoolConfig;

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

    use human_size::{Byte, SpecificSize};
    use phylactery::store::{self, DataConfig, MetaConfig, PoolConfig};

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
                    meta_store: MetaConfig {
                        size: SpecificSize::new(1024.0, Byte).unwrap(),
                    },
                    data_store: DataConfig {
                        node_size: SpecificSize::new(1024.0, Byte).unwrap(),
                        max_disk_usage: SpecificSize::new(4096.0, Byte).unwrap(),
                    },
                    pool: PoolConfig {
                        block_size: SpecificSize::new(1024.0, Byte).unwrap(),
                        capacity: 100,
                    },
                },
                store::Config {
                    dir: "./lich2/".to_owned(),
                    shards: 100,
                    meta_store: MetaConfig {
                        size: SpecificSize::new(1024, Byte).unwrap(),
                    },
                    data_store: DataConfig {
                        node_size: SpecificSize::new(2048, Byte).unwrap(),
                        max_disk_usage: SpecificSize::new(8192, Byte).unwrap(),
                    },
                    pool: PoolConfig {
                        block_size: SpecificSize::new(4096.0, Byte).unwrap(),
                        capacity: 4,
                    },
                },
            ],
            incoming_pool: PoolConfig {
                block_size: SpecificSize::new(1024.0, Byte).unwrap(),
                capacity: 1024,
            },
            outgoing_pool: PoolConfig {
                block_size: SpecificSize::new(1024.0, Byte).unwrap(),
                capacity: 1024,
            },
        };

        let example = include_str!("../test/example.toml");
        let actual = toml::from_str::<BackendConfig>(example).expect("valid config");
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_operator_config() {
        let config = toml::from_str::<OperatorConfig>("port = 10000").expect("valid config");
        assert_eq!(config, OperatorConfig { port: 10000 });
    }
}
