use phylactery::kv_store;

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

    pub store: kv_store::config::Config,

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

    use phylactery::kv_store;

    use super::{BackendConfig, EndpointConfig, OperatorConfig};

    #[test]
    fn test_backend_config() {
        let config = toml::from_str::<BackendConfig>(
            r"
            [endpoints]
            port = 10001
            operator_addr = 'localhost:10000'

            [store]
            path = './lich/'
            version = 'V1'

            [store.meta]
            max_disk_usage = 1024
            max_key_size = 128

            [store.data]
            node_size = 1024

            [incoming_pool]
            block_size = 1024
            capacity = 1024

            [outgoing_pool]
            block_size = 1024
            capacity = 1024
        ",
        )
        .expect("valid config");
        assert_eq!(
            config,
            BackendConfig {
                endpoints: EndpointConfig {
                    port: 10001,
                    operator_addr: "localhost:10000".to_string()
                },
                store: kv_store::config::Config {
                    path: "./lich/".to_owned(),
                    meta: kv_store::config::Metadata {
                        max_key_size: 128,
                        max_disk_usage: 1024,
                    },
                    data: kv_store::config::Data { node_size: 1024 },
                    version: phylactery::entry::Version::V1,
                },
                incoming_pool: crate::PoolConfig {
                    block_size: 1024,
                    capacity: 1024
                },
                outgoing_pool: crate::PoolConfig {
                    block_size: 1024,
                    capacity: 1024
                }
            }
        );
    }

    #[test]
    fn test_operator_config() {
        let config = toml::from_str::<OperatorConfig>("port = 10000").expect("valid config");
        assert_eq!(config, OperatorConfig { port: 10000 });
    }
}
