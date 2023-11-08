#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EndpointConfig {
    pub port: u16,

    pub operator_addr: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct BackendConfig {
    pub endpoints: EndpointConfig,

    pub store: StoreConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct FrontendConfig {
    pub endpoints: EndpointConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StoreConfig {
    pub path: String,

    pub meta_size: u64,

    pub node_size: u64,

    pub version: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct OperatorConfig {
    pub port: u16,
}

#[cfg(test)]
pub mod test {

    use super::{BackendConfig, EndpointConfig, OperatorConfig, StoreConfig};

    #[test]
    fn test_backend_config() {
        let config = toml::from_str::<BackendConfig>(
            r"
            [endpoints]
            port = 10001
            operator_addr = 'localhost:10000'

            [store]
            path = './lich/'
            meta_size = 1024
            node_size = 1024
            version = 1
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
                store: StoreConfig {
                    path: "./lich/".to_string(),
                    meta_size: 1024,
                    node_size: 1024,
                    version: 1,
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
