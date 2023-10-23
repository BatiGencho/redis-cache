use derive_more::{AsRef, Deref, DerefMut, From};
use serde::{
    de,
    ser::{self, SerializeStruct},
    Deserialize, Serialize,
};
use std::{convert::TryFrom, fmt};

use crate::Error;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    #[serde(default, flatten)]
    pub hosts: Hosts,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub pool_max_size: Option<usize>,
    #[serde(default)]
    pub pool_min_size: Option<usize>,
    #[serde(default)]
    pub mode: ConnectionMode,
    #[serde(default)]
    pub is_tls: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum ConnectionMode {
    Detect,
    Single,
    Cluster,
}

impl Default for ConnectionMode {
    fn default() -> Self {
        Self::Detect
    }
}

#[derive(AsRef, Clone, Debug, Default, Eq, From, PartialEq)]
pub struct Host(pub String, pub u16);

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

impl TryFrom<&str> for Host {
    type Error = &'static str;

    fn try_from(host: &str) -> Result<Self, Self::Error> {
        let (host, port) = host.rsplit_once(':').ok_or("host missing port")?;
        Ok(Self(
            host.to_owned(),
            port.parse().map_err(|_err| "invalid port")?,
        ))
    }
}

impl Serialize for Host {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(AsRef, Clone, Debug, Default, Deref, DerefMut, Eq, From, PartialEq)]
pub struct Hosts(Vec<Host>);

impl TryFrom<&str> for Hosts {
    type Error = &'static str;

    fn try_from(hosts: &str) -> Result<Self, Self::Error> {
        hosts
            .split(',')
            .map(Host::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum HostConfig {
    Host { host: String, port: u16 },
    Hosts { hosts: Vec<String> },
    // This is only needed for EnvOverride, e.g.
    // KSERVICE_TEST_REDIS_HOSTS=redis01:1234,redis02:1234
    EnvValue(String),
}

/// Always serialize using the format of the `Hosts` variant of `HostConfig`.
impl Serialize for Hosts {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut state = serializer.serialize_struct("Hosts", 1)?;
        state.serialize_field("hosts", &self.0)?;
        state.end()
    }
}

#[allow(single_use_lifetimes)]
impl<'de> Deserialize<'de> for Hosts {
    fn deserialize<D>(deserializer: D) -> Result<Hosts, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        match HostConfig::deserialize(deserializer)? {
            HostConfig::Host { host, port } => Ok(vec![(host, port).into()].into()),
            HostConfig::Hosts { hosts } => hosts
                .into_iter()
                .map(|host| Host::try_from(host.as_str()).map_err(de::Error::custom))
                .collect::<Result<Vec<_>, _>>()
                .map(Into::into),
            HostConfig::EnvValue(host) => Hosts::try_from(host.as_str()).map_err(de::Error::custom),
        }
    }
}

impl Config {
    pub fn new(hosts: &[&str]) -> Result<Self, Error> {
        Ok(Self {
            hosts: hosts
                .iter()
                .map(|host| TryFrom::try_from(*host))
                .collect::<Result<Vec<_>, _>>()
                .map(Into::into)
                .map_err(Error::HostParseError)?,
            password: None,
            pool_max_size: None,
            pool_min_size: None,
            mode: ConnectionMode::default(),
            is_tls: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_config_host_from_toml() {
        let config = r#"
host = "host01"
port = 1234
"#;
        let config: Config = toml::from_str(config).unwrap();
        assert_eq!(config.hosts, Hosts::try_from("host01:1234").unwrap());
    }

    #[test]
    pub fn test_config_hosts_from_toml() {
        let config = r#"
hosts = [ "host01:1234", "host02:2345" ]
"#;
        let config: Config = toml::from_str(config).unwrap();
        assert_eq!(
            config.hosts,
            Hosts::try_from("host01:1234,host02:2345").unwrap()
        );
    }

    #[test]
    pub fn test_config_env_overrides() {
        let config = Config::new(&["localhost:6379"]).unwrap();
        assert_eq!(config.hosts.len(), 1);
    }

    #[test]
    fn serialize_redis_hosts() {
        let host_json = r#"{"host":"1.2.3.4","port":5678}"#;
        let hosts_json = r#"{"hosts":["1.2.3.4:5678"]}"#;

        let host: Hosts = serde_json::from_str(host_json).unwrap();
        let hosts: Hosts = serde_json::from_str(hosts_json).unwrap();

        assert_eq!(host, hosts);

        let ser_host_json = serde_json::to_string(&host).unwrap();
        let ser_hosts_json = serde_json::to_string(&hosts).unwrap();

        // Both should serialize to the same `Hosts` format.
        assert_eq!(ser_host_json, hosts_json);
        assert_eq!(ser_hosts_json, hosts_json);
    }
}
