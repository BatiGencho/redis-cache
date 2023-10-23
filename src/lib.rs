// <https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html>
#![warn(
    anonymous_parameters,
    bare_trait_objects,
    elided_lifetimes_in_paths,
    rust_2018_idioms,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_extern_crates,
    unused_import_braces
)]
// <https://rust-lang.github.io/rust-clippy/stable/>
#![warn(
    clippy::all,
    clippy::cargo,
    clippy::dbg_macro,
    clippy::float_cmp_const,
    clippy::get_unwrap,
    clippy::mem_forget,
    clippy::nursery,
    clippy::pedantic,
    clippy::todo,
    clippy::unwrap_used
)]
// Allow some clippy lints
#![allow(
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::if_not_else,
    clippy::module_name_repetitions,
    clippy::multiple_crate_versions,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::use_self,
    clippy::cargo_common_metadata,
    clippy::missing_errors_doc,
    clippy::enum_glob_use,
    clippy::struct_excessive_bools
)]
// Allow some lints while testing
#![cfg_attr(test, allow(clippy::non_ascii_literal, clippy::unwrap_used))]

mod cluster;
pub mod config;
pub mod error;
mod metrics;
pub mod mock;
pub mod pool;
mod single;
pub mod stream;

use self::{
    cluster::ClusterConn, metrics::MeteredConn, single::SingleConn,
};
use crate::pool::{ConnectionManager, PoolConnection};
pub use crate::{
    config::{Config, ConnectionMode},
    metrics::RedisMetrics,
};
use async_trait::async_trait;
use displaydoc::Display;
use redis::{self, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
pub use redis::{
    ErrorKind, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value,
};
use std::{collections::HashMap, hash::Hash, sync::Arc, time::Duration};
use stream::{
    MessageId, ReadStream, ReadStreamOptions, StreamItem, StreamReadReply, WriteStreamOptions,
};

struct VecWrapper<'a>(&'a Vec<u8>);
impl<'a> ToRedisArgs for VecWrapper<'a> {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg(self.0);
    }
}

#[derive(Debug, Display, thiserror::Error)]
pub enum Error {
    /// KeyDoesNotExist
    KeyDoesNotExist,
    /// RedisError: `{0}`
    RedisError(#[from] RedisError),
    /// RedisInsertionError: `{0}`
    RedisInsertionError(String),
    /// Failed to parse host: `{0}`
    HostParseError(&'static str),
    /// IoError: `{0}`
    IoError(#[from] std::io::Error),
    /// No connection info was specified
    MissingConnectionInfo,
}

#[async_trait]
pub trait Store {
    async fn get_key<K, T>(&self, key: K) -> Result<T, Error>
    where
        K: ToRedisArgs + Send,
        T: FromRedisValue + Send + 'static;

    async fn set_key<K, D>(&self, key: K, ttl: Duration, data: D) -> Result<(), Error>
    where
        K: ToRedisArgs + Send,
        D: ToRedisArgs + Send;

    async fn del_key<T: ToRedisArgs + Send>(&self, key: T) -> Result<Option<u64>, Error>;

    async fn del_keys<'a, T>(&self, keys: &'a [T]) -> Result<Vec<Option<u64>>, Error>
    where
        T: Sync,
        &'a T: ToRedisArgs;

    /// Sets a key with TTL if not already present.
    /// NOTE: TTL used here is in seconds for the NX command as `redis::ToRedisArgs`
    /// is not implemented for `u128`.
    async fn set_if_not_exists<K, D>(&self, key: K, ttl: Duration, data: D) -> Result<bool, Error>
    where
        K: ToRedisArgs + Send,
        D: ToRedisArgs + Send;

    /// Add exactly one element to a sorted set ("zadd" query).
    async fn sorted_set_add_one<D>(&self, key: &str, score: i64, data: D) -> Result<(), Error>
    where
        D: ToRedisArgs + Send;
}

#[derive(Clone)]
pub struct Redis {
    pool: pool::Pool<Connection>,
    pub metrics: Arc<RedisMetrics>,
}

impl Redis {
    pub async fn new(config: &Config) -> Self {
        let metrics = Arc::new(RedisMetrics::default());
        let hosts = config
            .hosts
            .iter()
            .map(|host| (host.0.as_str(), host.1))
            .collect::<Vec<_>>();
        let config = pool::ConfigBuilder::new()
            .address(Client::with_mode(
                &hosts,
                config.password.as_deref(),
                metrics.clone(),
                config.mode,
                config.is_tls,
            ))
            .max_size(config.pool_max_size)
            .min_size(config.pool_min_size)
            .build();

        Self {
            pool: pool::Pool::new(config).await,
            metrics,
        }
    }

    pub async fn get_conn(&self) -> Result<PoolConnection<Connection>, Error> {
        self.pool.get_connection().await
    }

    pub async fn try_get_conn(&self) -> Result<PoolConnection<Connection>, Error> {
        self.pool.try_get_connection().await
    }

    pub async fn get_conn_timeout(
        &self,
        timeout: Duration,
    ) -> Result<PoolConnection<Connection>, Error> {
        self.pool.get_connection_timeout(timeout).await
    }

    /// Write single entry to a Redis Stream
    /// NOTE: The default message id provided by Redis is used
    /// (see: https://redis.io/commands/XADD#specifying-a-stream-id-as-an-argument)
    pub async fn stream_write_one<'s, D, S>(
        &self,
        stream_id: &'s S,
        data: D,
        options: &WriteStreamOptions,
    ) -> Result<MessageId, Error>
    where
        D: ToRedisArgs,
        &'s S: ToRedisArgs,
    {
        self.get_conn().await?.xadd(stream_id, data, options).await
    }

    /// Query if a message exists in a stream
    pub async fn stream_message_exists<'s, S>(
        &self,
        stream_id: &'s S,
        id: MessageId,
    ) -> Result<bool, Error>
    where
        &'s S: ToRedisArgs,
        S: FromRedisValue + Eq + Hash + Send + 'static,
    {
        let messages = self
            .get_conn()
            .await?
            .xrange::<S, Value>(stream_id, id, id, Some(1))
            .await?;

        Ok(!messages.is_empty())
    }
}

#[async_trait]
impl Store for Redis {
    async fn get_key<K, T>(&self, key: K) -> Result<T, Error>
    where
        K: ToRedisArgs + Send,
        T: FromRedisValue + Send + 'static,
    {
        self.get_conn().await?.get(key).await
    }

    async fn set_key<K, D>(&self, key: K, ttl: Duration, data: D) -> Result<(), Error>
    where
        K: ToRedisArgs + Send,
        D: ToRedisArgs + Send,
    {
        self.get_conn().await?.set_expiry(key, ttl, data).await
    }

    async fn del_key<T: ToRedisArgs + Send>(&self, key: T) -> Result<Option<u64>, Error> {
        self.get_conn().await?.del(key).await
    }

    async fn del_keys<'a, T>(&self, keys: &'a [T]) -> Result<Vec<Option<u64>>, Error>
    where
        T: Sync,
        &'a T: ToRedisArgs,
    {
        let mut pool = self.get_conn().await?;
        let mut conn = pool.multi().await?;

        for key in keys {
            conn.del(key).await?;
        }

        conn.exec().await
    }

    /// Sets a key with TTL if not already present.
    /// NOTE: TTL used here is in seconds for the NX command as `redis::ToRedisArgs`
    /// is not implemented for `u128`.
    async fn set_if_not_exists<K, D>(&self, key: K, ttl: Duration, data: D) -> Result<bool, Error>
    where
        K: ToRedisArgs + Send,
        D: ToRedisArgs + Send,
    {
        self.get_conn().await?.set_nx_ex(key, ttl, data).await
    }

    /// Add exactly one element to a sorted set ("zadd" query).
    async fn sorted_set_add_one<D>(&self, key: &str, score: i64, data: D) -> Result<(), Error>
    where
        D: ToRedisArgs + Send,
    {
        let mut conn = self.get_conn().await?;

        let inserted = conn.z_add(key, score, data).await?;

        if inserted == 1 {
            Ok(())
        } else {
            Err(Error::RedisInsertionError(key.to_string()))
        }
    }
}

pub enum Connection {
    Single(SingleConn),
    Clustered(ClusterConn),
    Metered(MeteredConn),
}

#[async_trait]
impl ConnectionManager for Connection {
    type Address = Client;
    type Connection = Self;
    type Error = Error;

    async fn connect(address: &Client) -> Result<Connection, Error> {
        Ok(address.get_connection().await?)
    }

    fn check_alive(connection: &Self::Connection) -> Option<bool> {
        match connection {
            Connection::Single(ref conn) => Some(conn.is_alive()),
            Connection::Clustered(ref conn) => Some(conn.is_alive()),
            Connection::Metered(ref conn) => Some(conn.is_alive()),
        }
    }

    async fn ping(connection: &mut Self::Connection) -> Result<(), Self::Error> {
        connection.ping().await
    }
}

impl Connection {
    pub async fn query<T>(&mut self, cmd: redis::Cmd) -> Result<T, Error>
    where
        T: FromRedisValue + Send + 'static,
    {
        match self {
            Self::Single(ref mut single) => Ok(single.query(cmd).await?),
            Self::Clustered(ref mut cluster) => Ok(cluster.query(cmd).await?),
            Self::Metered(ref mut metered) => Ok(metered.metered_query(cmd).await?),
        }
    }

    pub fn partition_keys_by_node<'a, I, K>(
        &self,
        keys: I,
    ) -> Result<HashMap<Address, Vec<&'a K>>, Error>
    where
        &'a K: ToRedisArgs,
        I: Iterator<Item = &'a K>,
    {
        match self {
            Self::Single(ref single) => Ok(single.partition_keys_by_node(keys)?),
            Self::Clustered(ref cluster) => Ok(cluster.partition_keys_by_node(keys)?),
            Self::Metered(ref metered) => Ok(metered.partition_keys_by_node(keys)?),
        }
    }

    /// Tells if the given key(s) exist.
    ///
    /// Returns the number of matches for the given key(s).
    pub async fn exists<K>(&mut self, key: K) -> Result<u64, Error>
    where
        K: ToRedisArgs,
    {
        Ok(self.query(cmd!["EXISTS", key]).await?)
    }

    pub async fn get<K, T>(&mut self, key: K) -> Result<T, Error>
    where
        K: ToRedisArgs,
        T: FromRedisValue + Send + 'static,
    {
        self.query(cmd!["GET", key]).await
    }

    pub async fn hget<H, K, T>(&mut self, hash: H, key: K) -> Result<T, Error>
    where
        H: ToRedisArgs,
        K: ToRedisArgs,
        T: FromRedisValue + Send + 'static,
    {
        self.query(cmd!["HGET", hash, key]).await
    }

    pub async fn hget_all<H, T>(&mut self, hash: H) -> Result<T, Error>
    where
        H: ToRedisArgs,
        T: FromRedisValue + Send + 'static,
    {
        self.query(cmd!["HGETALL", hash]).await
    }

    pub async fn ttl<T: ToRedisArgs>(&mut self, key: T) -> Result<Option<i64>, Error> {
        self.query(cmd!["TTL", key]).await
    }

    pub async fn pttl<T: ToRedisArgs>(&mut self, key: T) -> Result<Option<i64>, Error> {
        self.query(cmd!["PTTL", key]).await
    }

    pub async fn del<T: ToRedisArgs>(&mut self, key: T) -> Result<Option<u64>, Error> {
        self.query(cmd!["DEL", key]).await
    }

    pub async fn ping(&mut self) -> Result<(), Error> {
        match self {
            Self::Single(ref mut single) => Ok(single.ping().await?),
            Self::Clustered(ref mut cluster) => Ok(cluster.ping().await?),
            Self::Metered(ref mut metered) => Ok(metered.metered_ping().await?),
        }
    }

    pub async fn multi(&mut self) -> Result<ConnectionMulti<'_>, Error> {
        self.query(cmd!["MULTI"]).await?;

        Ok(ConnectionMulti(self))
    }

    pub async fn set<K, D>(&mut self, key: K, data: D) -> Result<(), Error>
    where
        K: ToRedisArgs,
        D: ToRedisArgs,
    {
        self.query(cmd!["SET", key, data]).await
    }

    pub async fn set_expiry<K, D>(&mut self, key: K, ttl: Duration, data: D) -> Result<(), Error>
    where
        K: ToRedisArgs,
        D: ToRedisArgs,
    {
        self.query(cmd!["SETEX", key, ttl.as_secs(), data]).await
    }

    pub async fn z_add<K, D>(&mut self, key: K, score: i64, data: D) -> Result<u64, Error>
    where
        K: ToRedisArgs,
        D: ToRedisArgs,
    {
        self.query(cmd!["ZADD", key, score, data]).await
    }

    pub async fn zadd_binary<K, T>(&mut self, key: K, score: i64, member: T) -> Result<i64, Error>
    where
        K: ToRedisArgs,
        T: AsRef<[u8]>,
    {
        self.query(cmd![
            "ZADD",
            key,
            score,
            VecWrapper(&member.as_ref().to_vec())
        ])
        .await
    }

    pub async fn z_range<K, T>(
        &mut self,
        key: K,
        min: i64,
        max: i64,
        is_reversed: bool,
    ) -> Result<T, Error>
    where
        K: ToRedisArgs,
        T: FromRedisValue + Send + 'static,
    {
        if is_reversed {
            return self.query(cmd!["ZREVRANGE", key, min, max]).await;
        }
        self.query(cmd!["ZRANGE", key, min, max]).await
    }

    pub async fn z_rem<K, T>(&mut self, key: K, members: T) -> Result<u64, Error>
    where
        K: ToRedisArgs,
        T: ToRedisArgs,
    {
        self.query(cmd!["ZREM", key, members]).await
    }

    pub async fn zrem_all<K>(&mut self, key: K) -> Result<i64, Error>
    where
        K: ToRedisArgs,
    {
        self.query(cmd!["ZREMRANGEBYRANK", key, 0, -1]).await
    }

    pub async fn z_card<K>(&mut self, key: K) -> Result<i64, Error>
    where
        K: ToRedisArgs,
    {
        self.query(cmd!["ZCARD", key]).await
    }

    pub async fn incr_by<T: ToRedisArgs>(&mut self, key: T, increment: i64) -> Result<i64, Error> {
        self.query(cmd!["INCRBY", key, increment]).await
    }

    pub async fn hset<H, D>(&mut self, hash: H, data: D) -> Result<i64, Error>
    where
        H: ToRedisArgs,
        D: ToRedisArgs,
    {
        self.query(cmd!["HSET", hash, data]).await
    }

    pub async fn hdel<H, D>(&mut self, hash: H, data: D) -> Result<i64, Error>
    where
        H: ToRedisArgs,
        D: ToRedisArgs,
    {
        self.query(cmd!["HDEL", hash, data]).await
    }

    pub async fn expire<T: ToRedisArgs>(&mut self, key: T, ttl: Duration) -> Result<(), Error> {
        match self.query(cmd!["EXPIRE", key, ttl.as_secs()]).await? {
            1 => Ok(()),
            _ => Err(Error::KeyDoesNotExist),
        }
    }

    pub async fn set_nx_ex<K, D>(&mut self, key: K, ttl: Duration, data: D) -> Result<bool, Error>
    where
        K: ToRedisArgs,
        D: ToRedisArgs,
    {
        self.query(cmd!["SET", key, data, "NX", "EX", ttl.as_secs()])
            .await
    }

    async fn xadd<'s, D, S>(
        &mut self,
        stream_id: &'s S,
        data: D,
        options: &WriteStreamOptions,
    ) -> Result<MessageId, Error>
    where
        D: ToRedisArgs,
        &'s S: ToRedisArgs,
    {
        let cmd = cmd!["XADD", stream_id, options, "*", data];
        self.query(cmd).await
    }

    async fn xrange<'s, S, T>(
        &mut self,
        stream_id: &'s S,
        start: MessageId,
        end: MessageId,
        limit: Option<u64>,
    ) -> Result<Vec<StreamItem<T>>, Error>
    where
        &'s S: ToRedisArgs,
        S: FromRedisValue + Eq + Hash + Send + 'static,
        T: FromRedisValue + Send + 'static,
    {
        let mut cmd = cmd!["XRANGE", stream_id, start, end];
        if let Some(limit) = limit {
            cmd.arg("COUNT");
            cmd.arg(limit);
        }
        self.query(cmd).await
    }

    pub async fn xread<'s, T, S>(
        &mut self,
        streams: &[ReadStream<'s, S>],
        options: &ReadStreamOptions,
    ) -> Result<StreamReadReply<S, T>, Error>
    where
        T: FromRedisValue + Send + 'static,
        S: FromRedisValue + Eq + Hash + Send + 'static,
        &'s S: ToRedisArgs,
    {
        let mut cmd = cmd!["XREAD", options, "STREAMS"];
        for stream in streams {
            cmd.arg(stream.id);
        }
        for stream in streams {
            cmd.arg(stream.offset);
        }
        self.query(cmd).await
    }

    pub async fn xlen<'s, S>(&mut self, stream_id: &'s S) -> Result<u64, Error>
    where
        &'s S: ToRedisArgs,
    {
        let cmd = cmd!["XLEN", stream_id];
        self.query(cmd).await
    }
}

pub struct ConnectionMulti<'a>(&'a mut Connection);

impl<'a> ConnectionMulti<'a> {
    pub async fn del<T: ToRedisArgs>(&mut self, key: T) -> Result<(), Error> {
        self.0.query(cmd!["DEL", key]).await?;
        Ok(())
    }

    pub async fn exec<T>(&mut self) -> Result<Vec<T>, Error>
    where
        T: FromRedisValue + Send + 'static,
    {
        self.0.query(cmd!["EXEC"]).await
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    connections: Vec<ConnectionInfo>,
    metrics: Arc<RedisMetrics>,
    mode: ConnectionMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Address {
    pub host: String,
    pub port: u16,
}

impl Client {
    pub fn new(
        hosts: &[(&str, u16)],
        password: Option<&str>,
        metrics: Arc<RedisMetrics>,
        is_tls: bool,
    ) -> Self {
        Self::with_mode(hosts, password, metrics, ConnectionMode::default(), is_tls)
    }

    pub fn with_mode(
        hosts: &[(&str, u16)],
        password: Option<&str>,
        metrics: Arc<RedisMetrics>,
        mode: ConnectionMode,
        is_tls: bool,
    ) -> Self {
        let connections = hosts
            .iter()
            .map(|(host, port)| build_info(host, *port, password, is_tls))
            .collect();
        Self {
            connections,
            metrics,
            mode,
        }
    }

    async fn connect_cluster(&self) -> Result<Connection, Error> {
        let conn = ClusterConn::try_connect(self.connections.clone()).await?;
        tracing::info!("initiated clustered redis connection");
        Ok(Connection::Metered(MeteredConn::new(
            Connection::Clustered(conn),
            self.metrics.clone(),
        )))
    }

    async fn connect_single(&self) -> Result<Connection, Error> {
        if self.connections.is_empty() {
            return Err(Error::MissingConnectionInfo);
        }

        let conn = SingleConn::try_connect(self.connections[0].clone()).await?;
        tracing::info!("initiated single redis connection");
        Ok(Connection::Metered(MeteredConn::new(
            Connection::Single(conn),
            self.metrics.clone(),
        )))
    }

    pub async fn get_connection(&self) -> Result<Connection, Error> {
        let addresses = self
            .connections
            .iter()
            .map(|info| format!("{:?}", info.addr))
            .collect::<Vec<_>>()
            .join(", ");
        tracing::info!("initiating redis connection with addresses {:?}", addresses);

        match self.mode {
            ConnectionMode::Detect => match self.connect_cluster().await {
                Ok(conn) => Ok(conn),
                // If cluster connection fails, it could've been because it's not a cluster,
                // so try to connect to just one if only one was provided.
                Err(_) if self.connections.len() == 1 => self.connect_single().await,
                Err(e) => Err(e),
            },
            ConnectionMode::Single => self.connect_single().await,
            ConnectionMode::Cluster => self.connect_cluster().await,
        }
    }
}

fn build_info(host: &str, port: u16, password: Option<&str>, is_tls: bool) -> ConnectionInfo {
    let addr = if is_tls {
        ConnectionAddr::TcpTls {
            host: host.to_owned(),
            port,
            insecure: true,
        }
    } else {
        ConnectionAddr::Tcp(host.to_owned(), port)
    };
    ConnectionInfo {
        addr,
        redis: RedisConnectionInfo {
            db: 0,
            username: None,
            password: password.filter(|p| !p.is_empty()).map(String::from),
        },
    }
}

#[macro_export]
macro_rules! cmd {
    [$($arg:expr $(,)*)*] => {{
        let mut cmd = redis::Cmd::new();
        $(cmd.arg($arg);)*
        cmd
    }}
}

pub mod integration_test {

    use super::*;
    use std::sync::Arc;

    pub struct TestRedis {
        pub single: Arc<Redis>,
        pub cluster: Arc<Redis>,
    }

    /// Utility for running tests that depend on redis
    /// Defaults to connecting to a single instance redis on `localhost:6379` and to a redis
    /// cluster on `localhost:7000`. These values can be overriden through environment variables,
    /// using our default env override method.
    /// Prefixes: KSERVICE_TEST_REDIS_SINGLE_, KSERVICE_TEST_REDIS_CLUSTER_
    impl TestRedis {
        pub async fn new() -> Self {
            let single_config = Config::new(&["localhost:6379"]).unwrap();
            let cluster_config = Config::new(&["localhost:7000"]).unwrap();
            let single_redis = Redis::new(&single_config).await;
            let cluster_redis = Redis::new(&cluster_config).await;

            Self {
                single: Arc::new(single_redis),
                cluster: Arc::new(cluster_redis),
            }
        }
    }

    /// Runs a test with both a single instance redis and a redis cluster
    #[macro_export]
    macro_rules! test_using_redis {
        (async fn $func:ident ($param:ident: Arc<Redis>) $code:block) => {
            paste::paste! {
                #[tokio::test]
                async fn [<$func _single>]() {
                    let redis = crate::integration_test::TestRedis::new().await;
                    let $param = redis.single;
                    $code
                }

                #[tokio::test]
                async fn [<$func _cluster>]() {
                    let redis = crate::integration_test::TestRedis::new().await;
                    let $param = redis.cluster;
                    $code
                }
            }
        };
    }

    test_using_redis! {
        async fn test_redis(redis: Arc<Redis>) {
            let mut connection = redis.get_conn().await.unwrap();
            println!("connected to redis");

            connection.set("foo", "bar").await.expect("set foo");
            let data: Option<String> = connection.get("foo").await.expect("data");
            assert_eq!(data.expect("foo"), "bar");
        }
    }
}
