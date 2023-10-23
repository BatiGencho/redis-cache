use crate::{cluster::ClusterConn, single::SingleConn, Address, Connection};
use metered::{metered, ErrorCount, HitCount, InFlight, ResponseTime, Throughput};
use redis::{Cmd, FromRedisValue, RedisError, ToRedisArgs};
use std::{collections::HashMap, sync::Arc};

/// Wraps a Redis connection to provide metrics.
pub struct MeteredConn {
    inner: InnerConn,
    metrics: Arc<RedisMetrics>,
}

enum InnerConn {
    Single(SingleConn),
    Clustered(ClusterConn),
}

#[metered(registry = RedisMetrics, visibility = pub, registry_expr = metrics)]
impl MeteredConn {
    pub fn new(conn: Connection, metrics: Arc<RedisMetrics>) -> MeteredConn {
        let inner = match conn {
            Connection::Metered(metered) => return metered,
            Connection::Clustered(clustered) => InnerConn::Clustered(clustered),
            Connection::Single(single) => InnerConn::Single(single),
        };

        MeteredConn { inner, metrics }
    }

    pub async fn metered_ping(&mut self) -> Result<(), RedisError> {
        Self::ping(&mut self.inner, &self.metrics).await
    }

    #[measure([HitCount, ErrorCount, InFlight, Throughput, ResponseTime])]
    async fn ping(inner: &mut InnerConn, metrics: &RedisMetrics) -> Result<(), RedisError> {
        match inner {
            InnerConn::Single(conn) => conn.ping().await,
            InnerConn::Clustered(conn) => conn.ping().await,
        }
    }

    pub fn is_alive(&self) -> bool {
        match &self.inner {
            InnerConn::Single(conn) => conn.is_alive(),
            InnerConn::Clustered(conn) => conn.is_alive(),
        }
    }

    pub async fn metered_query<T>(&mut self, cmd: Cmd) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        Self::query(&mut self.inner, &self.metrics, cmd).await
    }

    #[measure([HitCount, ErrorCount, InFlight, Throughput, ResponseTime])]
    async fn query<T>(
        inner: &mut InnerConn,
        metrics: &RedisMetrics,
        cmd: Cmd,
    ) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        match inner {
            InnerConn::Single(conn) => conn.query(cmd).await,
            InnerConn::Clustered(conn) => conn.query(cmd).await,
        }
    }

    pub fn partition_keys_by_node<'a, I, K>(
        &self,
        keys: I,
    ) -> Result<HashMap<Address, Vec<&'a K>>, RedisError>
    where
        &'a K: ToRedisArgs,
        I: Iterator<Item = &'a K>,
    {
        match &self.inner {
            InnerConn::Single(conn) => conn.partition_keys_by_node(keys),
            InnerConn::Clustered(conn) => conn.partition_keys_by_node(keys),
        }
    }

    pub async fn metered_execute_script<T>(
        &mut self,
        eval_command: &Cmd,
        load_command: &Cmd,
    ) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        Self::execute_script(&mut self.inner, &self.metrics, eval_command, load_command).await
    }

    #[measure([HitCount, ErrorCount, InFlight, Throughput, ResponseTime])]
    async fn execute_script<T>(
        inner: &mut InnerConn,
        metrics: &RedisMetrics,
        eval_command: &Cmd,
        load_command: &Cmd,
    ) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        match inner {
            InnerConn::Single(conn) => conn.execute_script(eval_command, load_command).await,
            InnerConn::Clustered(conn) => conn.execute_script(eval_command, load_command).await,
        }
    }
}
