use crate::cmd;
use crate::Address;
use redis::{
    aio::{Connection as RedisConnection, ConnectionLike},
    from_redis_value, Client, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, FromRedisValue,
    RedisError, ToRedisArgs,
};
use std::{collections::HashMap, io};

pub struct SingleConn {
    conn: Option<RedisConnection>,
    address: Address,
}

impl SingleConn {
    pub async fn req_packed_command<T>(&mut self, packed: &Cmd) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        let conn_opt = &mut self.conn;
        let mut conn = conn_opt.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Redis connection already closed from previous error",
            )
        })?;
        let result = conn.req_packed_command(packed).await.map_err(|e| {
            tracing::debug!(
                debug_error = &format!("{:?}", e),
                command = &format!("{}", String::from_utf8_lossy(&packed.get_packed_command())),
                "error running redis command: {e}"
            );
            e
        });

        // Check for recoverable errors
        let value = match result {
            Ok(value) => value,
            Err(e) => {
                if let ErrorKind::NoScriptError = e.kind() {
                    conn_opt.replace(conn);
                }
                return Err(e);
            }
        };
        conn_opt.replace(conn);

        from_redis_value(&value)
    }

    pub async fn execute_script<T>(
        &mut self,
        eval_command: &Cmd,
        load_command: &Cmd,
    ) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        match self.req_packed_command(eval_command).await {
            Ok(value) => Ok(value),
            Err(e) if e.kind() == ErrorKind::NoScriptError => {
                // Load script and retry
                self.req_packed_command(load_command).await?;
                self.req_packed_command(eval_command).await
            }
            e => e,
        }
    }

    pub async fn query<T>(&mut self, cmd: redis::Cmd) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        self.req_packed_command(&cmd).await
    }

    pub async fn try_connect(info: ConnectionInfo) -> Result<Self, RedisError> {
        let address = match &info.addr {
            ConnectionAddr::Tcp(host, port) => Address {
                host: host.clone(),
                port: *port,
            },
            ConnectionAddr::TcpTls { host, port, .. } => Address {
                host: host.clone(),
                port: *port,
            },
            ConnectionAddr::Unix(path) => Address {
                host: path.to_str().unwrap_or("").to_owned(),
                port: 0,
            },
        };
        let client = Client::open(info)?;
        let connection = client.get_tokio_connection().await?;
        Ok(SingleConn {
            conn: Some(connection),
            address,
        })
    }

    pub fn is_alive(&self) -> bool {
        self.conn.is_some()
    }

    pub async fn ping(&mut self) -> Result<(), RedisError> {
        self.query(cmd!["PING"]).await
    }

    pub fn partition_keys_by_node<'a, I, K>(
        &self,
        keys: I,
    ) -> Result<HashMap<Address, Vec<&'a K>>, RedisError>
    where
        &'a K: ToRedisArgs,
        I: Iterator<Item = &'a K>,
    {
        Ok(HashMap::from([(self.address.clone(), keys.collect())]))
    }
}
