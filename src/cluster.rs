use super::{Address, SingleConn};
use crate::cmd;
use futures::future::{join_all, TryFutureExt};
use rand::{distributions::Uniform, prelude::*};
use redis::{
    from_redis_value, parse_redis_value, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind,
    FromRedisValue, RedisError, RedisResult, ToRedisArgs, Value,
};
use slotmap::SlotMap;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
};

const RETRIES: usize = 3;
const SLOT_SIZE: u16 = 16384;

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

type Key = slotmap::DefaultKey;

pub struct ClusterConn {
    connections: SlotMap<Key, SingleConn>,
    password: Option<String>,
    node_slots: NodeSlots,
    distribution: Option<Uniform<usize>>,
    is_tls: bool,
}

#[derive(Debug, Clone)]
struct NodeSlots {
    pub slots: BTreeMap<u16, Key>,
    pub addresses: HashMap<Address, Key>,
}

fn cluster_error(msg: impl Into<String>) -> RedisError {
    RedisError::from((ErrorKind::ExtensionError, "cluster error", msg.into()))
}

fn slot_resp_error(msg: impl Into<String>) -> RedisError {
    RedisError::from((ErrorKind::TypeError, "error parsing slots", msg.into()))
}

fn partition_error(msg: impl Into<String>) -> RedisError {
    RedisError::from((
        ErrorKind::ExtensionError,
        "error partitioning keys by cluster node",
        msg.into(),
    ))
}

#[derive(Debug)]
struct SlotResp {
    start: u16,
    end: u16,
    address: Address,
}

impl FromRedisValue for SlotResp {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Bulk(arr) => {
                if arr.len() < 3 {
                    return Err(slot_resp_error("not enough elements for slot record"));
                }
                let start: u16 = from_redis_value(&arr[0])?;
                let end: u16 = from_redis_value(&arr[1])?;
                // We only connect to the slot master
                let address: Address = from_redis_value(&arr[2])?;
                Ok(SlotResp {
                    start,
                    end,
                    address,
                })
            }
            _ => Err(slot_resp_error("expecting bulk for slot resp")),
        }
    }
}

impl FromRedisValue for Address {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Bulk(arr) => {
                if arr.len() < 2 {
                    return Err(slot_resp_error("not enough elements for host record"));
                }

                let host: String = from_redis_value(&arr[0])?;
                let port: u16 = from_redis_value(&arr[1])?;

                Ok(Address { host, port })
            }
            _ => Err(slot_resp_error("expecting bulk for slot host")),
        }
    }
}

impl ClusterConn {
    fn get_slot_conn(&mut self, slot: Option<u16>) -> Option<&mut SingleConn> {
        let range = slot
            .and_then(|slot| self.node_slots.slots.range(..=slot).next_back())
            .map(|(_, key)| *key);

        if let Some(key) = range {
            self.connections.get_mut(key)
        } else {
            self.random_conn()
        }
    }

    fn random_conn(&mut self) -> Option<&mut SingleConn> {
        let idx = if let Some(distribution) = &self.distribution {
            let mut rng = rand::thread_rng();
            distribution.sample(&mut rng)
        } else {
            0
        };
        self.connections.values_mut().nth(idx)
    }

    async fn refresh_slots(&mut self) -> Result<(), RedisError> {
        // FIXME: We should cache current connections to avoid unnecessary
        // re-connecting
        let conn = self
            .random_conn()
            .ok_or_else(|| cluster_error("no connections left"))?;
        let slot_resp: Vec<SlotResp> = conn.query(cmd!["CLUSTER", "SLOTS"]).await?;
        self.connect_slots(slot_resp).await?;
        Ok(())
    }

    pub async fn query<T>(&mut self, cmd: Cmd) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        self.req_packed_command(&cmd).await
    }

    pub async fn execute_script<T>(
        &mut self,
        eval_command: &Cmd,
        load_command: &Cmd,
    ) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        let mut tries = 0;
        // Get routing info from the eval command
        let slot = match RoutingInfo::for_packed_command(&eval_command.get_packed_command()) {
            Some(routing) => routing.slot(),
            None => {
                return Err((
                    ErrorKind::ClientError,
                    "this command cannot be safely routed in cluster mode",
                )
                    .into());
            }
        };

        loop {
            let conn = self.get_slot_conn(slot);

            let error = if let Some(conn) = conn {
                if conn.is_alive() {
                    match conn.execute_script(eval_command, load_command).await {
                        Ok(res) => return Ok(res),
                        Err(e) => {
                            // IO errors and MOVED responses indicate that the
                            // cluster may have been shuffled and our connections
                            // need refreshing. Everything else should be surfaced.
                            if !e.is_io_error() && e.code() != Some("MOVED") {
                                return Err(e);
                            } else {
                                e
                            }
                        }
                    }
                } else {
                    cluster_error("fetched connection for slot was not alive")
                }
            } else {
                cluster_error("couldn't fetch a connection for slot")
            };

            if tries <= RETRIES {
                tries += 1;
                tracing::warn!(
                    "Failed to fetch a connection for execute_script: {}, retrying. i={} max={}",
                    error,
                    tries,
                    RETRIES
                );
                self.refresh_slots().await?;
            } else {
                return Err(error);
            }
        }
    }

    pub async fn req_packed_command<T>(&mut self, cmd: &Cmd) -> Result<T, RedisError>
    where
        T: FromRedisValue + Send + 'static,
    {
        let mut tries = 0;
        let slot = match RoutingInfo::for_packed_command(&cmd.get_packed_command()) {
            Some(routing) => routing.slot(),
            None => {
                return Err((
                    ErrorKind::ClientError,
                    "this command cannot be safely routed in cluster mode",
                )
                    .into());
            }
        };

        loop {
            let conn = self.get_slot_conn(slot);
            let error = if let Some(conn) = conn {
                if conn.is_alive() {
                    match conn.req_packed_command(cmd).await {
                        Ok(res) => return Ok(res),
                        Err(e) => {
                            // IO errors and MOVED responses indicate that the
                            // cluster may have been shuffled and our connections
                            // need refreshing. Everything else should be surfaced.
                            if !e.is_io_error() && e.code() != Some("MOVED") {
                                return Err(e);
                            } else {
                                e
                            }
                        }
                    }
                } else {
                    cluster_error("fetched connection for slot was not alive")
                }
            } else {
                cluster_error("couldn't fetch a connection for slot")
            };

            if tries <= RETRIES {
                tries += 1;
                tracing::warn!("Failed to fetch a connection for req_packed_command: {}, retrying. i={} max={}", error, tries, RETRIES);
                self.refresh_slots().await?;
            } else {
                return Err(error);
            }
        }
    }

    pub fn is_alive(&self) -> bool {
        self.connections.values().all(SingleConn::is_alive)
    }

    pub async fn try_connect(infos: Vec<ConnectionInfo>) -> Result<Self, RedisError> {
        if infos.is_empty() {
            return Err(cluster_error("no connection info provided"));
        }

        let password = infos[0].redis.password.as_ref().cloned();
        let is_tls = match infos[0].addr.clone() {
            ConnectionAddr::TcpTls {
                host: _,
                port: _,
                insecure: _,
            } => true,
            _ => false,
        };

        let mut addresses = HashMap::new();
        let mut connections = SlotMap::new();

        for info in infos {
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
            let conn = match SingleConn::try_connect(info).await {
                Ok(conn) => conn,
                Err(_) => continue,
            };

            let key = connections.insert(conn);
            addresses.insert(address, key);
            break;
        }

        let mut cluster = ClusterConn {
            connections,
            node_slots: NodeSlots {
                addresses,
                slots: BTreeMap::new(),
            },
            password,
            distribution: None,
            is_tls,
        };

        cluster.refresh_slots().await?;

        Ok(cluster)
    }

    async fn connect_multiple<'a, I>(
        &self,
        addresses: I,
    ) -> Result<Vec<(&'a Address, SingleConn)>, RedisError>
    where
        I: Iterator<Item = &'a Address>,
    {
        let connections = addresses.map(|address| {
            SingleConn::try_connect(super::build_info(
                &address.host,
                address.port,
                self.password.as_deref(),
                self.is_tls,
            ))
            .map_ok(move |conn| (address, conn))
        });

        join_all(connections).await.into_iter().collect()
    }

    async fn connect_slots(&mut self, slots: Vec<SlotResp>) -> Result<(), RedisError> {
        let previous_connections = self.connections.len();
        let addresses = unique_addresses(&slots);

        let (mut remaining, removed): (HashMap<_, _>, HashMap<_, _>) = self
            .node_slots
            .addresses
            .drain()
            .partition(|(address, _)| addresses.contains(address));

        for (_, key) in removed {
            self.connections.remove(key);
        }

        // Drop dead connections so that they are reconnected
        remaining.retain(|_, key| {
            let conn = match self.connections.get(*key) {
                Some(conn) => conn,
                None => return false,
            };

            if conn.is_alive() {
                true
            } else {
                self.connections.remove(*key);
                false
            }
        });

        self.node_slots.addresses = remaining;

        let added = addresses
            .into_iter()
            .filter(|address| !self.node_slots.addresses.contains_key(*address));

        let new_connections = self.connect_multiple(added).await?;

        for (address, connection) in new_connections {
            let key = self.connections.insert(connection);
            self.node_slots.addresses.insert(address.clone(), key);
        }

        let mut new_slots = BTreeMap::new();
        for slot in slots {
            if let Some(key) = self.node_slots.addresses.get(&slot.address) {
                new_slots.insert(slot.start, *key);
            } else {
                // This is a programming error and should not happen
                tracing::warn!(
                    start = slot.start,
                    end = slot.end,
                    address = format!("{}", slot.address),
                    "Redis cluster: missing address for slot connection",
                );
            }
        }
        self.node_slots.slots = new_slots;

        if self.connections.len() != previous_connections || self.distribution.is_none() {
            self.distribution = Some(Uniform::new(0, self.connections.len()));
        }

        Ok(())
    }

    pub async fn ping(&mut self) -> Result<(), RedisError> {
        let mut tries = 0;
        'retry: loop {
            // Ping all connections concurrently
            let results = futures::future::join_all(
                self.connections
                    .values_mut()
                    .filter(|c| c.is_alive())
                    .map(|c| c.ping()),
            )
            .await;

            for res in results {
                // If there's an error, it could simply mean that the cluster has been shuffled.
                // Refresh and try again.
                if let Err(e) = res {
                    if tries <= RETRIES {
                        tries += 1;
                        self.refresh_slots().await?;
                        continue 'retry;
                    } else {
                        return Err(e);
                    }
                }
            }

            return Ok(());
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
        let mut res = HashMap::new();

        for key in keys {
            let args = key.to_redis_args();
            let bytes = if args.len() != 1 {
                Err(partition_error("multiple args for key"))
            } else {
                Ok(&args[0])
            }?;
            let target_slot = RoutingInfo::for_key(bytes)
                .and_then(|routing_info| routing_info.slot())
                .ok_or_else(|| partition_error("no routing info for key"))?;
            let target_key = self
                .node_slots
                .slots
                .range(0..=target_slot)
                .next_back()
                .map(|(_, key)| *key)
                .ok_or_else(|| partition_error("unknown slot"))?;
            let address = self
                .node_slots
                .addresses
                .iter()
                .find(|(_, &key)| target_key == key)
                .map(|(address, _)| address)
                .ok_or_else(|| partition_error("unknown address"))?;

            let entry = res.entry(address.clone()).or_insert_with(Vec::new);
            entry.push(key);
        }

        Ok(res)
    }
}

fn unique_addresses(slots: &[SlotResp]) -> HashSet<&Address> {
    slots.iter().map(|slot| &slot.address).collect()
}

fn get_hashtag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|v| *v == b'{');
    let open = match open {
        Some(open) => open,
        None => return None,
    };

    let close = key[open..].iter().position(|v| *v == b'}');
    let close = match close {
        Some(close) => close,
        None => return None,
    };

    let rv = &key[open + 1..open + close];
    if rv.is_empty() {
        None
    } else {
        Some(rv)
    }
}

/// Taken from redis-rs cluster support
#[derive(Debug, Clone, Copy)]
enum RoutingInfo {
    Random,
    Slot(u16),
}

fn get_arg(values: &[Value], idx: usize) -> Option<&[u8]> {
    match values.get(idx) {
        Some(Value::Data(ref data)) => Some(&data[..]),
        _ => None,
    }
}

fn get_command_arg(values: &[Value], idx: usize) -> Option<Vec<u8>> {
    get_arg(values, idx).map(|x| x.to_ascii_uppercase())
}

fn get_u64_arg(values: &[Value], idx: usize) -> Option<u64> {
    get_arg(values, idx)
        .and_then(|x| std::str::from_utf8(x).ok())
        .and_then(|x| x.parse().ok())
}

impl RoutingInfo {
    pub fn slot(&self) -> Option<u16> {
        match self {
            RoutingInfo::Random => None,
            RoutingInfo::Slot(slot) => Some(*slot),
        }
    }

    pub fn for_packed_command(cmd: &[u8]) -> Option<RoutingInfo> {
        parse_redis_value(cmd).ok().and_then(RoutingInfo::for_value)
    }

    pub fn for_value(value: Value) -> Option<RoutingInfo> {
        let args = match value {
            Value::Bulk(args) => args,
            _ => return None,
        };

        match &get_command_arg(&args, 0)?[..] {
            b"SCAN" | b"CLIENT SETNAME" | b"SHUTDOWN" | b"SLAVEOF" | b"REPLICAOF"
            | b"SCRIPT KILL" | b"MOVE" | b"BITOP" => None,
            b"EVALSHA" | b"EVAL" => {
                let key_count = get_u64_arg(&args, 2)?;
                if key_count == 0 {
                    Some(RoutingInfo::Random)
                } else {
                    get_arg(&args, 3).and_then(RoutingInfo::for_key)
                }
            }
            b"XGROUP" | b"XINFO" => get_arg(&args, 2).and_then(RoutingInfo::for_key),
            b"XREAD" | b"XREADGROUP" => {
                let streams_position = args.iter().position(|a| match a {
                    Value::Data(a) => a == b"STREAMS",
                    _ => false,
                })?;
                get_arg(&args, streams_position + 1).and_then(RoutingInfo::for_key)
            }
            _ => match get_arg(&args, 1) {
                Some(key) => RoutingInfo::for_key(key),
                None => Some(RoutingInfo::Random),
            },
        }
    }

    pub fn for_key(key: &[u8]) -> Option<RoutingInfo> {
        let key = match get_hashtag(key) {
            Some(tag) => tag,
            None => key,
        };
        Some(RoutingInfo::Slot(
            crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE,
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_routing() {
        let key: &[u8] = b"[dbreq.approvedeviceemail]\0\0\0\0\0\nP\x08\x01";
        let slot = match RoutingInfo::for_key(key) {
            Some(RoutingInfo::Slot(x)) => x,
            _ => panic!("Expected slot"),
        };
        assert_eq!(8505, slot);

        let cmd: &[u8] =
            b"*2\r\n$3\r\nGET\r\n$35\r\n[dbreq.approvedeviceemail]\0\0\0\0\0\nP\x08\x01\r\n";
        let slot = match RoutingInfo::for_packed_command(cmd) {
            Some(RoutingInfo::Slot(x)) => x,
            _ => panic!("Expected slot"),
        };
        assert_eq!(8505, slot);
    }
}
