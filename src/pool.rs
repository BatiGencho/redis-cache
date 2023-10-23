use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use futures::future::BoxFuture;
use std::{
    fmt::Debug,
    io,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{self, AtomicUsize},
        Arc, Weak,
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::sleep,
};

#[async_trait]
pub trait ConnectionManager {
    /// Any information needed to connect to a database
    /// e.g. IP, port, username, password
    type Address: Clone + Send + Sync;

    /// A connection to the database
    type Connection: Sized + Send + Sync;

    /// All operations may return this error type
    type Error: From<io::Error> + Send;

    /// Connect to a given address
    async fn connect(address: &Self::Address) -> Result<Self::Connection, Self::Error>;

    /// Check if the connection is in a good state.
    /// If None, the status is unknown and the database should be pinged.
    fn check_alive(connection: &Self::Connection) -> Option<bool>;

    /// Ping the database
    async fn ping(connection: &mut Self::Connection) -> Result<(), Self::Error>;

    /// Reset the connection to a fresh state for reuse
    /// If this doesn't perform a network operation, returns None
    fn reset_connection(
        _connection: &mut Self::Connection,
    ) -> Option<BoxFuture<'_, Result<(), Self::Error>>> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigBuilder<C: ConnectionManager> {
    pub address: Option<C::Address>,
    pub min_size: Option<usize>,
    pub max_size: Option<usize>,
}

// #[derive(Default)] doesn't work, I think b/c of the type parameter
impl<C: ConnectionManager> Default for ConfigBuilder<C> {
    fn default() -> Self {
        ConfigBuilder {
            address: None,
            min_size: None,
            max_size: None,
        }
    }
}

impl<C: ConnectionManager> ConfigBuilder<C> {
    pub fn new() -> ConfigBuilder<C> {
        Self::default()
    }

    pub fn address(&mut self, val: C::Address) -> &mut Self {
        self.address = Some(val);
        self
    }

    pub fn min_size(&mut self, val: Option<usize>) -> &mut Self {
        self.min_size = val;
        self
    }

    pub fn max_size(&mut self, val: Option<usize>) -> &mut Self {
        self.max_size = val;
        self
    }

    pub fn build(&mut self) -> Config<C> {
        Config {
            address: self
                .address
                .take()
                .expect("ConfigBuilder address not specified"),
            min_size: self.min_size.take().unwrap_or(0),
            max_size: self.max_size.take().unwrap_or(100),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config<C: ConnectionManager> {
    pub address: C::Address,
    pub min_size: usize,
    pub max_size: usize,
}

struct PoolShared<C: ConnectionManager> {
    config: Config<C>,
    idle_queue: ArrayQueue<C::Connection>,
    /// Approximate at any given time because it's concurrent
    idle_queue_len: AtomicUsize,
    permits: Arc<Semaphore>,
}

pub struct Pool<C: ConnectionManager>(Arc<PoolShared<C>>);

impl<C: ConnectionManager> Clone for Pool<C> {
    fn clone(&self) -> Pool<C> {
        Pool(self.0.clone())
    }
}

pub struct PoolConnection<C: ConnectionManager + 'static> {
    connection: Option<(C::Connection, OwnedSemaphorePermit)>,
    pool: Pool<C>,
}

impl<C: ConnectionManager> Deref for PoolConnection<C> {
    type Target = C::Connection;

    fn deref(&self) -> &C::Connection {
        &self
            .connection
            .as_ref()
            .expect("PoolConnection doesn't have an underlying connection")
            .0
    }
}

impl<C: ConnectionManager> DerefMut for PoolConnection<C> {
    fn deref_mut(&mut self) -> &mut C::Connection {
        &mut self
            .connection
            .as_mut()
            .expect("PoolConnection doesn't have an underlying connection")
            .0
    }
}

impl<C: ConnectionManager> Drop for PoolConnection<C> {
    fn drop(&mut self) {
        let connection = match self.connection.take() {
            Some(c) => c,
            None => return,
        };
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let (mut conn, _permit) = connection;
            let is_alive = match C::reset_connection(&mut conn) {
                Some(fut) => Some(fut.await.is_ok()),
                None => None,
            };
            let is_alive = match is_alive {
                Some(x) => x,
                None => C::check_alive(&conn).unwrap_or(true),
            };
            if is_alive {
                // Ignore if we can't recycle because the queue is full?
                // TODO: should log
                if pool.0.idle_queue.push(conn).is_ok() {
                    pool.0
                        .idle_queue_len
                        .fetch_add(1, atomic::Ordering::Relaxed);
                }
            }
        });
    }
}

impl<C: ConnectionManager> Pool<C>
where
    C: 'static,
    C::Address: Debug,
    C::Error: Debug,
{
    /// Creates a new pool and fills it to the minimum idle size.
    pub async fn new(config: Config<C>) -> Self {
        let idle_queue = ArrayQueue::new(config.max_size);
        let mut init_len = 0;
        assert!(config.max_size >= config.min_size);
        let mut some_failed = false;
        for _ in 0..config.min_size {
            match C::connect(&config.address).await {
                Ok(conn) => {
                    idle_queue
                        .push(conn)
                        .ok()
                        .expect("Pool queue must have the capacity to allocate idle connections");
                    init_len += 1;
                }
                Err(err) => {
                    if !some_failed {
                        some_failed = true;
                        tracing::warn!(
                            "During pool initial connections to {:?} {:?}",
                            config.address,
                            err,
                        );
                    }
                }
            }
        }
        let permits = Arc::new(Semaphore::new(config.max_size));
        let this = Pool(Arc::new(PoolShared {
            idle_queue,
            idle_queue_len: AtomicUsize::new(init_len),
            config,
            permits,
        }));
        tokio::spawn(Self::keepalive(Arc::downgrade(&this.0)));
        this
    }

    /// Keeps idle pool connections alive by pinging them regularly.
    async fn keepalive(weak: Weak<PoolShared<C>>) {
        loop {
            let mut idle_count;
            {
                let this = match weak.upgrade() {
                    Some(arc) => Pool(arc),
                    None => return,
                };

                if let Some(mut conn) = this.try_get_idle_connection().await {
                    if let Err(err) = C::ping(&mut conn).await {
                        tracing::warn!("Failed to ping DB connection: {:?}", err);
                    }
                }
                idle_count = this.0.idle_queue_len.load(atomic::Ordering::Relaxed);
            }
            if idle_count == 0 {
                idle_count = 1;
            }
            let delay = Duration::from_secs(60) / (idle_count as u32);
            sleep(delay).await;
        }
    }

    /// Get an idle connection from the queue
    fn idle_connection(&self) -> Option<C::Connection> {
        let connection = self.0.idle_queue.pop()?;
        self.0
            .idle_queue_len
            .fetch_sub(1, atomic::Ordering::Relaxed);
        Some(connection)
    }

    /// Attempt to get an idle connection from the pool, along with it's permit.
    /// This method will not replenish the number of idle connections.
    async fn try_get_idle_connection(&self) -> Option<PoolConnection<C>> {
        let permit = self.0.permits.clone().try_acquire_owned().ok()?;
        Some(PoolConnection {
            connection: Some((self.idle_connection()?, permit)),
            pool: (*self).clone(),
        })
    }

    /// Get a connection from the pool or create a new one. If the pool is at capacity
    /// this will wait until a connection is available.
    pub async fn get_connection(&self) -> Result<PoolConnection<C>, C::Error> {
        let permit = self
            .0
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Connection pool closed"))?;
        self.get_connection_internal(permit).await
    }

    /// Attempt to get a connection from the pool or create a new one. If the pool is at
    /// capacity this will return an error without waiting.
    pub async fn try_get_connection(&self) -> Result<PoolConnection<C>, C::Error> {
        let permit = self.0.permits.clone().try_acquire_owned().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "Connection pool size reached maximum")
        })?;
        self.get_connection_internal(permit).await
    }

    /// Attempt to get a connection from the pool or create a new one, waiting up to `timeout`
    /// before returning an error if the pool is at capacity.
    pub async fn get_connection_timeout(
        &self,
        timeout: Duration,
    ) -> Result<PoolConnection<C>, C::Error> {
        let permit = tokio::time::timeout(timeout, self.0.permits.clone().acquire_owned())
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "Connection pool size reached maximum")
            })?
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Connection pool closed"))?;
        self.get_connection_internal(permit).await
    }

    fn create_connection(
        &self,
        permit: OwnedSemaphorePermit,
        conn: C::Connection,
    ) -> PoolConnection<C> {
        PoolConnection {
            connection: Some((conn, permit)),
            pool: (*self).clone(),
        }
    }

    /// Returns a connection from the pool. If there's no idle connection, creates a new one.
    async fn get_connection_internal(
        &self,
        mut permit: OwnedSemaphorePermit,
    ) -> Result<PoolConnection<C>, C::Error> {
        loop {
            match self.idle_connection() {
                Some(c) => {
                    // Wrap the connection in a PoolConnection to ensure it's returned to the queue
                    // if this future is canceled
                    let mut conn = self.create_connection(permit, c);
                    let alive = match C::check_alive(&conn) {
                        Some(alive) => alive,
                        None => C::ping(&mut conn).await.is_ok(),
                    };
                    if alive {
                        break Ok(conn);
                    } else {
                        // Extract the bad connection from the PoolConnection so that it's not
                        // returned to the queue
                        let c = conn
                            .connection
                            .take()
                            .expect("PoolConnection doesn't have an underlying connection");
                        permit = c.1;
                    }
                }
                None => {
                    let conn = C::connect(&self.0.config.address).await?;
                    break Ok(self.create_connection(permit, conn));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::time::Duration;
    use tokio::time::timeout;

    struct TestConnection;

    #[async_trait]
    impl ConnectionManager for TestConnection {
        type Address = ();
        type Connection = Self;
        type Error = Error;
        async fn connect(_address: &Self::Address) -> Result<Self::Connection, Self::Error> {
            Ok(TestConnection)
        }

        fn check_alive(_connection: &Self::Connection) -> Option<bool> {
            Some(true)
        }

        async fn ping(_connection: &mut Self::Connection) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_connection_pool() {
        let config = ConfigBuilder::<TestConnection>::new()
            .address(())
            .max_size(Some(3))
            .build();

        let pool = Pool::<TestConnection>::new(config).await;

        let mut connections = Vec::with_capacity(3);
        // Get all 3 connections we can have
        for _ in 0..3 {
            connections.push(
                pool.try_get_connection()
                    .await
                    .expect("Unable to get connection"),
            );
        }

        // Check if getting a new connection fails
        assert!(pool.try_get_connection().await.is_err());

        // Pop a connection, this will drop the pool connection
        // and eventually free the connection slot
        connections.pop();

        // Check if we can get the connection, get_connection will wait until one is available
        timeout(Duration::from_millis(1), pool.get_connection())
            .await
            .expect("get_connection timed out")
            .expect("Unable to get connection");
    }
}
