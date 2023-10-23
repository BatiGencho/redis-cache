use super::{Error, Store};
use async_trait::async_trait;
use redis::{FromRedisValue, ToRedisArgs};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

/// The type of the key that `redis::ToRedisArgs::to_redis_args` exposes
/// that we can use to lookup in our HashMap.
type Key = Vec<Vec<u8>>;

struct RedisMockEntry {
    value: redis::Value,
    expires: Instant,
}

#[derive(Default, Clone)]
pub struct RedisMock(Arc<RwLock<HashMap<Key, RedisMockEntry>>>);

#[async_trait]
impl Store for RedisMock {
    async fn get_key<K, T>(&self, key: K) -> Result<T, Error>
    where
        // doesn't have to live past an await point but async_trait
        // messes with this
        K: ToRedisArgs + Send,
        T: FromRedisValue + Send + 'static,
    {
        self.0
            .read()
            .await
            .get(&key.to_redis_args())
            .filter(|e| e.expires > Instant::now())
            .ok_or(Error::KeyDoesNotExist)
            .and_then(|e| T::from_redis_value(&e.value).map_err(Error::from))
    }

    async fn set_key<K, D>(&self, key: K, ttl: Duration, data: D) -> Result<(), Error>
    where
        K: ToRedisArgs + Send,
        D: ToRedisArgs + Send,
    {
        let key = key.to_redis_args();
        let data = data.to_redis_args();

        let entry = RedisMockEntry {
            value: convert_data_to_value(data)?,
            expires: Instant::now() + ttl,
        };

        self.0.write().await.insert(key, entry);

        Ok(())
    }

    async fn del_key<T: ToRedisArgs + Send>(&self, key: T) -> Result<Option<u64>, Error> {
        Ok(self.0.write().await.remove(&key.to_redis_args()).map(|_| 1))
    }

    async fn del_keys<'a, T>(&self, keys: &'a [T]) -> Result<Vec<Option<u64>>, Error>
    where
        T: Sync,
        &'a T: ToRedisArgs,
    {
        let mut map = self.0.write().await;

        Ok(keys
            .iter()
            .map(|k| map.remove(&k.to_redis_args()).map(|_| 1))
            .collect())
    }

    async fn set_if_not_exists<K, D>(&self, key: K, ttl: Duration, data: D) -> Result<bool, Error>
    where
        K: ToRedisArgs + Send,
        D: ToRedisArgs + Send,
    {
        use std::collections::hash_map::Entry;

        let mut map = self.0.write().await;

        match map.entry(key.to_redis_args()) {
            Entry::Occupied(_) => Ok(false),
            Entry::Vacant(entry) => {
                entry.insert(RedisMockEntry {
                    value: convert_data_to_value(data)?,
                    expires: Instant::now() + ttl,
                });
                Ok(true)
            }
        }
    }

    async fn sorted_set_add_one<D>(&self, key: &str, _score: i64, data: D) -> Result<(), Error>
    where
        D: ToRedisArgs + Send,
    {
        use std::collections::hash_map::Entry;

        let mut map = self.0.write().await;

        let entry = match map.entry(key.to_redis_args()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(RedisMockEntry {
                value: redis::Value::Bulk(Vec::new()),
                expires: Instant::now() + Duration::from_secs(86_400 * 365),
            }),
        };

        match &mut entry.value {
            redis::Value::Bulk(v) => {
                v.push(convert_data_to_value(data)?);
                Ok(())
            }
            _ => Err(Error::HostParseError("not a bulk entry")),
        }
    }
}

fn convert_data_to_value<D>(data: D) -> Result<redis::Value, Error>
where
    D: ToRedisArgs,
{
    let args = data.to_redis_args();

    if args.is_single_arg() {
        args.into_iter()
            .next()
            .ok_or_else(|| Error::RedisInsertionError("missing data?".to_string()))
            .map(redis::Value::Data)
    } else {
        Ok(redis::Value::Bulk(
            args.into_iter().map(redis::Value::Data).collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::RedisMock;
    use crate::Store;
    use std::time::Duration;

    #[tokio::test]
    pub async fn simple_insert_retrieve() {
        // test insert
        let store = RedisMock::default();
        store
            .set_key("my-cool-key", Duration::from_secs(10), "oh yes indeed")
            .await
            .unwrap();

        // test retrieve
        let val: String = store.get_key("my-cool-key").await.unwrap();
        assert_eq!(val, "oh yes indeed");

        // test delete
        let del = store.del_key("my-cool-key").await.unwrap();
        assert_eq!(del, Some(1));

        // test retrieve after delete
        store
            .get_key::<_, String>("my-cool-key")
            .await
            .expect_err("key still exists in map");
    }

    #[tokio::test]
    pub async fn test_expires() {
        // test insert with expires of 0
        let store = RedisMock::default();
        store
            .set_key("my-cool-key", Duration::from_millis(0), "oh yes indeed")
            .await
            .unwrap();

        // test retrieve immediately doesn't return
        store
            .get_key::<_, String>("my-cool-key")
            .await
            .expect_err("key still exists in map after expiry");
    }
}
