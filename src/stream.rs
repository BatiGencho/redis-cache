use displaydoc::Display;
use redis::{FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value};
use std::{
    cmp::Ord,
    fmt,
    hash::Hash,
    num::ParseIntError,
    str::FromStr,
    time::{Duration, SystemTime},
};
use strum::IntoStaticStr;
use thiserror::Error;

#[derive(Debug, Display, Error)]
pub enum Error {
    /// Invalid message id: timestamp={0} sequence={1} id={2}
    InvalidMessageId(ParseIntError, ParseIntError, String),
    /// Invalid message id timestamp: timestamp={0} id={1}
    InvalidMessageIdTimestamp(ParseIntError, String),
    /// Invalid message id sequence number: sequence={0} id={1}
    InvalidMessageIdSequence(ParseIntError, String),
    /// Malformed message id: id={0}
    MalformedMessageId(String),
}

#[derive(Debug, Clone)]
pub struct ReadStream<'s, S>
where
    &'s S: ToRedisArgs,
{
    pub id: &'s S,
    pub offset: MessageId,
}

/// Options for XADD command
#[derive(Debug, Copy, Clone, Default)]
pub struct WriteStreamOptions {
    // Do not create new stream if nonexistent
    pub disable_create: bool,
    // Args for capping the stream length during a write
    pub capacity: Option<(Trim, TrimPrecision)>,
}

/// Options for XREAD command
#[derive(Debug, Copy, Clone, Default)]
pub struct ReadStreamOptions {
    // Block until response available or timeout
    pub block: Option<Duration>,
    // Maximum items returned from read
    pub count: Option<u64>,
}

#[derive(Debug, Copy, Clone)]
pub enum Trim {
    Length(u64),
    Id(MessageId),
}

#[derive(Debug, Copy, Clone, IntoStaticStr)]
pub enum TrimPrecision {
    #[strum(to_string = "=")]
    Exact,
    #[strum(to_string = "~")]
    Approximate,
}

impl ToRedisArgs for &WriteStreamOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if self.disable_create {
            out.write_arg(b"NOMKSTREAM")
        }
        match self.capacity {
            Some((Trim::Length(len), precision)) => {
                let precision: &'static str = precision.into();
                out.write_arg(b"MAXLEN");
                out.write_arg(precision.as_bytes());
                out.write_arg(len.to_string().as_bytes());
            }
            Some((Trim::Id(id), precision)) => {
                let precision: &'static str = precision.into();
                out.write_arg(b"MINID");
                out.write_arg(precision.as_bytes());
                out.write_arg(id.to_string().as_bytes());
            }
            None => {}
        }
    }
}

impl ToRedisArgs for &ReadStreamOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(count) = self.count {
            out.write_arg(b"COUNT");
            out.write_arg(count.to_string().as_bytes());
        }
        if let Some(block) = self.block {
            out.write_arg(b"BLOCK");
            out.write_arg(block.as_millis().to_string().as_bytes());
        }
    }
}

#[derive(Debug)]
pub struct StreamReadReply<S: FromRedisValue, T: FromRedisValue>(pub Vec<StreamItems<S, T>>);

#[derive(Debug)]
pub struct StreamItems<S: FromRedisValue, T: FromRedisValue> {
    pub id: S,
    pub items: Vec<StreamItem<T>>,
}

#[derive(Debug)]
pub struct StreamItem<T: FromRedisValue> {
    pub offset: MessageId,
    pub payload: Result<T, RedisError>,
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageId {
    pub timestamp_ms: u128,
    pub sequence: u64,
}

impl MessageId {
    pub fn now() -> Self {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("System time before Unix epoch");
        Self {
            timestamp_ms: now.as_millis(),
            sequence: 0,
        }
    }
}

impl FromStr for MessageId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('-').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(Error::MalformedMessageId(s.to_string()));
        }

        match (parts[0].parse::<u128>(), parts[1].parse::<u64>()) {
            (Ok(timestamp_ms), Ok(sequence)) => Ok(MessageId {
                timestamp_ms,
                sequence,
            }),
            (Err(ts_err), Ok(_)) => Err(Error::InvalidMessageIdTimestamp(ts_err, s.to_string())),
            (Ok(_), Err(sequence_err)) => {
                Err(Error::InvalidMessageIdSequence(sequence_err, s.to_string()))
            }
            (Err(ts_err), Err(sequence_err)) => {
                Err(Error::InvalidMessageId(ts_err, sequence_err, s.to_string()))
            }
        }
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.timestamp_ms, self.sequence)
    }
}

impl FromRedisValue for MessageId {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        String::from_redis_value(v)?
            .parse::<MessageId>()
            .map_err(|e| {
                redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "invalid value for MessageId",
                    e.to_string(),
                ))
            })
    }
}

impl ToRedisArgs for MessageId {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.to_string().as_bytes())
    }
}

impl<S, T> FromRedisValue for StreamReadReply<S, T>
where
    S: FromRedisValue + Eq + Hash,
    T: FromRedisValue,
{
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Bulk(bulk) => {
                let stream_reply = bulk
                    .iter()
                    .map(|streams| match streams {
                        redis::Value::Bulk(bulk) => {
                            let mut iter = bulk.iter();
                            let id = get_next_value::<_, S>(
                                &mut iter,
                                "missing key `id` in `StreamItems` response",
                            )?;
                            let items = get_next_value::<_, Vec<StreamItem<T>>>(
                                &mut iter,
                                "missing key `items` in `StreamItems` response",
                            )?;
                            Ok(StreamItems { id, items })
                        }
                        _ => Err(redis::RedisError::from((
                            redis::ErrorKind::TypeError,
                            "expecting Value::Bulk for `StreamItems` response",
                        ))),
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Self(stream_reply))
            }
            Value::Nil => Ok(Self(vec![])),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "expecting Value::Bulk for `StreamReadReply` response",
            ))),
        }
    }
}

impl<T> FromRedisValue for StreamItem<T>
where
    T: FromRedisValue,
{
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            redis::Value::Bulk(bulk) => {
                let mut iter = bulk.iter();
                let offset = get_next_value::<_, MessageId>(
                    &mut iter,
                    "missing key `offset` in `StreamItem` response",
                )?;
                let payload = get_next_value::<_, T>(
                    &mut iter,
                    "missing key `payload` in `StreamItem` response",
                );

                Ok(StreamItem { offset, payload })
            }
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "expecting Value::Bulk for `StreamItem` response",
            ))),
        }
    }
}

fn get_next_value<'v, I, T>(iter: &mut I, err_msg: &'static str) -> Result<T, redis::RedisError>
where
    I: Iterator<Item = &'v Value>,
    T: FromRedisValue,
{
    iter.next()
        .ok_or_else(|| redis::RedisError::from((redis::ErrorKind::TypeError, err_msg)))
        .and_then(T::from_redis_value)
}

#[cfg(test)]
mod tests {
    use super::{
        MessageId, ReadStreamOptions, StreamReadReply, Trim, TrimPrecision, WriteStreamOptions,
    };
    use redis::{FromRedisValue, ToRedisArgs, Value};
    use std::{collections::HashMap, str, time::Duration};

    #[test]
    fn stream_write_message_id_to_args() {
        let bytes = MessageId {
            timestamp_ms: 111,
            sequence: 22,
        }
        .to_redis_args();
        assert_eq!(bytes.len(), 1);
        assert_eq!(str::from_utf8(bytes[0].as_slice()).unwrap(), "111-22");
    }

    #[test]
    fn stream_write_options() {
        let bytes = (&WriteStreamOptions::default()).to_redis_args();
        assert_eq!(bytes.len(), 0);
        let bytes = (&WriteStreamOptions {
            disable_create: true,
            capacity: Some((Trim::Length(10), TrimPrecision::Approximate)),
        })
            .to_redis_args();
        assert_eq!(bytes.len(), 4);
        assert_eq!(str::from_utf8(bytes[0].as_slice()).unwrap(), "NOMKSTREAM");
        assert_eq!(str::from_utf8(bytes[1].as_slice()).unwrap(), "MAXLEN");
        assert_eq!(str::from_utf8(bytes[2].as_slice()).unwrap(), "~");
        assert_eq!(str::from_utf8(bytes[3].as_slice()).unwrap(), "10");
    }

    #[test]
    fn stream_read_options() {
        let bytes = (&ReadStreamOptions::default()).to_redis_args();
        assert_eq!(bytes.len(), 0);
        let bytes = (&ReadStreamOptions {
            block: Some(Duration::from_secs(1)),
            count: Some(50),
        })
            .to_redis_args();
        assert_eq!(bytes.len(), 4);
        assert_eq!(str::from_utf8(bytes[0].as_slice()).unwrap(), "COUNT");
        assert_eq!(str::from_utf8(bytes[1].as_slice()).unwrap(), "50");
        assert_eq!(str::from_utf8(bytes[2].as_slice()).unwrap(), "BLOCK");
        assert_eq!(str::from_utf8(bytes[3].as_slice()).unwrap(), "1000");
    }

    #[test]
    fn stream_message_id_ordering() {
        let msg = MessageId {
            timestamp_ms: 5,
            sequence: 0,
        };

        assert!(
            msg > MessageId {
                timestamp_ms: 4,
                sequence: 0
            }
        );
        assert!(
            msg == MessageId {
                timestamp_ms: 5,
                sequence: 0
            }
        );
        assert!(
            msg < MessageId {
                timestamp_ms: 5,
                sequence: 1
            }
        );
        assert!(
            msg < MessageId {
                timestamp_ms: 6,
                sequence: 0
            }
        );
    }
    #[test]
    fn stream_message_id_display() {
        assert_eq!(
            MessageId {
                timestamp_ms: 1000,
                sequence: 3
            }
            .to_string(),
            "1000-3".to_string()
        )
    }

    #[test]
    fn stream_message_id_deser() {
        let raw = Value::Data(b"1-0".to_vec());
        let deserialized = MessageId::from_redis_value(&raw);
        assert_eq!(
            deserialized.unwrap(),
            MessageId {
                timestamp_ms: 1,
                sequence: 0
            }
        );
        let raw = Value::Data(b"1636634305271-1".to_vec());
        let deserialized = MessageId::from_redis_value(&raw);
        assert_eq!(
            deserialized.unwrap(),
            MessageId {
                timestamp_ms: 1636634305271,
                sequence: 1
            }
        );

        let raw = Value::Data(b"18446744073709551615-99".to_vec());
        let deserialized = MessageId::from_redis_value(&raw);
        assert_eq!(
            deserialized.unwrap(),
            MessageId {
                timestamp_ms: 18446744073709551615,
                sequence: 99
            }
        );

        // invalid
        let raw = Value::Data(b"123".to_vec());
        let deserialized = MessageId::from_redis_value(&raw);
        assert!(deserialized.is_err());
        let raw = Value::Data(b"123-a".to_vec());
        let deserialized = MessageId::from_redis_value(&raw);
        assert!(deserialized.is_err());
        let raw = Value::Data(b"a23-0".to_vec());
        let deserialized = MessageId::from_redis_value(&raw);
        assert!(deserialized.is_err());
    }

    #[test]
    fn stream_read_reply_deser_nil() {
        let nil = Value::Nil;
        let deserialized =
            StreamReadReply::<String, Vec<(String, String)>>::from_redis_value(&nil).unwrap();
        assert!(deserialized.0.is_empty());
    }

    #[test]
    fn stream_read_reply_deser_one() {
        let raw = Value::Bulk(vec![Value::Bulk(vec![
            Value::Data(b"stream-id".to_vec()),
            Value::Bulk(vec![Value::Bulk(vec![
                Value::Data(b"1000-2".to_vec()),
                Value::Bulk(vec![
                    Value::Data(b"key".to_vec()),
                    Value::Data(b"value".to_vec()),
                    Value::Data(b"key2".to_vec()),
                    Value::Data(b"value".to_vec()),
                ]),
            ])]),
        ])]);
        let deserialized =
            StreamReadReply::<String, HashMap<String, String>>::from_redis_value(&raw).unwrap();
        assert_eq!(deserialized.0.len(), 1);
        assert_eq!(deserialized.0[0].id, "stream-id".to_string());
        assert_eq!(deserialized.0[0].items.len(), 1);
        assert_eq!(
            deserialized.0[0].items[0].offset,
            MessageId {
                timestamp_ms: 1000,
                sequence: 2
            }
        );
        assert_eq!(
            deserialized.0[0].items[0].payload,
            Ok(HashMap::from([
                ("key".to_string(), "value".to_string()),
                ("key2".to_string(), "value".to_string())
            ]))
        );
    }

    #[test]
    fn stream_read_reply_deser_many() {
        let raw = Value::Bulk(vec![
            Value::Bulk(vec![
                Value::Data(b"stream-0".to_vec()),
                Value::Bulk(vec![
                    Value::Bulk(vec![
                        Value::Data(b"0-0".to_vec()),
                        Value::Bulk(vec![
                            Value::Data(b"key0-0".to_vec()),
                            Value::Data(b"value0-0".to_vec()),
                            Value::Data(b"key0-1".to_vec()),
                            Value::Data(b"value0-1".to_vec()),
                        ]),
                    ]),
                    Value::Bulk(vec![
                        Value::Data(b"0-1".to_vec()),
                        Value::Bulk(vec![
                            Value::Data(b"key0-0".to_vec()),
                            Value::Data(b"value0-0".to_vec()),
                        ]),
                    ]),
                ]),
            ]),
            Value::Bulk(vec![
                Value::Data(b"stream-1".to_vec()),
                Value::Bulk(vec![Value::Bulk(vec![
                    Value::Data(b"1-0".to_vec()),
                    Value::Bulk(vec![]),
                ])]),
            ]),
            Value::Bulk(vec![
                Value::Data(b"stream-2".to_vec()),
                Value::Bulk(vec![Value::Bulk(vec![
                    Value::Data(b"2-0".to_vec()),
                    Value::Bulk(vec![
                        Value::Data(b"key2-0".to_vec()),
                        Value::Data(b"value2-0".to_vec()),
                        Value::Data(b"key2-1".to_vec()),
                        Value::Data(b"value2-1".to_vec()),
                        Value::Data(b"key2-2".to_vec()),
                        Value::Data(b"value2-2".to_vec()),
                    ]),
                ])]),
            ]),
        ]);

        let deserialized =
            StreamReadReply::<String, HashMap<String, String>>::from_redis_value(&raw).unwrap();
        assert_eq!(deserialized.0.len(), 3);
        for (stream_idx, stream_reply) in deserialized.0.into_iter().enumerate() {
            assert_eq!(stream_reply.id, format!("stream-{}", stream_idx));
            for (item_idx, item) in stream_reply.items.into_iter().enumerate() {
                assert_eq!(
                    item.offset,
                    MessageId {
                        timestamp_ms: stream_idx as u128,
                        sequence: item_idx as u64
                    }
                );
                let payload = item.payload.unwrap();
                assert_eq!(
                    payload,
                    std::iter::repeat(0)
                        .take(payload.len())
                        .enumerate()
                        .map(|(idx, _)| {
                            (
                                format!("key{}-{}", stream_idx, idx),
                                format!("value{}-{}", stream_idx, idx),
                            )
                        })
                        .collect()
                );
            }
        }
    }
}
