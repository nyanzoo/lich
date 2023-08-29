use std::{collections::BTreeMap, fs::remove_dir, path::PathBuf};

use hashlink::LinkedHashMap;
use log::{trace, warn};

use necronomicon::{
    dequeue_codec::{self, Create, Delete, Enqueue},
    kv_store_codec::Put,
    Encode, Packet, FAILED_TO_PUSH_TO_TRANSACTION_LOG, INTERNAL_ERROR, KEY_DOES_NOT_EXIST,
    QUEUE_ALREADY_EXISTS, QUEUE_DOES_NOT_EXIST,
};
use phylactery::{
    buffer::MmapBuffer,
    dequeue::Dequeue,
    entry::Version,
    kv_store::{Graveyard, KVStore, Lookup},
    ring_buffer::{self, ring_buffer},
};
use serde::de;

use crate::{config::Config, error::Error, reqres::ClientRequest, util::megabytes};

#[derive(Clone, Debug)]
enum CommitStatus {
    Committed(ClientRequest),
    Pending(ClientRequest),
}

impl CommitStatus {
    pub fn complete(&mut self) {
        match self {
            Self::Committed(_) => warn!("already committed"),
            Self::Pending(patch) => *self = Self::Committed(patch.clone()),
        }
    }
}

// Store needs to take in patches for pending and later commit upon ack if not tail.
// What we need to think through is how to lower the number of copies and still be correct.
// We could keep things in memory and then write to disk on ack. Which is probably the best option.
// We also need to write some extra data to disk for the transaction log.

pub struct Store<S>
where
    S: AsRef<str>,
{
    kvs: KVStore<S>,
    root: String,
    version: Version,
    dequeues: BTreeMap<S, Dequeue<S>>,
    pending: hashlink::LinkedHashMap<u128, CommitStatus>,
    transaction_log_tx: ring_buffer::Pusher<MmapBuffer>,
    transaction_log_rx: ring_buffer::Popper<MmapBuffer>,
}

impl Store<String> {
    /// # Description
    /// Create a new store. There should be only one store per node.
    /// Users are allowed to have a single KV Store and multiple Dequeues.
    ///
    /// Additionally, this will spawn a thread for the graveyard (GC of disk fragmentation).
    ///
    /// # Arguments
    /// * `config`: The configuration for the store.
    ///
    /// # Errors
    /// See `error::Error` for details.
    pub fn new(config: &Config) -> Result<Self, Error> {
        let mmap_path = format!("{}/mmap.bin", config.path);

        let version = Version::try_from(config.version).expect("valid version");

        let mmap = MmapBuffer::new(mmap_path, megabytes(1))?;
        let (pusher, popper) = ring_buffer(mmap, version).map_err(phylactery::Error::RingBuffer)?;

        let graveyard_path = format!("{}/graveyard.bin", config.path);
        let graveyard_path = PathBuf::from(graveyard_path);

        let graveyard = Graveyard::new(graveyard_path, popper);

        std::thread::spawn(move || {
            graveyard.bury(10);
        });

        let meta_path = format!("{}/meta.bin", config.path);
        let data_path = format!("{}/kvdata/", config.path);
        let tl_path = format!("{}/tl.bin", config.path);

        let mmap = MmapBuffer::new(tl_path, megabytes(1))?;

        let (transaction_log_tx, transaction_log_rx) =
            ring_buffer(mmap, version).map_err(phylactery::Error::RingBuffer)?;

        Ok(Self {
            // meta
            root: config.path.clone(),
            version,

            // patches
            pending: LinkedHashMap::new(),
            transaction_log_rx,
            transaction_log_tx,

            // data
            kvs: KVStore::new(
                meta_path,
                config.meta_size,
                data_path,
                config.node_size,
                version,
                pusher,
            )
            .map_err(phylactery::Error::KVStore)?,
            dequeues: BTreeMap::new(),
        })
    }

    pub(crate) fn add_to_pending(&mut self, patch: ClientRequest) {
        self.pending
            .insert(patch.id(), CommitStatus::Pending(patch));
    }

    pub(crate) fn commit_pending(&mut self, id: u128, buf: &mut Vec<u8>) -> Vec<Packet> {
        if let Some(commit) = self.pending.get_mut(&id) {
            commit.complete();
        }

        let mut packets = Vec::new();
        while let Some((_, commit)) = self.pending.front() {
            if let CommitStatus::Committed(patch) = commit {
                packets.push(self.commit_patch(patch.clone(), buf));
                self.pending.pop_front();
            } else {
                break;
            }
        }

        packets
    }

    pub(crate) fn commit_patch(&mut self, patch: ClientRequest, buf: &mut Vec<u8>) -> Packet {
        let mut tl_patch = vec![];

        // TODO: make `push_encode`
        patch.encode(&mut tl_patch).expect("must be able to encode");

        if let Err(err) = self.transaction_log_tx.push(tl_patch) {
            warn!("failed to push to transaction log: {}", err);
            return patch.nack(FAILED_TO_PUSH_TO_TRANSACTION_LOG);
        }

        match patch {
            // dequeue
            ClientRequest::CreateQueue(queue) => {
                let path = queue.path();
                let ack = if self.dequeues.contains_key(&path.to_string()) {
                    warn!("queue({}) already exists", path);
                    queue.nack(QUEUE_ALREADY_EXISTS)
                } else {
                    match Dequeue::new(
                        format!("{}/{}", self.root, path),
                        queue.node_size(),
                        self.version,
                    ) {
                        Ok(dequeue) => {
                            self.dequeues.insert(path.to_owned(), dequeue);
                            trace!("created queue {:?}", path);
                            queue.ack()
                        }
                        Err(_) => todo!(),
                    }
                };

                Packet::CreateQueueAck(ack)
            }

            ClientRequest::DeleteQueue(queue) => {
                let path = queue.path();
                let ack = if let Some(delete_queue) = self.dequeues.remove(path) {
                    remove_dir(format!("{}/{}", self.root, path)).expect("must be able to remove");
                    trace!("deleted queue {:?}", path);
                    queue.ack()
                } else {
                    warn!("queue({}) does not exist", path);
                    queue.nack(QUEUE_DOES_NOT_EXIST)
                };

                Packet::DeleteQueueAck(ack)
            }

            ClientRequest::Enqueue(enqueue) => {
                let path = enqueue.path();
                let ack = if let Some(queue) = self.dequeues.get_mut(path) {
                    let push = queue.push(enqueue.value()).expect("queue full");
                    trace!("pushed to queue({}): {:?}", path, push);
                    enqueue.ack()
                } else {
                    warn!("queue({}) does not exist", path);
                    enqueue.nack(QUEUE_DOES_NOT_EXIST)
                };

                Packet::EnqueueAck(ack)
            }

            ClientRequest::Dequeue(dequeue) => {
                let path = dequeue.path();
                let ack = if let Some(queue) = self.dequeues.get_mut(path) {
                    let pop = queue.pop(buf).expect("queue empty");
                    trace!("popped from queue({}): {:?}", path, pop);
                    dequeue.ack(buf.to_vec())
                } else {
                    warn!("queue({}) does not exist", path);
                    dequeue.nack(QUEUE_DOES_NOT_EXIST)
                };

                Packet::DequeueAck(ack)
            }

            // kv store
            ClientRequest::Put(put) => match self.kvs.insert(put.key().clone(), put.value()) {
                Ok(_) => Packet::PutAck(put.ack()),
                Err(err) => {
                    warn!("failed to insert into kv store: {}", err);
                    Packet::PutAck(put.nack(INTERNAL_ERROR))
                }
            },
            ClientRequest::Delete(delete) => match self.kvs.delete(delete.key()) {
                Ok(_) => Packet::DeleteAck(delete.ack()),
                Err(err) => {
                    warn!("failed to delete from kv store: {}", err);
                    Packet::DeleteAck(delete.nack(INTERNAL_ERROR))
                }
            },
            ClientRequest::Get(get) => match self.kvs.get(get.key(), buf) {
                Ok(lookup) => match lookup {
                    Lookup::Absent => Packet::GetAck(get.nack(KEY_DOES_NOT_EXIST)),
                    Lookup::Found(found) => Packet::GetAck(get.ack(found.into_inner())),
                },
                Err(err) => {
                    warn!("failed to get from kv store: {}", err);
                    Packet::GetAck(get.nack(INTERNAL_ERROR))
                }
            },

            // other
            _ => panic!("invalid packet type {patch:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cmp::min,
        sync::atomic::{AtomicU64, Ordering},
    };

    use tempfile::tempdir;

    use necronomicon::{
        kv_store_codec::{Delete, Get, Key, Put},
        Header, Kind, Packet,
    };

    use crate::{config::Config, reqres::ClientRequest};

    use super::Store;

    static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn test_commit_pending() {
        let dir = tempdir().expect("temp file");
        let path = dir.path().to_str().unwrap().to_string();

        let config = Config::test_new(path);
        let mut store = Store::new(&config).expect("store");

        let put = Put::new(
            generate_header(Kind::Put),
            generate_key("key"),
            "kittens".as_bytes().to_vec(),
        );

        let get = Get::new(generate_header(Kind::Get), generate_key("key"));

        let delete = Delete::new(generate_header(Kind::Delete), generate_key("key"));

        let patches = [
            ClientRequest::Put(put.clone()),
            ClientRequest::Get(get.clone()),
            ClientRequest::Delete(delete.clone()),
        ];

        for patch in patches.iter() {
            store.add_to_pending(patch.clone());
        }

        let mut results = vec![];
        // Get acks in reverse to show we still get the correct packets
        for id in patches.iter().rev().map(|patch| patch.id()) {
            let mut buf = vec![];
            let packets = store.commit_pending(id, &mut buf);
            results.extend(packets);
        }

        assert_eq!(results.len(), 3);

        assert_eq!(results[0], Packet::PutAck(put.ack()));
        assert_eq!(
            results[1],
            Packet::GetAck(get.ack("kittens".as_bytes().to_vec()))
        );
        assert_eq!(results[2], Packet::DeleteAck(delete.ack()));
    }

    fn generate_header(kind: Kind) -> Header {
        let id = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Header::new(kind, 1, id as u128)
    }

    fn generate_key(key: &str) -> Key {
        let mut slice = [0; 32];
        let len = min(key.len(), slice.len());
        slice[..len].copy_from_slice(&key.as_bytes()[..len]);
        Key::new(slice)
    }
}
