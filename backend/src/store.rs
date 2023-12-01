use std::{
    collections::BTreeMap,
    fs::{create_dir_all, read, remove_dir},
    io::Cursor,
    path::PathBuf,
};

use hashlink::LinkedHashMap;
use log::{trace, warn};

use config::StoreConfig;
use necronomicon::{
    kv_store_codec::Key, system_codec::Transfer, Decode, Encode, Packet,
    FAILED_TO_PUSH_TO_TRANSACTION_LOG, INTERNAL_ERROR, KEY_DOES_NOT_EXIST, QUEUE_ALREADY_EXISTS,
    QUEUE_DOES_NOT_EXIST,
};
use phylactery::{
    buffer::MmapBuffer,
    dequeue::Dequeue,
    entry::Version,
    kv_store::{Graveyard, KVStore, Lookup},
    ring_buffer::{self, ring_buffer},
};
use requests::ClientRequest;
use util::megabytes;

use crate::error::Error;

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
    api_version: Version,
    store_version: u128,
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
    pub fn new(config: &StoreConfig) -> Result<Self, Error> {
        let dir_all_res = create_dir_all(&config.path);
        trace!("creating store at {}, res {:?}", config.path, dir_all_res);

        let mmap_path = format!("{}/mmap.bin", config.path);

        let api_version = Version::try_from(config.version).expect("valid version");

        trace!("creating mmap buffer at {}", mmap_path);
        let mmap = MmapBuffer::new(mmap_path, megabytes(1)).expect("mmap buffer");
        trace!("create push and pop for graveyard");
        let (pusher, popper) = ring_buffer(mmap, api_version)?;

        let graveyard_path = format!("{}/graveyard", config.path);
        let graveyard_path = PathBuf::from(graveyard_path);
        trace!("creating graveyard at {graveyard_path:?}");
        _ = create_dir_all(&graveyard_path);

        let graveyard = Graveyard::new(graveyard_path, popper);

        std::thread::spawn(move || {
            graveyard.bury(10);
        });

        let meta_path = format!("{}/meta.bin", config.path);
        let data_path = format!("{}/kvdata", config.path);
        let tl_path = format!("{}/tl.bin", config.path);
        trace!("creating transaction log at {}", tl_path);
        let mmap = MmapBuffer::new(tl_path, megabytes(1))?;

        let (transaction_log_tx, transaction_log_rx) = ring_buffer(mmap, api_version)?;

        trace!("creating kv store at {} & {}", meta_path, data_path);
        let mut kvs = KVStore::new(
            meta_path,
            config.meta_size,
            data_path,
            config.node_size,
            api_version,
            pusher,
        )?;

        // NOTE: we might want to consider not storing the version this way.
        // As it prohibits the user from using this key.
        let mut buf = vec![];
        let store_version = kvs
            .get(&Key::try_from("version").expect("store version"), &mut buf)
            .expect("get version");

        let store_version = match store_version {
            Lookup::Absent => 0,
            Lookup::Found(data) => {
                let mut buf = Cursor::new(data.into_inner());
                u128::decode(&mut buf).expect("decode version")
            }
        };

        Ok(Self {
            // meta
            root: config.path.clone(),
            api_version,
            store_version,

            // patches
            pending: LinkedHashMap::new(),
            transaction_log_rx,
            transaction_log_tx,

            // data
            kvs,
            dequeues: BTreeMap::new(),
        })
    }

    pub(crate) fn version(&self) -> u128 {
        self.store_version
    }

    fn update_version(&mut self) -> Result<(), Error> {
        self.store_version += 1;
        let mut buf = vec![];
        self.store_version.encode(&mut buf).expect("encode version");
        self.kvs
            .insert(Key::try_from("version").expect("store version"), &buf)?;
        Ok(())
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
        while let Some(commit) = self.pending.front().map(|(_, v)| v.clone()) {
            if let CommitStatus::Committed(patch) = commit {
                packets.push(self.commit_patch(patch, buf));
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

        while let Err(err) = self.transaction_log_tx.push(&tl_patch) {
            if let phylactery::Error::EntryTooBig { .. } = err {
                let mut buf = vec![];
                self.transaction_log_rx
                    .pop(&mut buf)
                    .expect("must be able to pop");
            } else {
                warn!("failed to push to transaction log: {}", err);
                return patch.nack(FAILED_TO_PUSH_TO_TRANSACTION_LOG);
            }
        }

        let res = match patch {
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
                        self.api_version,
                    ) {
                        Ok(dequeue) => {
                            self.dequeues.insert(path.to_owned(), dequeue);
                            trace!("created queue {:?}", path);

                            if let Err(err) = self.update_version() {
                                warn!("failed to update version: {err:?}");
                                queue.nack(INTERNAL_ERROR)
                            } else {
                                queue.ack()
                            }
                        }
                        Err(_) => todo!(),
                    }
                };

                Packet::CreateQueueAck(ack)
            }

            ClientRequest::DeleteQueue(queue) => {
                let path = queue.path();
                let ack = if self.dequeues.remove(path).is_some() {
                    remove_dir(format!("{}/{}", self.root, path)).expect("must be able to remove");
                    trace!("deleted queue {:?}", path);

                    if let Err(err) = self.update_version() {
                        warn!("failed to update version: {err:?}");
                        queue.nack(INTERNAL_ERROR)
                    } else {
                        queue.ack()
                    }
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

                    if let Err(err) = self.update_version() {
                        warn!("failed to update version: {err:?}");
                        enqueue.nack(INTERNAL_ERROR)
                    } else {
                        enqueue.ack()
                    }
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

                    if let Err(err) = self.update_version() {
                        warn!("failed to update version: {err:?}");
                        dequeue.nack(INTERNAL_ERROR)
                    } else {
                        dequeue.ack(buf.to_vec())
                    }
                } else {
                    warn!("queue({}) does not exist", path);
                    dequeue.nack(QUEUE_DOES_NOT_EXIST)
                };

                Packet::DequeueAck(ack)
            }

            // kv store
            ClientRequest::Put(put) => match self.kvs.insert(*put.key(), put.value()) {
                Ok(_) => {
                    self.store_version += 1;
                    let ack = if let Err(err) = self.update_version() {
                        warn!("failed to update version: {err:?}");
                        put.nack(INTERNAL_ERROR)
                    } else {
                        put.ack()
                    };

                    Packet::PutAck(ack)
                }
                Err(err) => {
                    warn!("failed to insert into kv store: {}", err);
                    Packet::PutAck(put.nack(INTERNAL_ERROR))
                }
            },
            ClientRequest::Delete(delete) => match self.kvs.delete(delete.key()) {
                Ok(_) => {
                    self.store_version += 1;
                    let ack = if let Err(err) = self.update_version() {
                        warn!("failed to update version: {err:?}");
                        delete.nack(INTERNAL_ERROR)
                    } else {
                        delete.ack()
                    };

                    Packet::DeleteAck(ack)
                }
                Err(err) => {
                    warn!("failed to delete from kv store: {}", err);
                    Packet::DeleteAck(delete.nack(INTERNAL_ERROR))
                }
            },
            ClientRequest::Get(get) => {
                // NOTE: no need to update version as store did not change.
                let ack = match self.kvs.get(get.key(), buf) {
                    Ok(lookup) => match lookup {
                        Lookup::Absent => get.nack(KEY_DOES_NOT_EXIST),
                        Lookup::Found(found) => get.ack(found.into_inner()),
                    },
                    Err(err) => {
                        warn!("failed to get from kv store: {}", err);
                        get.nack(INTERNAL_ERROR)
                    }
                };

                Packet::GetAck(ack)
            }

            // other
            _ => panic!("invalid packet type {patch:?}"),
        };

        res
    }

    pub(crate) fn deconstruct_iter(&self) -> impl Iterator<Item = Transfer> + '_ {
        self.kvs.deconstruct_iter().map(|path| {
            let content = read(&path).expect("must be able to read file");
            let uuid = uuid::Uuid::new_v4().as_u128();
            Transfer::new((self.api_version.into(), uuid), path, content)
        })
    }

    pub(crate) fn reconstruct(&self, transfer: &Transfer) {
        self.kvs.reconstruct(transfer.path(), transfer.content())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cmp::min,
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
    };

    use tempfile::tempdir;

    use config::StoreConfig;
    use necronomicon::{
        kv_store_codec::{Delete, Get, Key, Put},
        Packet,
    };
    use requests::ClientRequest;

    use super::Store;

    static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn test_commit_pending() {
        let dir = tempdir().expect("temp file");
        let path = dir.path().to_str().unwrap().to_string();

        let config = StoreConfig {
            path: path.clone(),
            meta_size: 1024,
            node_size: 1024,
            version: 1,
        };
        let mut store = Store::new(&config).expect("store");

        let put = Put::new(
            generate_header(),
            generate_key("key"),
            "kittens".as_bytes().to_vec(),
        );

        let get = Get::new(generate_header(), generate_key("key"));

        let delete = Delete::new(generate_header(), generate_key("key"));

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

        hexyl(&Path::new(&format!("{}/meta.bin", path)));
    }

    fn generate_header() -> (u8, u128) {
        let id = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        (1, id as u128)
    }

    fn generate_key(key: &str) -> Key {
        let mut slice = [0; 32];
        let len = min(key.len(), slice.len());
        slice[..len].copy_from_slice(&key.as_bytes()[..len]);
        Key::new(slice)
    }

    #[allow(dead_code)]
    fn hexyl(path: &std::path::Path) {
        use std::io::Write;
        std::io::stdout()
            .write_all(
                &std::process::Command::new("hexyl")
                    .arg(path)
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap();
    }
}
