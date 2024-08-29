use std::{
    collections::BTreeMap,
    fs::{remove_dir, File},
    io::{Cursor, Read, Seek, SeekFrom},
    mem::size_of,
    sync::mpsc::{Receiver, Sender},
};

use hashlink::LinkedHashMap;
use log::{trace, warn};

use necronomicon::{
    system_codec::Transfer, BinaryData, ByteStr, Decode, Encode, Owned, OwnedImpl, Packet,
    Pool as _, PoolImpl, Shared, SharedImpl, Uuid, INTERNAL_ERROR, KEY_DOES_NOT_EXIST,
    QUEUE_ALREADY_EXISTS, QUEUE_DOES_NOT_EXIST, QUEUE_EMPTY,
};
use phylactery::{
    deque::{self, Deque},
    entry::Version,
    store::{config::Config, Lookup, Request, Response, Store as KVStore},
};
use requests::ClientRequest;
use util::megabytes;

use crate::{error::Error, BufferOwner};

#[derive(Clone, Debug)]
enum CommitStatus {
    Committed(ClientRequest<SharedImpl>),
    Pending(ClientRequest<SharedImpl>),
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

pub struct Store {
    kvs: KVStore,
    api_version: Version,
    store_version: u128,
    pending: hashlink::LinkedHashMap<necronomicon::Uuid, CommitStatus>,
}

impl Store {
    /// # Description
    /// Create a new store. There should be only one store per node.
    /// Users are allowed to have a single KV Store and multiple Deques.
    ///
    /// Additionally, this will spawn a thread for the graveyard (GC of disk fragmentation).
    ///
    /// # Arguments
    /// * `config`: The configuration for the store.
    ///
    /// # Errors
    /// See `error::Error` for details.
    pub fn new(
        mut configs: Vec<Config>,
        requests: Receiver<Request>,
        responses: Sender<Response>,
        pool: PoolImpl,
    ) -> Result<Self, Error> {
        let configs = configs
            .drain(..)
            .map(|config| {
                #[cfg(test)]
                let hostname = "test";
                #[cfg(not(test))]
                let hostname = std::env::var("HOSTNAME").expect("hostname");
                let path = format!("{}/{}/{:?}", config.path, hostname, config.version);
                config.path = path;
                config
            })
            .collect::<Vec<_>>();
        let kvs =
            KVStore::new(configs, requests, responses, pool).map_err(phylactery::Error::Store)?;

        // NOTE: we might want to consider not storing the version this way.
        // As it prohibits the user from using this key.

        let mut buffer = pool.acquire(BufferOwner::StoreVersion);
        let store_version = kvs
            .get(
                &Self::version_key(&mut buffer).expect("store version"),
                &mut buffer,
            )
            .expect("get version");

        let store_version = match store_version {
            Lookup::Absent => 0,
            Lookup::Found(data) => {
                data.verify().expect("verify version");
                let inner = data.into_inner();
                let data = inner.data().as_slice();
                u128::decode(&mut Cursor::new(data)).expect("decode version")
            }
        };

        Ok(Self {
            // meta
            api_version: config.version,
            store_version,

            // patches
            pending: LinkedHashMap::new(),

            // data
            kvs,
        })
    }

    pub(crate) fn version(&self) -> u128 {
        self.store_version
    }

    fn update_version(&mut self, buffer: &mut OwnedImpl) -> Result<(), Error> {
        self.store_version += 1;
        let version = Self::version_key(buffer)?;
        let store_version = self.version_value();
        self.kvs.insert(version, &store_version)?;
        Ok(())
    }

    pub(crate) fn add_to_pending(&mut self, patch: ClientRequest<SharedImpl>) {
        self.pending
            .insert(patch.id(), CommitStatus::Pending(patch));
    }

    pub(crate) fn commit_pending(
        &mut self,
        id: Uuid,
        buf: &mut OwnedImpl,
    ) -> Vec<Packet<SharedImpl>> {
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

    pub(crate) fn commit_patch(
        &mut self,
        patch: ClientRequest<SharedImpl>,
        buffer: &mut OwnedImpl,
    ) -> Packet<SharedImpl> {
        let mut tl_patch = vec![];

        trace!("committing patch: {patch:?}");
        // TODO: make `push_encode`
        patch.encode(&mut tl_patch).expect("must be able to encode");

        let res = match patch {
            // deque
            ClientRequest::CreateQueue(queue) => {
                let path = queue.path();
                let ack = if self.deques.contains_key(&path) {
                    warn!("queue({:?}) already exists", path);
                    queue.nack(QUEUE_ALREADY_EXISTS)
                } else {
                    match Deque::new(
                        format!("{}/{:?}", self.root, path),
                        queue.node_size(),
                        queue.max_disk_usage(),
                        self.api_version,
                    ) {
                        Ok(deque) => {
                            self.deques.insert(path.to_owned(), deque);
                            trace!("created queue {:?}", path);

                            if let Err(err) = self.update_version(buffer) {
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
                let ack = if self.deques.remove(path).is_some() {
                    remove_dir(format!("{}/{:?}", self.root, path))
                        .expect("must be able to remove");
                    trace!("deleted queue {:?}", path);

                    if let Err(err) = self.update_version(buffer) {
                        warn!("failed to update version: {err:?}");
                        queue.nack(INTERNAL_ERROR)
                    } else {
                        queue.ack()
                    }
                } else {
                    warn!("queue({:?}) does not exist", path);
                    queue.nack(QUEUE_DOES_NOT_EXIST)
                };

                Packet::DeleteQueueAck(ack)
            }

            ClientRequest::Enqueue(enqueue) => {
                let path = enqueue.path();
                let ack = if let Some(queue) = self.deques.get_mut(path) {
                    let push = queue
                        .push(enqueue.value().data().as_slice())
                        .expect("queue full");
                    trace!("pushed to queue({:?}): {:?}", path, push);

                    if let Err(err) = self.update_version(buffer) {
                        warn!("failed to update version: {err:?}");
                        enqueue.nack(INTERNAL_ERROR)
                    } else {
                        enqueue.ack()
                    }
                } else {
                    warn!("queue({:?}) does not exist", path);
                    enqueue.nack(QUEUE_DOES_NOT_EXIST)
                };

                Packet::EnqueueAck(ack)
            }

            ClientRequest::Deque(deque) => {
                let path = deque.path();
                let ack = if let Some(queue) = self.deques.get_mut(path) {
                    let pop = queue.pop(buffer).expect("queue empty");
                    trace!("popped from queue({:?}): {:?}", path, pop);

                    if let Err(err) = self.update_version(buffer) {
                        warn!("failed to update version: {err:?}");
                        deque.nack(INTERNAL_ERROR)
                    } else {
                        match pop {
                            deque::Pop::Popped(data) => deque.ack(data.into_inner()),
                            deque::Pop::WaitForFlush => deque.nack(QUEUE_EMPTY, None),
                        }
                    }
                } else {
                    warn!("queue({:?}) does not exist", path);
                    deque.nack(QUEUE_DOES_NOT_EXIST, None)
                };

                Packet::DequeueAck(ack)
            }

            // kv store
            ClientRequest::Put(put) => {
                match self
                    .kvs
                    .insert(put.key().clone(), put.value().data().as_slice())
                {
                    Ok(_) => {
                        self.store_version += 1;
                        let ack = if let Err(err) = self.update_version(buffer) {
                            warn!("failed to update version: {err:?}");
                            put.nack(INTERNAL_ERROR, None)
                        } else {
                            put.ack()
                        };

                        Packet::PutAck(ack)
                    }
                    Err(err) => {
                        warn!("failed to insert into kv store: {}", err);
                        Packet::PutAck(put.nack(INTERNAL_ERROR, None))
                    }
                }
            }
            ClientRequest::Delete(delete) => match self.kvs.delete(delete.key()) {
                Ok(_) => {
                    self.store_version += 1;
                    let ack = if let Err(err) = self.update_version(buffer) {
                        warn!("failed to update version: {err:?}");
                        delete.nack(INTERNAL_ERROR, None)
                    } else {
                        delete.ack()
                    };

                    Packet::DeleteAck(ack)
                }
                Err(err) => {
                    warn!("failed to delete from kv store: {}", err);
                    Packet::DeleteAck(delete.nack(INTERNAL_ERROR, None))
                }
            },
            ClientRequest::Get(get) => {
                // NOTE: no need to update version as store did not change.
                let ack = match self.kvs.get(get.key(), buffer) {
                    Ok(lookup) => match lookup {
                        Lookup::Absent => get.nack(KEY_DOES_NOT_EXIST, None),
                        Lookup::Found(found) => get.ack(found.into_inner()),
                    },
                    Err(err) => {
                        warn!("failed to get from kv store: {}", err);
                        get.nack(INTERNAL_ERROR, None)
                    }
                };

                Packet::GetAck(ack)
            }

            // other
            _ => panic!("invalid packet type {patch:?}"),
        };

        res
    }

    pub(crate) fn deconstruct_iter(
        &self,
        pool: PoolImpl,
    ) -> impl Iterator<Item = Transfer<SharedImpl>> + '_ {
        self.kvs
            .deconstruct_iter(u64::try_from(pool.block_size()).expect("usize -> u64"))
            .filter_map(move |(path, off)| {
                let mut file = File::open(&path).expect("file open");
                file.seek(SeekFrom::Start(off)).expect("seek file");

                // using two buffers like this is wasteful, but it is easy for now.
                let mut buffer = pool.acquire(BufferOwner::DeconstructPath);
                let path = ByteStr::from_owned(path, &mut buffer).expect("buffer");

                let mut buffer = pool.acquire(BufferOwner::DeconstructContent);

                let mut unfilled = buffer.unfilled();
                let bytes = file.read(&mut unfilled).expect("read file");
                if bytes == 0 {
                    return None;
                }

                buffer.fill(bytes);

                let shared = buffer.into_shared();
                let version: necronomicon::Version = necronomicon::Version::from(self.api_version);
                Some(Transfer::new(
                    version,
                    uuid::Uuid::new_v4().as_u128(),
                    path,
                    off,
                    BinaryData::new(shared),
                ))
            })
    }

    pub(crate) fn reconstruct(&self, transfer: &Transfer<SharedImpl>) {
        self.kvs.reconstruct(
            transfer.path().as_str().expect("valid transfer path"),
            transfer.content().data().as_slice(),
        )
    }

    fn version_key(buffer: &mut OwnedImpl) -> Result<BinaryData<SharedImpl>, Error> {
        trace!("creating version key");
        const VERSION: &str = "version";
        if buffer.unfilled_capacity() < VERSION.len() + size_of::<usize>() {
            Err(Error::Necronomicon(necronomicon::Error::OwnedRemaining {
                acquire: VERSION.len(),
                capacity: buffer.unfilled_capacity(),
            }))
        } else {
            let mut unfilled = buffer.unfilled();

            VERSION.len().encode(&mut unfilled)?;
            buffer.fill(size_of::<usize>());

            let version = VERSION.as_bytes();
            let mut unfilled = buffer.unfilled();
            version.encode(&mut unfilled)?;
            buffer.fill(version.len());

            let buffer = buffer.split_at(version.len() + size_of::<usize>());
            Ok(BinaryData::new(buffer.into_shared()))
        }
    }

    fn version_value(&self) -> Vec<u8> {
        let mut unfilled = vec![];
        self.store_version
            .encode(&mut unfilled)
            .expect("encode version");
        unfilled
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use phylactery::kv_store::config::{Config, Data, Metadata};
    use tempfile::tempdir;

    use necronomicon::{
        binary_data,
        kv_store_codec::{Delete, Get, Put},
        Packet, Pool, PoolImpl,
    };
    use requests::ClientRequest;

    use super::Store;

    #[test]
    fn test_commit_pending() {
        let dir = tempdir().expect("temp file");
        let path = dir.path().to_str().unwrap().to_string();

        let config = Config {
            path: path.clone(),
            meta: Metadata {
                max_disk_usage: 2048,
                max_key_size: 128,
            },
            data: Data {
                node_size: 1024,
                max_disk_usage: 1024 * 1024,
            },
            version: phylactery::entry::Version::V1,
        };
        let pool = PoolImpl::new(1024, 1024);
        let mut store = Store::new(config, pool.clone()).expect("store");

        let put = Put::new(1, 1, binary_data(b"key"), binary_data(b"kittens"));

        let get = Get::new(1, 2, binary_data(b"key"));

        let delete = Delete::new(1, 3, binary_data(b"key"));

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
            let mut buf = pool.acquire("commit");
            let packets = store.commit_pending(id, &mut buf);
            results.extend(packets);
        }

        assert_eq!(results.len(), 3);

        assert_eq!(results[0], Packet::PutAck(put.ack()));
        assert_eq!(results[1], Packet::GetAck(get.ack(binary_data(b"kittens"))));
        assert_eq!(results[2], Packet::DeleteAck(delete.ack()));

        hexyl(&Path::new(&format!("{}/meta.bin", path)));

        drop(store);
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
