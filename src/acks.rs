use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use parking_lot::Mutex;

use necronomicon::{Ack, Header};

use crate::store::Store;

pub(super) enum EventStatus {
    Pending,
    Complete,
}

pub(super) struct Event {
    inner: Arc<Mutex<EventInner>>,
}

struct EventInner {
    status: EventStatus,
    store: Store<String>,
}

impl EventInner {
    pub(super) fn new(store: Store<String>) -> Self {
        Self {
            status: EventStatus::Pending,
            store,
        }
    }

    pub(super) fn ack(&mut self, header: Header, response_code: u8) {
        self.store.ack(header, response_code);
        self.status = EventStatus::Complete;
    }
}

impl Event {
    pub(super) fn new(store: Arc<Mutex<Store<String>>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(EventInner::new(store))),
        }
    }

    pub(super) fn ack(&self, header: Header, response_code: u8) {
        let mut inner = self.inner.lock();
        inner.ack(header, response_code);
    }
}

// Do we even need this? Maybe what we can do is if not tail
// we just hold onto the patches in pending and then we have
// pending commit from the acks that we can go through
// and commit the patches?
pub(super) struct Acks {
    order: VecDeque<u128>,
    incomplete: VecDeque<u128>,
    lookup: HashMap<u128, Event>,
}

impl Acks {
    pub(super) fn new() -> Self {
        Self {
            order: VecDeque::new(),
            lookup: HashMap::new(),
        }
    }

    pub(super) fn ack(&self, ack: impl Ack) {
        let Some(event) = self.lookup.get(&ack.header().uuid())  else {
            panic!("ack not found")
        };
        (*event)(*ack.header(), ack.response_code());
    }

    pub(super) fn push(&mut self, id: u128, event: Box<dyn FnOnce(Header, u8)>) {
        self.order.push_back(id);
        self.lookup.insert(id, event);
    }

    pub(super) fn pop(&mut self) -> Option<Event> {
        self.order
            .pop_front()
            .map(|ack| self.lookup.remove(&ack))
            .flatten()
    }

    pub(super) fn contains(&self, ack: &u128) -> bool {
        self.lookup.contains_key(ack)
    }
}
