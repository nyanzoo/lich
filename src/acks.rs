use std::collections::{VecDeque, HashMap};

pub(super) struct Acks {
    order: VecDeque<u128>,
    lookup: HashMap<u128, ()>,
}

impl Acks {
    pub(super) fn new() -> Self {
        Self {
            order: VecDeque::new(),
            lookup: HashMap::new(),
        }
    }

    pub(super) fn push(&mut self, ack: ()) {
        self.order.push_back(ack);
        self.lookup.insert(ack, ());
    }

    pub(super) fn pop(&mut self) -> Option<()> {
        self.order.pop_front().map(|ack| {
            self.lookup.remove(&ack);
            ack
        })
    }

    pub(super) fn contains(&self, ack: &()) -> bool {
        self.lookup.contains_key(ack)
    }
}