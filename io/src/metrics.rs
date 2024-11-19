#![allow(dead_code)]

use necronomicon::{Packet, Shared};
use opentelemetry::{
    metrics::{Counter, Meter},
    KeyValue, Value,
};

pub(crate) struct IncomingRequestCounter(Counter<u64>);

impl IncomingRequestCounter {
    pub(crate) fn new(meter: &Meter) -> Self {
        let counter = meter
            .u64_counter("incoming requests")
            .with_description("counts incoming requests")
            .with_unit("messages")
            .build();
        Self(counter)
    }

    pub(crate) fn add<S>(&self, requester: impl Into<Value>, kind: Packet<S>, value: u64)
    where
        S: Shared,
    {
        let kind = format!("{:?}", kind.header().kind);
        self.0.add(
            value,
            &[
                KeyValue::new("requester", requester.into()),
                KeyValue::new("kind", kind),
            ],
        );
    }
}

pub(crate) struct IncomingAckCounter(Counter<u64>);

impl IncomingAckCounter {
    pub(crate) fn new(meter: &Meter) -> Self {
        let counter = meter
            .u64_counter("incoming acks")
            .with_description("counts incoming acks to requests")
            .with_unit("messages")
            .build();
        Self(counter)
    }

    pub(crate) fn add<S>(&self, requester: impl Into<Value>, kind: Packet<S>, value: u64)
    where
        S: Shared,
    {
        let kind = format!("{:?}", kind.header().kind);
        self.0.add(
            value,
            &[
                KeyValue::new("requester", requester.into()),
                KeyValue::new("kind", kind),
            ],
        );
    }
}
