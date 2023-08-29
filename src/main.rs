use std::sync::mpsc;

use incoming::Incoming;

use phylactery::{buffer, entry::Version, ring_buffer::ring_buffer};

// mod acks;
mod config;
mod error;
mod incoming;
mod outgoing;
mod reqres;
mod session;
mod state;
mod store;
mod util;

pub const LICH_DIR: &str = "./lich/";

// INCOMING:
// Incoming will need to have tx+rx for requests and acks.
// Incoming will read in a request and send it over to state for processing.
//
// STATE:
// If state is tail then it will commit the change to the store, perform any actions and send back ack to incoming
// If state is head/mid then it will need to store the change in pending and send to next node via Outgoing. It will need to get an
// ack back from next node and then commit that change and send ack back to Incoming.
// If state is candidate then it will need to read out transaction log and send to next node via Outgoing.
//
// OUTGOING:
// Outgoing will need to have tx+rx for requests and acks. It sends out operator msgs to operator. The rest to next node.
// Need to read out responses and send acks back to STATE.

fn main() {
    pretty_env_logger::init();

    // let (pusher, popper) =
    //     ring_buffer(buffer::InMemBuffer::new(util::gigabytes(1)), Version::V1).expect("dequeue");

    // let (requests_tx, requests_rx) = mpsc::channel();
    // Incoming::new(9999, requests_tx).run();

    // let state = state::State::new(store, requests_rx);

    // loop {}
    todo!()
}
