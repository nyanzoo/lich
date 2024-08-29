use error::Error;
use log::trace;

use necronomicon::{full_decode, system_codec::Join, Owned, Packet, Pool, SystemPacket};
use net::session::SessionReader;

pub mod error;
pub mod incoming;
pub mod outgoing;

#[derive(Clone, Copy, Debug)]
enum BufferOwner {
    FullDecode,
}

impl necronomicon::BufferOwner for BufferOwner {
    fn why(&self) -> &'static str {
        match self {
            BufferOwner::FullDecode => "full packet decode",
        }
    }
}

pub fn wait_for_join<P>(
    reader: &mut SessionReader,
    pool: &P,
) -> Result<Join<<P::Buffer as Owned>::Shared>, Error>
where
    P: Pool,
{
    let mut previous_decoded_header = None;

    loop {
        let mut buffer = pool.acquire(BufferOwner::FullDecode);

        'decode: loop {
            // We should know whether we have enough buffer to read the packet or not by checking the header.
            // BUT, that also means we need to keep the header around and not just discard it. That way we can read
            // the rest of the packet... so we need to change the `full_decode` to take an optional header.
            match full_decode(reader, &mut buffer, previous_decoded_header.take()) {
                Ok(Packet::System(SystemPacket::Join(join))) => {
                    reader.update_last_seen();
                    trace!("got {:?} join", join);

                    return Ok(join);
                }
                Ok(_) => {
                    trace!("ignoring non-join packet");
                }
                Err(necronomicon::Error::BufferTooSmallForPacketDecode { header, .. }) => {
                    trace!("buffer too small for packet decode");
                    let _ = previous_decoded_header.insert(header);
                    break 'decode;
                }
                Err(err) => {
                    trace!("closing session due to err: {err}");
                    return Err(err.into());
                }
            }
        }
    }
}

pub fn decode_packet_on_reader_and<P, F>(reader: &mut SessionReader, pool: &P, mut service: F)
where
    P: Pool,
    F: FnMut(Packet<<P::Buffer as Owned>::Shared>) -> bool,
{
    let mut previous_decoded_header = None;

    'pool: loop {
        let mut buffer = pool.acquire(BufferOwner::FullDecode);

        'decode: loop {
            // We should know whether we have enough buffer to read the packet or not by checking the header.
            // BUT, that also means we need to keep the header around and not just discard it. That way we can read
            // the rest of the packet... so we need to change the `full_decode` to take an optional header.
            match full_decode(reader, &mut buffer, previous_decoded_header.take()) {
                Ok(packet) => {
                    reader.update_last_seen();
                    trace!("got {:?} packet", packet);

                    if !service(packet) {
                        trace!("service returned false");
                        break 'pool;
                    }
                }
                Err(necronomicon::Error::BufferTooSmallForPacketDecode { header, .. }) => {
                    trace!("buffer too small for packet decode");
                    let _ = previous_decoded_header.insert(header);
                    break 'decode;
                }
                Err(err) => {
                    trace!("closing session due to err: {err}");
                    break 'pool;
                }
            }
        }
    }
}
