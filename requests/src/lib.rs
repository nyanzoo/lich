use std::io::{Read, Write};

use crossbeam::channel::Sender;
use log::{trace, warn};

use necronomicon::{
    deque_codec::{
        self, Create, CreateAck, Dequeue, DequeueAck, Enqueue, EnqueueAck, Len, LenAck, Peek,
        PeekAck,
    },
    kv_store_codec::{self, Get, GetAck, Put, PutAck},
    system_codec::{Join, JoinAck, Ping, PingAck, Report, ReportAck, Transfer, TransferAck},
    Ack, ByteStr, Decode, DecodeOwned, DequePacket, Encode, Header, Kind, Owned, Packet,
    PartialDecode, Response, Shared, SharedImpl, StorePacket, SystemPacket, Uuid,
};
use phylactery::store::Request;

#[derive(Clone, Debug)]
pub enum System<S>
where
    S: Shared,
{
    Join(Join<S>),
    JoinAck(JoinAck<S>),

    Ping(Ping<S>),
    PingAck(PingAck<S>),

    Report(Report<S>),
    ReportAck(ReportAck<S>),

    Transfer(Transfer<S>),
    TransferAck(TransferAck<S>),
}

impl<S> From<SystemPacket<S>> for System<S>
where
    S: Shared,
{
    fn from(value: SystemPacket<S>) -> Self {
        match value {
            SystemPacket::Join(packet) => System::Join(packet),
            SystemPacket::JoinAck(packet) => System::JoinAck(packet),
            SystemPacket::Ping(packet) => System::Ping(packet),
            SystemPacket::PingAck(packet) => System::PingAck(packet),
            SystemPacket::Report(packet) => System::Report(packet),
            SystemPacket::ReportAck(packet) => System::ReportAck(packet),
            SystemPacket::Transfer(packet) => System::Transfer(packet),
            SystemPacket::TransferAck(packet) => System::TransferAck(packet),
        }
    }
}

impl<S> From<Packet<S>> for System<S>
where
    S: Shared,
{
    fn from(value: Packet<S>) -> Self {
        match value {
            Packet::System(system) => Self::from(system),
            _ => panic!("invalid packet type {value:?}"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ClientRequest<S>
where
    S: Shared,
{
    // deque
    CreateQueue(Create<S>),
    DeleteQueue(deque_codec::Delete<S>),
    Enqueue(Enqueue<S>),
    Deque(Dequeue<S>),
    Peek(Peek<S>),
    Len(Len<S>),

    // kv store
    Put(Put<S>),
    Get(Get<S>),
    Delete(kv_store_codec::Delete<S>),

    // transfer
    #[cfg(feature = "backend")]
    Transfer(Transfer<S>),
}

impl From<ClientRequest<SharedImpl>> for Request {
    fn from(value: ClientRequest<SharedImpl>) -> Self {
        match value {
            // deque
            ClientRequest::CreateQueue(packet) => Request::Create(packet),
            ClientRequest::DeleteQueue(packet) => Request::Remove(packet),
            ClientRequest::Enqueue(packet) => Request::Push(packet),
            ClientRequest::Deque(packet) => Request::Pop(packet),
            ClientRequest::Peek(packet) => Request::Peek(packet),
            // ClientRequest::Len(packet) => Request::Deque(DequeRequest::Len(packet)),

            // kv store
            ClientRequest::Put(packet) => Request::Put(packet),
            ClientRequest::Get(packet) => Request::Get(packet),
            ClientRequest::Delete(packet) => Request::Delete(packet),

            _ => panic!("invalid packet type {value:?}"),
        }
    }
}

impl<S> Into<Packet<S>> for ClientRequest<S>
where
    S: Shared,
{
    fn into(self) -> Packet<S> {
        match self {
            // deque
            ClientRequest::CreateQueue(packet) => Packet::Deque(DequePacket::CreateQueue(packet)),
            ClientRequest::DeleteQueue(packet) => Packet::Deque(DequePacket::DeleteQueue(packet)),
            ClientRequest::Enqueue(packet) => Packet::Deque(DequePacket::Enqueue(packet)),
            ClientRequest::Deque(packet) => Packet::Deque(DequePacket::Dequeue(packet)),
            ClientRequest::Peek(packet) => Packet::Deque(DequePacket::Peek(packet)),
            ClientRequest::Len(packet) => Packet::Deque(DequePacket::Len(packet)),

            // kv store
            ClientRequest::Put(packet) => Packet::Store(StorePacket::Put(packet)),
            ClientRequest::Get(packet) => Packet::Store(StorePacket::Get(packet)),
            ClientRequest::Delete(packet) => Packet::Store(StorePacket::Delete(packet)),

            // transfer
            #[cfg(feature = "backend")]
            ClientRequest::Transfer(packet) => Packet::System(SystemPacket::Transfer(packet)),
        }
    }
}

impl<S> ClientRequest<S>
where
    S: Shared,
{
    pub fn id(&self) -> Uuid {
        match self {
            // deque
            ClientRequest::CreateQueue(packet) => packet.header().uuid,
            ClientRequest::DeleteQueue(packet) => packet.header().uuid,
            ClientRequest::Enqueue(packet) => packet.header().uuid,
            ClientRequest::Deque(packet) => packet.header().uuid,
            ClientRequest::Peek(packet) => packet.header().uuid,
            ClientRequest::Len(packet) => packet.header().uuid,

            // kv store
            ClientRequest::Put(packet) => packet.header().uuid,
            ClientRequest::Get(packet) => packet.header().uuid,
            ClientRequest::Delete(packet) => packet.header().uuid,

            // transfer
            #[cfg(feature = "backend")]
            ClientRequest::Transfer(packet) => packet.header().uuid,
        }
    }

    pub fn is_for_head(&self) -> bool {
        match self {
            // deque
            ClientRequest::CreateQueue(_) => true,
            ClientRequest::DeleteQueue(_) => true,
            ClientRequest::Enqueue(_) => true,
            ClientRequest::Deque(_) => true,
            ClientRequest::Peek(_) => false,
            ClientRequest::Len(_) => false,

            // kv store
            ClientRequest::Put(_) => true,
            ClientRequest::Get(_) => false,
            ClientRequest::Delete(_) => true,

            // transfer
            #[cfg(feature = "backend")]
            ClientRequest::Transfer(_) => false,
        }
    }

    pub fn is_for_tail(&self) -> bool {
        match self {
            // deque
            ClientRequest::CreateQueue(_) => false,
            ClientRequest::DeleteQueue(_) => false,
            ClientRequest::Enqueue(_) => false,
            ClientRequest::Deque(_) => false,
            ClientRequest::Peek(_) => true,
            ClientRequest::Len(_) => true,

            // kv store
            ClientRequest::Put(_) => false,
            ClientRequest::Get(_) => true,
            ClientRequest::Delete(_) => false,

            // transfer
            #[cfg(feature = "backend")]
            ClientRequest::Transfer(_) => false,
        }
    }

    pub fn nack(self, response_code: u8, reason: Option<ByteStr<S>>) -> Packet<S> {
        match self {
            // deque
            ClientRequest::CreateQueue(packet) => {
                DequePacket::CreateQueueAck(packet.nack(response_code, reason)).into()
            }
            ClientRequest::DeleteQueue(packet) => {
                DequePacket::DeleteQueueAck(packet.nack(response_code, reason)).into()
            }
            ClientRequest::Enqueue(packet) => {
                DequePacket::EnqueueAck(packet.nack(response_code, reason)).into()
            }
            ClientRequest::Deque(packet) => {
                DequePacket::DequeueAck(packet.nack(response_code, reason)).into()
            }
            ClientRequest::Peek(packet) => {
                DequePacket::PeekAck(packet.nack(response_code, reason)).into()
            }
            ClientRequest::Len(packet) => {
                DequePacket::LenAck(packet.nack(response_code, reason)).into()
            }

            // kv store
            ClientRequest::Put(packet) => {
                StorePacket::PutAck(packet.nack(response_code, reason)).into()
            }
            ClientRequest::Get(packet) => {
                StorePacket::GetAck(packet.nack(response_code, reason)).into()
            }
            ClientRequest::Delete(packet) => {
                StorePacket::DeleteAck(packet.nack(response_code, reason)).into()
            }

            // transfer
            #[cfg(feature = "backend")]
            ClientRequest::Transfer(packet) => {
                SystemPacket::TransferAck(packet.nack(response_code, reason)).into()
            }
        }
    }
}

impl<W, S> Encode<W> for ClientRequest<S>
where
    W: Write,
    S: Shared,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            // deque
            ClientRequest::CreateQueue(packet) => packet.encode(writer),
            ClientRequest::DeleteQueue(packet) => packet.encode(writer),
            ClientRequest::Enqueue(packet) => packet.encode(writer),
            ClientRequest::Deque(packet) => packet.encode(writer),
            ClientRequest::Peek(packet) => packet.encode(writer),
            ClientRequest::Len(packet) => packet.encode(writer),

            // kv store
            ClientRequest::Put(packet) => packet.encode(writer),
            ClientRequest::Get(packet) => packet.encode(writer),
            ClientRequest::Delete(packet) => packet.encode(writer),

            // system
            #[cfg(feature = "backend")]
            ClientRequest::Transfer(packet) => packet.encode(writer),
        }
    }
}

impl<W, S> Encode<W> for System<S>
where
    W: Write,
    S: Shared,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            System::Join(ack) => ack.encode(writer),
            System::JoinAck(ack) => ack.encode(writer),

            System::Ping(ack) => ack.encode(writer),
            System::PingAck(ack) => ack.encode(writer),

            System::Report(ack) => ack.encode(writer),
            System::ReportAck(ack) => ack.encode(writer),

            System::Transfer(ack) => ack.encode(writer),
            System::TransferAck(ack) => ack.encode(writer),
        }
    }
}

impl<R, O> DecodeOwned<R, O> for System<O::Shared>
where
    R: Read,
    O: Owned,
{
    fn decode_owned(reader: &mut R, buffer: &mut O) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let header = Header::decode(reader)?;

        match header.kind {
            Kind::Join => Ok(System::Join(Join::decode(header, reader, buffer)?)),
            Kind::JoinAck => Ok(System::JoinAck(JoinAck::decode(header, reader, buffer)?)),

            Kind::Ping => Ok(System::Ping(Ping::decode(header, reader, buffer)?)),
            Kind::PingAck => Ok(System::PingAck(PingAck::decode(header, reader, buffer)?)),

            Kind::Report => Ok(System::Report(Report::decode(header, reader, buffer)?)),
            Kind::ReportAck => Ok(System::ReportAck(ReportAck::decode(
                header, reader, buffer,
            )?)),

            Kind::Transfer => Ok(System::Transfer(Transfer::decode(header, reader, buffer)?)),
            Kind::TransferAck => Ok(System::TransferAck(TransferAck::decode(
                header, reader, buffer,
            )?)),

            _ => panic!("invalid packet type {header:?}"),
        }
    }
}

#[derive(Debug)]
pub enum ClientResponse<S>
where
    S: Shared,
{
    // deque
    CreateQueue(CreateAck<S>),
    DeleteQueue(deque_codec::DeleteAck<S>),
    Enqueue(EnqueueAck<S>),
    Dequeue(DequeueAck<S>),
    Peek(PeekAck<S>),
    Len(LenAck<S>),

    // kv store
    Put(PutAck<S>),
    Get(GetAck<S>),
    Delete(kv_store_codec::DeleteAck<S>),

    // transfer
    #[cfg(feature = "backend")]
    Transfer(TransferAck<S>),
}

impl<S> Ack<S> for ClientResponse<S>
where
    S: Shared,
{
    fn header(&self) -> &Header {
        match self {
            // deque
            ClientResponse::CreateQueue(ack) => ack.header(),
            ClientResponse::DeleteQueue(ack) => ack.header(),
            ClientResponse::Enqueue(ack) => ack.header(),
            ClientResponse::Dequeue(ack) => ack.header(),
            ClientResponse::Peek(ack) => ack.header(),
            ClientResponse::Len(ack) => ack.header(),

            // kv store
            ClientResponse::Put(ack) => ack.header(),
            ClientResponse::Get(ack) => ack.header(),
            ClientResponse::Delete(ack) => ack.header(),

            // system
            #[cfg(feature = "backend")]
            ClientResponse::Transfer(ack) => ack.header(),
        }
    }

    fn response(&self) -> Response<S> {
        match self {
            // deque
            ClientResponse::CreateQueue(ack) => ack.response(),
            ClientResponse::DeleteQueue(ack) => ack.response(),
            ClientResponse::Enqueue(ack) => ack.response(),
            ClientResponse::Dequeue(ack) => ack.response(),
            ClientResponse::Peek(ack) => ack.response(),
            ClientResponse::Len(ack) => ack.response(),

            // kv store
            ClientResponse::Put(ack) => ack.response(),
            ClientResponse::Get(ack) => ack.response(),
            ClientResponse::Delete(ack) => ack.response(),

            // system
            #[cfg(feature = "backend")]
            ClientResponse::Transfer(ack) => ack.response(),
        }
    }
}

impl<W, S> Encode<W> for ClientResponse<S>
where
    W: Write,
    S: Shared,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            // deque
            ClientResponse::CreateQueue(ack) => ack.encode(writer),
            ClientResponse::DeleteQueue(ack) => ack.encode(writer),
            ClientResponse::Enqueue(ack) => ack.encode(writer),
            ClientResponse::Dequeue(ack) => ack.encode(writer),
            ClientResponse::Peek(ack) => ack.encode(writer),
            ClientResponse::Len(ack) => ack.encode(writer),

            // kv store
            ClientResponse::Put(ack) => ack.encode(writer),
            ClientResponse::Get(ack) => ack.encode(writer),
            ClientResponse::Delete(ack) => ack.encode(writer),

            // system
            #[cfg(feature = "backend")]
            ClientResponse::Transfer(ack) => ack.encode(writer),
        }
    }
}

#[derive(Debug)]
pub struct ProcessRequest<S>
where
    S: Shared,
{
    pub request: ClientRequest<S>,
    response_tx: Sender<ClientResponse<S>>,
}

impl<S> ProcessRequest<S>
where
    S: Shared,
{
    pub fn new(request: ClientRequest<S>, response_tx: Sender<ClientResponse<S>>) -> Self {
        Self {
            request,
            response_tx,
        }
    }

    pub fn into_parts(self) -> (ClientRequest<S>, PendingRequest<S>) {
        (
            self.request,
            PendingRequest {
                response_tx: self.response_tx,
            },
        )
    }
}

pub struct PendingRequest<S>
where
    S: Shared,
{
    response_tx: Sender<ClientResponse<S>>,
}

impl<S> PendingRequest<S>
where
    S: Shared,
{
    pub fn complete(self, response: ClientResponse<S>) {
        trace!("sending response: {:?}", response);
        if let Err(err) = self.response_tx.send(response) {
            warn!("failed to send response due to: {:?}", err);
        }
    }
}

impl<S> From<Packet<S>> for ClientRequest<S>
where
    S: Shared,
{
    fn from(value: Packet<S>) -> Self {
        match value {
            Packet::Deque(deque) => match deque {
                DequePacket::Enqueue(enqueue) => ClientRequest::Enqueue(enqueue),
                DequePacket::Dequeue(deque) => ClientRequest::Deque(deque),
                DequePacket::Peek(peek) => ClientRequest::Peek(peek),
                DequePacket::Len(len) => ClientRequest::Len(len),
                DequePacket::CreateQueue(create) => ClientRequest::CreateQueue(create),
                DequePacket::DeleteQueue(delete) => ClientRequest::DeleteQueue(delete),
                _ => panic!("invalid packet type {deque:?}"),
            },

            Packet::Store(store) => match store {
                StorePacket::Put(put) => ClientRequest::Put(put),
                StorePacket::Get(get) => ClientRequest::Get(get),
                StorePacket::Delete(delete) => ClientRequest::Delete(delete),
                _ => panic!("invalid packet type {store:?}"),
            },
            _ => panic!("invalid packet type {value:?}"),
        }
    }
}

impl<S> From<Packet<S>> for ClientResponse<S>
where
    S: Shared,
{
    fn from(value: Packet<S>) -> Self {
        match value {
            Packet::Deque(deque) => match deque {
                DequePacket::EnqueueAck(ack) => ClientResponse::Enqueue(ack),
                DequePacket::DequeueAck(ack) => ClientResponse::Dequeue(ack),
                DequePacket::PeekAck(ack) => ClientResponse::Peek(ack),
                DequePacket::LenAck(ack) => ClientResponse::Len(ack),
                DequePacket::CreateQueueAck(ack) => ClientResponse::CreateQueue(ack),
                DequePacket::DeleteQueueAck(ack) => ClientResponse::DeleteQueue(ack),
                _ => panic!("invalid packet type {deque:?}"),
            },
            Packet::Store(store) => match store {
                StorePacket::PutAck(ack) => ClientResponse::Put(ack),
                StorePacket::GetAck(ack) => ClientResponse::Get(ack),
                StorePacket::DeleteAck(ack) => ClientResponse::Delete(ack),
                _ => panic!("invalid packet type {store:?}"),
            },

            _ => panic!("invalid packet type {value:?}"),
        }
    }
}
