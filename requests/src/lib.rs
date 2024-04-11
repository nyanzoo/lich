use std::io::{Read, Write};

use crossbeam::channel::Sender;
use log::trace;

use necronomicon::{
    dequeue_codec::{
        self, Create, CreateAck, Dequeue, DequeueAck, Enqueue, EnqueueAck, Len, LenAck, Peek,
        PeekAck,
    },
    kv_store_codec::{self, Get, GetAck, Put, PutAck},
    system_codec::{Join, JoinAck, Ping, PingAck, Report, ReportAck, Transfer, TransferAck},
    Ack, Decode, DecodeOwned, Encode, Header, Kind, Owned, Packet, PartialDecode, Shared, Uuid,
};

#[derive(Clone, Debug)]
pub enum System<S>
where
    S: Shared,
{
    Join(Join<S>),
    JoinAck(JoinAck),

    Ping(Ping),
    PingAck(PingAck),

    Report(Report<S>),
    ReportAck(ReportAck),

    Transfer(Transfer<S>),
    TransferAck(TransferAck),
}

impl<S> From<Packet<S>> for System<S>
where
    S: Shared,
{
    fn from(value: Packet<S>) -> Self {
        match value {
            Packet::Join(join) => System::Join(join),
            Packet::JoinAck(join_ack) => System::JoinAck(join_ack),

            Packet::Ping(ping) => System::Ping(ping),
            Packet::PingAck(ping_ack) => System::PingAck(ping_ack),

            Packet::Report(report) => System::Report(report),
            Packet::ReportAck(report_ack) => System::ReportAck(report_ack),

            Packet::Transfer(transfer) => System::Transfer(transfer),
            Packet::TransferAck(transfer_ack) => System::TransferAck(transfer_ack),

            _ => panic!("invalid packet type {value:?}"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ClientRequest<S>
where
    S: Shared,
{
    // dequeue
    CreateQueue(Create<S>),
    DeleteQueue(dequeue_codec::Delete<S>),
    Enqueue(Enqueue<S>),
    Dequeue(Dequeue<S>),
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

impl<S> Into<Packet<S>> for ClientRequest<S>
where
    S: Shared,
{
    fn into(self) -> Packet<S> {
        match self {
            // dequeue
            ClientRequest::CreateQueue(packet) => Packet::CreateQueue(packet),
            ClientRequest::DeleteQueue(packet) => Packet::DeleteQueue(packet),
            ClientRequest::Enqueue(packet) => Packet::Enqueue(packet),
            ClientRequest::Dequeue(packet) => Packet::Dequeue(packet),
            ClientRequest::Peek(packet) => Packet::Peek(packet),
            ClientRequest::Len(packet) => Packet::Len(packet),

            // kv store
            ClientRequest::Put(packet) => Packet::Put(packet),
            ClientRequest::Get(packet) => Packet::Get(packet),
            ClientRequest::Delete(packet) => Packet::Delete(packet),

            // transfer
            #[cfg(feature = "backend")]
            ClientRequest::Transfer(packet) => Packet::Transfer(packet),
        }
    }
}

impl<S> ClientRequest<S>
where
    S: Shared,
{
    pub fn id(&self) -> Uuid {
        match self {
            // dequeue
            ClientRequest::CreateQueue(packet) => packet.header().uuid,
            ClientRequest::DeleteQueue(packet) => packet.header().uuid,
            ClientRequest::Enqueue(packet) => packet.header().uuid,
            ClientRequest::Dequeue(packet) => packet.header().uuid,
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
            // dequeue
            ClientRequest::CreateQueue(_) => true,
            ClientRequest::DeleteQueue(_) => true,
            ClientRequest::Enqueue(_) => true,
            ClientRequest::Dequeue(_) => true,
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
            // dequeue
            ClientRequest::CreateQueue(_) => false,
            ClientRequest::DeleteQueue(_) => false,
            ClientRequest::Enqueue(_) => false,
            ClientRequest::Dequeue(_) => false,
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

    pub fn nack(self, response_code: u8) -> Packet<S> {
        match self {
            // dequeue
            ClientRequest::CreateQueue(packet) => {
                Packet::CreateQueueAck(packet.nack(response_code))
            }
            ClientRequest::DeleteQueue(packet) => {
                Packet::DeleteQueueAck(packet.nack(response_code))
            }
            ClientRequest::Enqueue(packet) => Packet::EnqueueAck(packet.nack(response_code)),
            ClientRequest::Dequeue(packet) => Packet::DequeueAck(packet.nack(response_code)),
            ClientRequest::Peek(packet) => Packet::PeekAck(packet.nack(response_code)),
            ClientRequest::Len(packet) => Packet::LenAck(packet.nack(response_code)),

            // kv store
            ClientRequest::Put(packet) => Packet::PutAck(packet.nack(response_code)),
            ClientRequest::Get(packet) => Packet::GetAck(packet.nack(response_code)),
            ClientRequest::Delete(packet) => Packet::DeleteAck(packet.nack(response_code)),

            // transfer
            #[cfg(feature = "backend")]
            ClientRequest::Transfer(packet) => Packet::TransferAck(packet.nack(response_code)),
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
            // dequeue
            ClientRequest::CreateQueue(packet) => packet.encode(writer),
            ClientRequest::DeleteQueue(packet) => packet.encode(writer),
            ClientRequest::Enqueue(packet) => packet.encode(writer),
            ClientRequest::Dequeue(packet) => packet.encode(writer),
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
    // dequeue
    CreateQueue(CreateAck),
    DeleteQueue(dequeue_codec::DeleteAck),
    Enqueue(EnqueueAck),
    Dequeue(DequeueAck<S>),
    Peek(PeekAck<S>),
    Len(LenAck),

    // kv store
    Put(PutAck),
    Get(GetAck<S>),
    Delete(kv_store_codec::DeleteAck),

    // transfer
    #[cfg(feature = "backend")]
    Transfer(TransferAck),
}

impl<S> Ack for ClientResponse<S>
where
    S: Shared,
{
    fn header(&self) -> &Header {
        match self {
            // dequeue
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

    fn response_code(&self) -> u8 {
        match self {
            // dequeue
            ClientResponse::CreateQueue(ack) => ack.response_code(),
            ClientResponse::DeleteQueue(ack) => ack.response_code(),
            ClientResponse::Enqueue(ack) => ack.response_code(),
            ClientResponse::Dequeue(ack) => ack.response_code(),
            ClientResponse::Peek(ack) => ack.response_code(),
            ClientResponse::Len(ack) => ack.response_code(),

            // kv store
            ClientResponse::Put(ack) => ack.response_code(),
            ClientResponse::Get(ack) => ack.response_code(),
            ClientResponse::Delete(ack) => ack.response_code(),

            // system
            #[cfg(feature = "backend")]
            ClientResponse::Transfer(ack) => ack.response_code(),
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
            // dequeue
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
        self.response_tx.send(response).expect("send");
    }
}

impl<S> From<Packet<S>> for ClientRequest<S>
where
    S: Shared,
{
    fn from(value: Packet<S>) -> Self {
        match value {
            // dequeue
            Packet::Enqueue(enqueue) => ClientRequest::Enqueue(enqueue),
            Packet::Dequeue(dequeue) => ClientRequest::Dequeue(dequeue),
            Packet::Peek(peek) => ClientRequest::Peek(peek),
            Packet::Len(len) => ClientRequest::Len(len),
            Packet::CreateQueue(create) => ClientRequest::CreateQueue(create),
            Packet::DeleteQueue(delete) => ClientRequest::DeleteQueue(delete),

            // kv store
            Packet::Put(put) => ClientRequest::Put(put),
            Packet::Get(get) => ClientRequest::Get(get),
            Packet::Delete(delete) => ClientRequest::Delete(delete),

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
            // dequeue
            Packet::EnqueueAck(ack) => ClientResponse::Enqueue(ack),
            Packet::DequeueAck(ack) => ClientResponse::Dequeue(ack),
            Packet::PeekAck(ack) => ClientResponse::Peek(ack),
            Packet::LenAck(ack) => ClientResponse::Len(ack),
            Packet::CreateQueueAck(ack) => ClientResponse::CreateQueue(ack),
            Packet::DeleteQueueAck(ack) => ClientResponse::DeleteQueue(ack),

            // kv store
            Packet::PutAck(ack) => ClientResponse::Put(ack),
            Packet::GetAck(ack) => ClientResponse::Get(ack),
            Packet::DeleteAck(ack) => ClientResponse::Delete(ack),

            _ => panic!("invalid packet type {value:?}"),
        }
    }
}
