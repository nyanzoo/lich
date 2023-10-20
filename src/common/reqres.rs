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
    Ack, Decode, Encode, Header, Kind, Packet, PartialDecode,
};

#[derive(Clone, Debug)]
pub enum System {
    Join(Join),
    JoinAck(JoinAck),

    Ping(Ping),
    PingAck(PingAck),

    Report(Report),
    ReportAck(ReportAck),

    Transfer(Transfer),
    TransferAck(TransferAck),
}

impl From<Packet> for System {
    fn from(value: Packet) -> Self {
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
pub enum ClientRequest {
    // dequeue
    CreateQueue(Create),
    DeleteQueue(dequeue_codec::Delete),
    Enqueue(Enqueue),
    Dequeue(Dequeue),
    Peek(Peek),
    Len(Len),

    // kv store
    Put(Put),
    Get(Get),
    Delete(kv_store_codec::Delete),

    // transfer
    Transfer(Transfer),
}

impl Into<Packet> for ClientRequest {
    fn into(self) -> Packet {
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
            ClientRequest::Transfer(packet) => Packet::Transfer(packet),
        }
    }
}

impl ClientRequest {
    pub fn id(&self) -> u128 {
        match self {
            // dequeue
            ClientRequest::CreateQueue(packet) => packet.header().uuid(),
            ClientRequest::DeleteQueue(packet) => packet.header().uuid(),
            ClientRequest::Enqueue(packet) => packet.header().uuid(),
            ClientRequest::Dequeue(packet) => packet.header().uuid(),
            ClientRequest::Peek(packet) => packet.header().uuid(),
            ClientRequest::Len(packet) => packet.header().uuid(),

            // kv store
            ClientRequest::Put(packet) => packet.header().uuid(),
            ClientRequest::Get(packet) => packet.header().uuid(),
            ClientRequest::Delete(packet) => packet.header().uuid(),

            // transfer
            ClientRequest::Transfer(packet) => packet.header().uuid(),
        }
    }

    pub fn nack(self, response_code: u8) -> Packet {
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
            ClientRequest::Transfer(packet) => Packet::TransferAck(packet.nack(response_code)),
        }
    }
}

impl<W> Encode<W> for ClientRequest
where
    W: Write,
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
            ClientRequest::Transfer(packet) => packet.encode(writer),
        }
    }
}

impl<W> Encode<W> for System
where
    W: Write,
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

impl<R> Decode<R> for System
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let header = Header::decode(reader)?;

        match header.kind() {
            Kind::Join => Ok(System::Join(Join::decode(header, reader)?)),
            Kind::JoinAck => Ok(System::JoinAck(JoinAck::decode(header, reader)?)),

            Kind::Ping => Ok(System::Ping(Ping::decode(header, reader)?)),
            Kind::PingAck => Ok(System::PingAck(PingAck::decode(header, reader)?)),

            Kind::Report => Ok(System::Report(Report::decode(header, reader)?)),
            Kind::ReportAck => Ok(System::ReportAck(ReportAck::decode(header, reader)?)),

            Kind::Transfer => Ok(System::Transfer(Transfer::decode(header, reader)?)),
            Kind::TransferAck => Ok(System::TransferAck(TransferAck::decode(header, reader)?)),

            _ => panic!("invalid packet type {header:?}"),
        }
    }
}

#[derive(Debug)]
pub enum ClientResponse {
    // dequeue
    CreateQueue(CreateAck),
    DeleteQueue(dequeue_codec::DeleteAck),
    Enqueue(EnqueueAck),
    Dequeue(DequeueAck),
    Peek(PeekAck),
    Len(LenAck),

    // kv store
    Put(PutAck),
    Get(GetAck),
    Delete(kv_store_codec::DeleteAck),

    // transfer
    Transfer(TransferAck),
}

impl Ack for ClientResponse {
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
            ClientResponse::Transfer(ack) => ack.response_code(),
        }
    }
}

impl<W> Encode<W> for ClientResponse
where
    W: Write,
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
            ClientResponse::Transfer(ack) => ack.encode(writer),
        }
    }
}

#[derive(Debug)]
pub struct ProcessRequest {
    pub request: ClientRequest,
    response_tx: Sender<ClientResponse>,
}

impl ProcessRequest {
    pub fn new(request: ClientRequest, response_tx: Sender<ClientResponse>) -> Self {
        Self {
            request,
            response_tx,
        }
    }

    pub fn into_parts(self) -> (ClientRequest, PendingRequest) {
        (
            self.request,
            PendingRequest {
                response_tx: self.response_tx,
            },
        )
    }
}

pub struct PendingRequest {
    response_tx: Sender<ClientResponse>,
}

impl PendingRequest {
    pub fn complete(self, response: ClientResponse) {
        trace!("sending response: {:?}", response);
        self.response_tx.send(response).expect("send");
    }
}

impl From<Packet> for ClientRequest {
    fn from(value: Packet) -> Self {
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

impl From<Packet> for ClientResponse {
    fn from(value: Packet) -> Self {
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
