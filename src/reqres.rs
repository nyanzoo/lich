use std::{io::Write, sync::mpsc};

use necronomicon::{
    dequeue_codec::{
        self, Create, CreateAck, Dequeue, DequeueAck, Enqueue, EnqueueAck, Len, LenAck, Peek,
        PeekAck,
    },
    kv_store_codec::{self, Get, GetAck, Put, PutAck},
    system_codec::{Chain, ChainAck, Join, JoinAck, Transfer, TransferAck},
    Encode, Packet,
};

#[derive(Clone, Debug)]
pub(crate) enum OperatorRequest {
    Chain(Chain),
    Join(Join),
    Transfer(Transfer),
}

#[derive(Clone, Debug)]
pub(crate) enum ClientRequest {
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
}

impl Into<Packet> for ClientRequest {
    fn into(self) -> Packet {
        match self {
            ClientRequest::CreateQueue(packet) => Packet::CreateQueue(packet),
            ClientRequest::DeleteQueue(packet) => Packet::DeleteQueue(packet),
            ClientRequest::Enqueue(packet) => Packet::Enqueue(packet),
            ClientRequest::Dequeue(packet) => Packet::Dequeue(packet),
            ClientRequest::Peek(packet) => Packet::Peek(packet),
            ClientRequest::Len(packet) => Packet::Len(packet),
            ClientRequest::Put(packet) => Packet::Put(packet),
            ClientRequest::Get(packet) => Packet::Get(packet),
            ClientRequest::Delete(packet) => Packet::Delete(packet),
        }
    }
}

impl ClientRequest {
    pub fn id(&self) -> u128 {
        match self {
            ClientRequest::CreateQueue(packet) => packet.header().uuid(),
            ClientRequest::DeleteQueue(packet) => packet.header().uuid(),
            ClientRequest::Enqueue(packet) => packet.header().uuid(),
            ClientRequest::Dequeue(packet) => packet.header().uuid(),
            ClientRequest::Peek(packet) => packet.header().uuid(),
            ClientRequest::Len(packet) => packet.header().uuid(),
            ClientRequest::Put(packet) => packet.header().uuid(),
            ClientRequest::Get(packet) => packet.header().uuid(),
            ClientRequest::Delete(packet) => packet.header().uuid(),
        }
    }

    pub fn nack(self, response_code: u8) -> Packet {
        match self {
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
            ClientRequest::Put(packet) => Packet::PutAck(packet.nack(response_code)),
            ClientRequest::Get(packet) => Packet::GetAck(packet.nack(response_code)),
            ClientRequest::Delete(packet) => Packet::DeleteAck(packet.nack(response_code)),
        }
    }
}

impl<W> Encode<W> for ClientRequest
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            ClientRequest::CreateQueue(packet) => packet.encode(writer),
            ClientRequest::DeleteQueue(packet) => packet.encode(writer),
            ClientRequest::Enqueue(packet) => packet.encode(writer),
            ClientRequest::Dequeue(packet) => packet.encode(writer),
            ClientRequest::Peek(packet) => packet.encode(writer),
            ClientRequest::Len(packet) => packet.encode(writer),
            ClientRequest::Put(packet) => packet.encode(writer),
            ClientRequest::Get(packet) => packet.encode(writer),
            ClientRequest::Delete(packet) => packet.encode(writer),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Request {
    Operator(OperatorRequest),
    Client(ClientRequest),
}

#[derive(Debug)]
pub(crate) enum ChainResponse {
    Chain(ChainAck),
    Join(JoinAck),
    Transfer(TransferAck),
}

impl<W> Encode<W> for ChainResponse
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            ChainResponse::Chain(ack) => ack.encode(writer),
            ChainResponse::Join(ack) => ack.encode(writer),
            ChainResponse::Transfer(ack) => ack.encode(writer),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ClientResponse {
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
}

impl<W> Encode<W> for ClientResponse
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            ClientResponse::CreateQueue(ack) => ack.encode(writer),
            ClientResponse::DeleteQueue(ack) => ack.encode(writer),
            ClientResponse::Enqueue(ack) => ack.encode(writer),
            ClientResponse::Dequeue(ack) => ack.encode(writer),
            ClientResponse::Peek(ack) => ack.encode(writer),
            ClientResponse::Len(ack) => ack.encode(writer),
            ClientResponse::Put(ack) => ack.encode(writer),
            ClientResponse::Get(ack) => ack.encode(writer),
            ClientResponse::Delete(ack) => ack.encode(writer),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Response {
    Chain(ChainResponse),
    Client(ClientResponse),
}

impl<W> Encode<W> for Response
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            Response::Chain(ack) => ack.encode(writer),
            Response::Client(ack) => ack.encode(writer),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ProcessRequest {
    pub request: Request,
    response_tx: mpsc::Sender<Response>,
}

impl ProcessRequest {
    pub(crate) fn new(request: Request, response_tx: mpsc::Sender<Response>) -> Self {
        Self {
            request,
            response_tx,
        }
    }

    pub(crate) fn complete(self, response: Response) {
        self.response_tx
            .send(response)
            .expect("must be able to send response");
    }
}

impl From<Packet> for Request {
    fn from(value: Packet) -> Self {
        match value {
            // dequeue
            Packet::Enqueue(enqueue) => Request::Client(ClientRequest::Enqueue(enqueue)),
            Packet::Dequeue(dequeue) => Request::Client(ClientRequest::Dequeue(dequeue)),
            Packet::Peek(peek) => Request::Client(ClientRequest::Peek(peek)),
            Packet::Len(len) => Request::Client(ClientRequest::Len(len)),
            Packet::CreateQueue(create) => Request::Client(ClientRequest::CreateQueue(create)),
            Packet::DeleteQueue(delete) => Request::Client(ClientRequest::DeleteQueue(delete)),

            // kv store
            Packet::Put(put) => Request::Client(ClientRequest::Put(put)),
            Packet::Get(get) => Request::Client(ClientRequest::Get(get)),
            Packet::Delete(delete) => Request::Client(ClientRequest::Delete(delete)),

            // system
            Packet::Chain(chain) => Request::Operator(OperatorRequest::Chain(chain)),
            Packet::Join(join) => Request::Operator(OperatorRequest::Join(join)),
            Packet::Transfer(transfer) => Request::Operator(OperatorRequest::Transfer(transfer)),

            _ => panic!("invalid packet type {value:?}"),
        }
    }
}

impl From<Packet> for Response {
    fn from(value: Packet) -> Self {
        match value {
            // dequeue
            Packet::EnqueueAck(ack) => Response::Client(ClientResponse::Enqueue(ack)),
            Packet::DequeueAck(ack) => Response::Client(ClientResponse::Dequeue(ack)),
            Packet::PeekAck(ack) => Response::Client(ClientResponse::Peek(ack)),
            Packet::LenAck(ack) => Response::Client(ClientResponse::Len(ack)),
            Packet::CreateQueueAck(ack) => Response::Client(ClientResponse::CreateQueue(ack)),
            Packet::DeleteQueueAck(ack) => Response::Client(ClientResponse::DeleteQueue(ack)),

            // kv store
            Packet::PutAck(ack) => Response::Client(ClientResponse::Put(ack)),
            Packet::GetAck(ack) => Response::Client(ClientResponse::Get(ack)),
            Packet::DeleteAck(ack) => Response::Client(ClientResponse::Delete(ack)),

            // system
            Packet::ChainAck(ack) => Response::Chain(ChainResponse::Chain(ack)),
            Packet::JoinAck(ack) => Response::Chain(ChainResponse::Join(ack)),
            Packet::TransferAck(ack) => Response::Chain(ChainResponse::Transfer(ack)),

            _ => panic!("invalid packet type {value:?}"),
        }
    }
}
