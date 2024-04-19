use std::{io::Write, println};

use clap::Parser;
use necronomicon::{
    full_decode, kv_store_codec, BinaryData, ByteStr, Encode, Packet, Pool as _, PoolImpl,
    SharedImpl,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short = 'H', long, required = true)]
    host: String,

    #[arg(short, long, required = true)]
    port: u16,

    #[command(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
enum Command {
    Put {
        #[arg(short, long)]
        key: String,

        #[arg(short, long)]
        value: String,
    },

    Get {
        #[arg(short, long)]
        key: String,
    },

    Delete {
        #[arg(short, long)]
        key: String,
    },
}

fn send_command(host: String, port: u16, command: Command) {
    let mut stream = std::net::TcpStream::connect(format!("{host}:{port}")).unwrap();
    let pool = PoolImpl::new(1024, 1024);
    let packet: Packet<_> = command.into_packet(&pool);
    println!("packet: {:?}", packet);

    packet.encode(&mut stream).unwrap();
    stream.flush().unwrap();
    println!("sent packet: {:?}", packet);

    let mut owned = pool.acquire().unwrap();
    let response = full_decode(&mut stream, &mut owned, None).unwrap();
    println!("response: {:?}", response);
}

impl Command {
    fn into_packet(self, pool: &PoolImpl) -> necronomicon::Packet<SharedImpl> {
        let uuid = uuid::Uuid::new_v4().as_u128();
        match self {
            Command::Put { key, value } => {
                let mut owned = pool.acquire().expect("pool.acquire");
                let key = ByteStr::from_owned(key, &mut owned).expect("key");
                let mut owned = pool.acquire().expect("pool.acquire");
                let value = BinaryData::from_owned(value, &mut owned).expect("value");

                Packet::Put(kv_store_codec::Put::new(
                    1,
                    uuid,
                    key.inner().clone(),
                    value,
                ))
            }
            Command::Get { key } => {
                let mut owned = pool.acquire().expect("pool.acquire");
                let key = ByteStr::from_owned(key, &mut owned).expect("key");

                Packet::Get(kv_store_codec::Get::new(1, uuid, key.inner().clone()))
            }
            Command::Delete { key } => {
                let mut owned = pool.acquire().expect("pool.acquire");
                let key = ByteStr::from_owned(key, &mut owned).expect("key");

                Packet::Delete(kv_store_codec::Delete::new(1, uuid, key.inner().clone()))
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();

    send_command(cli.host, cli.port, cli.command);
}
