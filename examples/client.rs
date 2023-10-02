use std::{io::Write, println};

use clap::Parser;
use necronomicon::{full_decode, kv_store_codec, Encode, Header, Kind, Packet};

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
    let packet: Packet = command.into();
    println!("packet: {:?}", packet);

    packet.encode(&mut stream).unwrap();
    stream.flush().unwrap();
    println!("sent packet: {:?}", packet);

    let response = full_decode(&mut stream).unwrap();
    println!("response: {:?}", response);
}

impl Into<necronomicon::Packet> for Command {
    fn into(self) -> necronomicon::Packet {
        let uuid = uuid::Uuid::new_v4().as_u128();
        match self {
            Command::Put { key, value } => Packet::Put(kv_store_codec::Put::new(
                Header::new(Kind::Put, 1, uuid),
                key.try_into().unwrap(),
                value.as_bytes().to_vec(),
            )),
            Command::Get { key } => Packet::Get(kv_store_codec::Get::new(
                Header::new(Kind::Get, 1, uuid),
                key.try_into().unwrap(),
            )),
            Command::Delete { key } => Packet::Delete(kv_store_codec::Delete::new(
                Header::new(Kind::Delete, 1, uuid),
                key.try_into().unwrap(),
            )),
        }
    }
}

fn main() {
    let cli = Cli::parse();

    send_command(cli.host, cli.port, cli.command);
}
