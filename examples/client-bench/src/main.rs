use std::{collections::HashSet, io::Write, net::TcpStream, println, time::Instant};

use clap::Parser;
use necronomicon::{
    full_decode,
    kv_store_codec::{Get, Put},
    Ack, BinaryData, ByteStr, Encode, Packet, Pool, PoolImpl,
};
use rand::Rng;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short = 'H', long, required = true)]
    host: String,

    #[arg(short, long, required = true)]
    port: u16,

    #[command(subcommand)]
    scenario: Scenario,
}

#[derive(Parser, Debug)]
enum Scenario {
    PutOnly {
        #[clap(short, long, required = true)]
        key_set: Vec<String>,

        #[arg(short, long)]
        value_size: usize,

        #[arg(short, long)]
        count: usize,
    },

    PutAndGet {
        #[arg(short, long, required = true)]
        key_set: Vec<String>,

        #[arg(short, long)]
        value_size: usize,

        #[arg(short, long)]
        count: usize,

        #[arg(short, long)]
        distribution: f32,
    },
}

fn run(host: String, port: u16, scenario: Scenario) {
    let pool = PoolImpl::new(2048, 4096);
    let start = Instant::now();
    match scenario {
        Scenario::PutOnly {
            key_set,
            value_size,
            count,
        } => {
            let mut stream = TcpStream::connect(format!("{host}:{port}")).unwrap();
            let mut packet_tracker = HashSet::new();
            let time_to_send = Instant::now();
            for i in 0..count {
                let key = key_set[i % key_set.len()].clone();
                let mut owned = pool.acquire("bench").expect("pool.acquire");
                let key = ByteStr::from_owned(key, &mut owned).expect("key");
                // generate random value
                let value = generate_random_bytes(value_size);
                let mut owned = pool.acquire("bench").expect("pool.acquire");
                let value = BinaryData::from_owned(value, &mut owned).expect("value");
                let uuid = Uuid::new_v4().as_u128();
                packet_tracker.insert(necronomicon::Uuid::from(uuid));
                let packet = Put::new(1, uuid, key.inner().clone(), value);
                packet.encode(&mut stream).unwrap();
            }
            stream.flush().unwrap();
            println!("time to send: {:?}", time_to_send.elapsed());

            let time_to_receive = Instant::now();
            for _ in 0..count {
                let mut buffer = pool.acquire("bench").expect("pool.acquire");
                let response = full_decode(&mut stream, &mut buffer, None).unwrap();
                let Packet::PutAck(response) = response else {
                    panic!("received unexpected response: {:?}", response);
                };
                if !packet_tracker.remove(&response.header().uuid) {
                    panic!("received unexpected response: {:?}", response);
                }
            }
            println!("time to receive: {:?}", time_to_receive.elapsed());
        }
        Scenario::PutAndGet {
            key_set,
            value_size,
            count,
            distribution,
        } => {
            // use distribution to determine how many gets to do vs puts
            let mut stream = TcpStream::connect(format!("{host}:{port}")).unwrap();
            let mut packet_tracker = HashSet::new();
            let time_to_send = Instant::now();
            let mut rng = rand::thread_rng();
            for i in 0..count {
                let key = key_set[i % key_set.len()].clone();
                let mut owned = pool.acquire("bench").expect("pool.acquire");
                let key = ByteStr::from_owned(key, &mut owned).expect("key");
                // generate random value
                let uuid = Uuid::new_v4().as_u128();
                packet_tracker.insert(necronomicon::Uuid::from(uuid));
                let chance = rng.gen_range(0.0..=1.0);
                if distribution > chance {
                    let value = generate_random_bytes(value_size);
                    let mut owned = pool.acquire("bench").expect("pool.acquire");
                    let value = BinaryData::from_owned(value, &mut owned).expect("value");
                    let packet = Put::new(1, uuid, key.inner().clone(), value);
                    packet.encode(&mut stream).unwrap();
                } else {
                    let packet = Get::new(1, uuid, key.inner().clone());
                    packet.encode(&mut stream).unwrap();
                }
            }
            stream.flush().unwrap();
            println!("time to send: {:?}", time_to_send.elapsed());

            let time_to_receive = Instant::now();
            for _ in 0..count {
                let mut buffer = pool.acquire("bench").expect("pool.acquire");

                let response = full_decode(&mut stream, &mut buffer, None).unwrap();
                match response {
                    Packet::PutAck(response) => {
                        if !packet_tracker.remove(&response.header().uuid) {
                            panic!("received unexpected response: {:?}", response);
                        }
                    }
                    Packet::GetAck(response) => {
                        if !packet_tracker.remove(&response.header().uuid) {
                            panic!("received unexpected response: {:?}", response);
                        }
                    }
                    _ => {
                        panic!("received unexpected response: {:?}", response);
                    }
                }
            }
            println!("time to receive: {:?}", time_to_receive.elapsed());
        }
    }
    println!("elapsed: {:?}", start.elapsed());
}

fn generate_random_bytes(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen::<u8>()).collect()
}

fn main() {
    let cli = Cli::parse();

    run(cli.host, cli.port, cli.scenario);
}
