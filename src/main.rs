use env_logger::Env;

#[macro_use]
extern crate capnp_rpc;

pub mod pubsub_capnp {
    include!(concat!(env!("OUT_DIR"), "/src/pubsub_capnp.rs"));
}

pub mod client;
pub mod server;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();

    let args: Vec<String> = ::std::env::args().collect();
    if args.len() >= 2 {
        match &args[1][..] {
            "client" => return client::main().await,
            "server" => return server::main().await,
            _ => panic!("unknown"),
        }
    }

    println!("usage: {} [client | server] ADDRESS", args[0]);
    Ok(())
}
