use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use log::{info, trace};
use std::{error::Error, net::SocketAddr};

use crate::pubsub_capnp::{publisher, subscriber};

struct SubscriberImpl {
    count: i32,
}

impl SubscriberImpl {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl subscriber::Server<::capnp::text::Owned> for SubscriberImpl {
    fn push_message(
        &mut self,
        params: subscriber::PushMessageParams<::capnp::text::Owned>,
        _: subscriber::PushMessageResults<::capnp::text::Owned>,
    ) -> Promise<(), capnp::Error> {
        self.count += 1;
        println!(
            "{}: message from publisher: {}",
            self.count,
            pry!(pry!(params.get()).get_message()),
        );
        Promise::ok(())
    }
}

pub async fn main() -> Result<(), Box<dyn Error>> {
    use std::net::ToSocketAddrs;

    let args: Vec<String> = ::std::env::args().collect();
    if args.len() != 3 {
        println!("usage: {} client HOST:PORT", args[0]);
        return Ok(());
    }

    let addr = args[2]
        .to_socket_addrs()?
        .next()
        .expect("could not parse address");

    tokio::task::LocalSet::new().run_until(try_main(addr)).await
}

async fn try_main(addr: SocketAddr) -> Result<(), Box<dyn Error>> {
    trace!("start");
    let stream = tokio::net::TcpStream::connect(&addr).await?;
    info!("connected");
    stream.set_nodelay(true)?;
    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
    let rpc_network = Box::new(twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));
    let mut rpc_system = RpcSystem::new(rpc_network, None);
    let publisher: publisher::Client<::capnp::text::Owned> =
        rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

    let mut request = publisher.subscribe_request();
    let sub = capnp_rpc::new_client(SubscriberImpl::new());
    request.get().set_subscriber(sub);

    futures::future::try_join(rpc_system, request.send().promise).await?;

    Ok(())
}
