use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt, StreamExt};
use log::{info, trace, warn};
use std::error::Error;
use std::net::SocketAddr;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::pubsub_capnp::{publisher, subscriber, subscription};

const MAX_CONN: i32 = 3;

struct SubscriberHandle {
    client: subscriber::Client<::capnp::text::Owned>,
    requests_in_flight: i32,
}

struct SubscriberMap {
    subscribers: HashMap<u64, SubscriberHandle>,
}

impl SubscriberMap {
    fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
        }
    }
}

struct SubscriptionImpl {
    id: u64,
    subscribers: Rc<RefCell<SubscriberMap>>,
}

impl SubscriptionImpl {
    fn new(id: u64, subscribers: Rc<RefCell<SubscriberMap>>) -> Self {
        Self { id, subscribers }
    }
}

impl Drop for SubscriptionImpl {
    fn drop(&mut self) {
        trace!("subscription dropped");
        self.subscribers.borrow_mut().subscribers.remove(&self.id);
    }
}

impl subscription::Server for SubscriptionImpl {}

struct PublisherImpl {
    next_id: u64,
    subscribers: Rc<RefCell<SubscriberMap>>,
}

impl PublisherImpl {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            subscribers: Rc::new(RefCell::new(SubscriberMap::new())),
        }
    }
}

impl publisher::Server<::capnp::text::Owned> for PublisherImpl {
    fn subscribe(
        &mut self,
        params: publisher::SubscribeParams<::capnp::text::Owned>,
        mut results: publisher::SubscribeResults<::capnp::text::Owned>,
    ) -> Promise<(), capnp::Error> {
        info!("subscribe");
        self.subscribers.borrow_mut().subscribers.insert(
            self.next_id,
            SubscriberHandle {
                client: pry!(pry!(params.get()).get_subscriber()),
                requests_in_flight: 0,
            },
        );

        let sub: subscription::Client = capnp_rpc::new_client(SubscriptionImpl::new(
            self.next_id,
            Rc::clone(&self.subscribers),
        ));
        results.get().set_subscription(sub);

        self.next_id += 1;
        Promise::ok(())
    }
}

pub async fn main() -> Result<(), Box<dyn Error>> {
    use std::net::ToSocketAddrs;
    let args: Vec<String> = ::std::env::args().collect();
    if args.len() != 3 {
        println!("usage: {} server HOST:PORT", args[0]);
        return Ok(());
    }

    let addr = args[2]
        .to_socket_addrs()?
        .next()
        .expect("could not parse address");

    tokio::task::LocalSet::new().run_until(try_main(addr)).await
}

async fn try_main(addr: SocketAddr) -> Result<(), Box<dyn Error>> {
    trace!("start server");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let publisher_impl = PublisherImpl::new();
    let subscribers = Rc::clone(&publisher_impl.subscribers);
    let publisher: publisher::Client<_> = capnp_rpc::new_client(publisher_impl);

    let handle_incoming = async move {
        loop {
            trace!("listening...");
            let (stream, _) = listener.accept().await?;
            info!("accepted");
            stream.set_nodelay(true)?;
            let (reader, writer) =
                tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
            let rpc_network = Box::new(twoparty::VatNetwork::new(
                reader,
                writer,
                rpc_twoparty_capnp::Side::Server,
                Default::default(),
            ));
            let rpc_system = RpcSystem::new(rpc_network, Some(publisher.clone().client));

            tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));
        }
    };

    let (tx, mut rx) = futures::channel::mpsc::unbounded::<()>();
    std::thread::spawn(move || {
        while let Ok(()) = tx.unbounded_send(()) {
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    });

    let send_to_subscribers = async move {
        while let Some(()) = rx.next().await {
            let subscribers1 = Rc::clone(&subscribers);
            let subs = &mut subscribers.borrow_mut().subscribers;
            for (&idx, mut subscriber) in subs.iter_mut() {
                if subscriber.requests_in_flight < MAX_CONN {
                    info!(
                        "id: {}, requests_in_flight : {}",
                        idx, subscriber.requests_in_flight
                    );
                    subscriber.requests_in_flight += 1;
                    trace!(
                        "Increment: request_in_flight: {}",
                        subscriber.requests_in_flight
                    );
                    let mut request = subscriber.client.push_message_request();
                    request.get().set_message(&format!(
                        "system time is: {:?}",
                        ::std::time::SystemTime::now()
                    ))?;
                    let subscribers2 = Rc::clone(&subscribers1);
                    tokio::task::spawn_local(Box::pin(request.send().promise.map(
                        move |r| match r {
                            Ok(_) => {
                                subscribers2.borrow_mut().subscribers.get_mut(&idx).map(
                                    |ref mut s| {
                                        s.requests_in_flight -= 1;
                                        trace!(
                                            "Decrement: request_in_flight: {}",
                                            s.requests_in_flight
                                        );
                                    },
                                );
                            }
                            Err(e) => {
                                warn!("Got error: {:?}. Dropping subscriber.", e);
                                subscribers2.borrow_mut().subscribers.remove(&idx);
                            }
                        },
                    )));
                }
            }
        }
        Ok::<(), Box<dyn Error>>(())
    };

    let _: ((), ()) = futures::future::try_join(handle_incoming, send_to_subscribers).await?;
    Ok(())
}
