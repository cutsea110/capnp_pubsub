use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt, StreamExt};
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::pubsub_capnp::{publisher, subscriber, subscription};

struct SubscriberHandle {
    client: subscriber::Client<::capnp::text::Owned>,
    requests_in_flight: i32,
}

struct SubscriberMap {
    subscribers: HashMap<u64, SubscriberHandle>,
}

impl SubscriberMap {
    fn new() -> SubscriberMap {
        SubscriberMap {
            subscribers: HashMap::new(),
        }
    }
}

struct SubscriptionImpl {
    id: u64,
    subscribers: Rc<RefCell<SubscriberMap>>,
}

impl SubscriptionImpl {
    fn new(id: u64, subscribers: Rc<RefCell<SubscriberMap>>) -> SubscriptionImpl {
        SubscriptionImpl { id, subscribers }
    }
}

impl Drop for SubscriptionImpl {
    fn drop(&mut self) {
        println!("subscription dropped");
        self.subscribers.borrow_mut().subscribers.remove(&self.id);
    }
}

impl subscription::Server for SubscriptionImpl {}

struct PublisherImpl {
    next_id: u64,
    subscribers: Rc<RefCell<SubscriberMap>>,
}

impl PublisherImpl {
    pub fn new() -> (PublisherImpl, Rc<RefCell<SubscriberMap>>) {
        let subscribers = Rc::new(RefCell::new(SubscriberMap::new()));
        (
            PublisherImpl {
                next_id: 0,
                subscribers: subscribers.clone(),
            },
            subscribers.clone(),
        )
    }
}

impl publisher::Server<::capnp::text::Owned> for PublisherImpl {
    fn subscribe(
        &mut self,
        params: publisher::SubscribeParams<::capnp::text::Owned>,
        mut results: publisher::SubscribeResults<::capnp::text::Owned>,
    ) -> Promise<(), capnp::Error> {
        println!("subscribe");
        self.subscribers.borrow_mut().subscribers.insert(
            self.next_id,
            SubscriberHandle {
                client: pry!(pry!(params.get()).get_subscriber()),
                requests_in_flight: 0,
            },
        );

        results
            .get()
            .set_subscription(capnp_rpc::new_client(SubscriptionImpl::new(
                self.next_id,
                self.subscribers.clone(),
            )));

        self.next_id += 1;
        Promise::ok(())
    }
}

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::net::ToSocketAddrs;
    let args: Vec<String> = ::std::env::args().collect();
    if args.len() != 3 {
        println!("usage: {} server HOST:PORT", args[0]);
        return Ok(());
    }

    let addr = args[2]
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("could not parse address");

    tokio::task::LocalSet::new()
        .run_until(async move {
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            let (publisher_impl, subscribers) = PublisherImpl::new();
            let publisher: publisher::Client<_> = capnp_rpc::new_client(publisher_impl);

            let handle_incoming = async move {
                loop {
                    let (stream, _) = listener.accept().await?;
                    stream.set_nodelay(true)?;
                    let (reader, writer) =
                        tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                    let network = twoparty::VatNetwork::new(
                        reader,
                        writer,
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                    );
                    let rpc_system =
                        RpcSystem::new(Box::new(network), Some(publisher.clone().client));

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
                    let subscribers1 = subscribers.clone();
                    let subs = &mut subscribers.borrow_mut().subscribers;
		    for (&idx, mut subscriber) in subs.iter_mut() {
			if subscriber.requests_in_flight < 5 {
			    subscriber.requests_in_flight += 1;
			    let mut request = subscriber.client.push_message_request();
			    request.get().set_message(&format!("system time is: {:?}", ::std::time::SystemTime::now())[..])?;
			    let subscribers2 = subscribers1.clone();
			    tokio::task::spawn_local(Box::pin(request.send().promise.map(move |r| {
				match r {
				    Ok(_) => {
				    },
				    Err(e) => {
					println!("Got error: {:?}. Dropping subscriber.", e);
					subscribers2.borrow_mut().subscribers.remove(&idx);
				    }
				}
			    })));
			}
		    }
                }
		Ok::<(), Box<dyn std::error::Error>>(())
            };

	    let _: (() , ()) = futures::future::try_join(handle_incoming, send_to_subscribers).await?;
	    Ok(())
        })
        .await
}
