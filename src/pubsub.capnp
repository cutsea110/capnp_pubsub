@0xaabb018bf3072f99;

interface Subscription {}

interface Publisher(T) {
  subscribe @0 (subscriber: Subscriber(T)) -> (subscription: Subscription);
}

interface Subscriber(T) {
  pushMessage @0 (message: T) -> ();
}
