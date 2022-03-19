extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .file("src/pubsub.capnp")
        .run()
        .unwrap();
}
