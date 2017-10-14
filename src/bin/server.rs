extern crate worker_pool;

extern crate serde;
extern crate serde_json;
extern crate fibers;
extern crate futures;
extern crate handy_async;

use std::io;
use fibers::{Spawn, Executor, ThreadPoolExecutor};
use fibers::executor::ThreadPoolExecutorHandle;
use fibers::net::TcpListener;
use futures::{Future, Sink, Stream};
use handy_async::io::{AsyncWrite, ReadFrom};
use handy_async::pattern::AllowPartial;

#[derive(Debug)]
struct OneHandler;

impl worker_pool::Handler<i32> for OneHandler {
    fn handle_present(&self, h: ThreadPoolExecutorHandle, msg: &i32) -> Result<(), ()> {
        let msg = *msg;
        h.spawn(futures::lazy(move || {
            println!("Got a value: {}", msg);
            Ok(()) as Result<(), ()>
        }));

        Ok(()) as Result<(), ()>
    }

    fn handle_missing(&self, h: ThreadPoolExecutorHandle) -> Result<(), ()> {
        h.spawn(futures::lazy(move || {
            println!("Got nothing!");
            Ok(()) as Result<(), ()>
        }));

        Ok(()) as Result<(), ()>
    }
}

static ONE_HANDLER: OneHandler = OneHandler {};

fn main() {
    let mut config = worker_pool::Config::new();
    config.register_handler("one", &ONE_HANDLER).expect(
        "Failed to register handler",
    );
    let handles = worker_pool::run(config);

    let sender = handles.sender();

    let server_addr = "127.0.0.1:3000".parse().expect("Invalid TCP bind address");

    let mut executor = ThreadPoolExecutor::new().expect("Cannot create Executor");
    let handle0 = executor.handle();
    let monitor = executor.spawn_monitor(TcpListener::bind(server_addr).and_then(move |listener| {
        println!("# Start listening: {}: ", server_addr);

        // Creates a stream of incoming TCP client sockets
        listener.incoming().for_each(move |(client, addr)| {
            // New client is connected.
            println!("# CONNECTED: {}", addr);
            let handle1 = handle0.clone();

            let sender1 = sender.clone();

            // Spawns a fiber to handle the client.
            handle0.spawn(
                client
                    .and_then(move |client| {
                        // For simplicity, splits reading process and
                        // writing process into differrent fibers.
                        let (reader, writer) = (client.clone(), client);
                        let (tx, rx) = fibers::sync::mpsc::channel();

                        // let handle2 = handle1.clone();
                        let sender2 = sender1.clone();

                        // Spawns a fiber for the writer side.
                        // When a message is arrived in `rx`,
                        // this fiber sends it back to the client.
                        handle1.spawn(
                            rx.map_err(|_| -> io::Error { unreachable!() })
                                .fold(writer, move |writer, buf: Vec<u8>| {
                                    let buf2 = buf.clone();

                                    if let Ok(value) = String::from_utf8(buf) {
                                        let values = value.split("\r\n\r\n");
                                        for v in values {
                                            println!("JSON: {}", &v);
                                            if let Ok(message) = serde_json::from_str(&v) {
                                                if let Err(err) = sender2
                                                    .clone()
                                                    .send(message)
                                                    .wait()
                                                {
                                                    println!("Result: {}", err);
                                                }
                                            }
                                        }
                                    }

                                    println!("# SEND: {} bytes", buf2.len());
                                    writer.async_write_all(buf2).map(|(w, _)| w).map_err(|e| {
                                        e.into_error()
                                    })
                                })
                                .then(|r| {
                                    println!("# Writer finished: {:?}", r);
                                    Ok(())
                                }),
                        );

                        // The reader side is executed in the current fiber.
                        let stream = vec![0; 1024].allow_partial().into_stream(reader);
                        stream.map_err(|e| e.into_error()).fold(
                            tx,
                            |tx, (mut buf, len)| {
                                buf.truncate(len);
                                println!("# RECV: {} bytes", buf.len());

                                // Sends received  to the writer half.
                                tx.send(buf).expect("Cannot send");
                                Ok(tx) as io::Result<_>
                            },
                        )
                    })
                    .then(|r| {
                        println!("# Client finished: {:?}", r);
                        Ok(())
                    }),
            );
            Ok(())
        })
    }));
    let result = executor.run_fiber(monitor).expect("Execution failed");
    println!("# Listener finished: {:?}", result);

    handles.join().expect("Failed to wait on thread");
}
