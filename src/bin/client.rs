extern crate worker_pool;

extern crate fibers;
extern crate futures;
extern crate handy_async;

extern crate serde;
extern crate serde_json;

use fibers::{Spawn, Executor, InPlaceExecutor};
use fibers::net::TcpStream;
use futures::{Future, Stream};
use handy_async::io::{AsyncWrite, ReadFrom};
use handy_async::pattern::AllowPartial;

fn main() {
    let server_addr = "127.0.0.1:3000".parse().expect(
        "Failed to parse server addr",
    );

    // `InPlaceExecutor` is suitable to execute a few fibers.
    // It does not create any background threads,
    // so the overhead to manage fibers is lower than `ThreadPoolExecutor`.
    let mut executor = InPlaceExecutor::new().expect("Cannot create Executor");
    let handle = executor.handle();

    // Spawns a fiber for echo client.
    let monitor = executor.spawn_monitor(TcpStream::connect(server_addr).and_then(move |stream| {
        println!("# CONNECTED: {}", server_addr);
        let (reader, writer) = (stream.clone(), stream);

        handle.spawn(futures::lazy(move || {
            for value in 0..10000 {
                let msg = worker_pool::Message::new("one".to_owned(), Some(value));
                let mut buf: String =
                    serde_json::to_string(&msg).expect("Failed to serialize message");

                buf.push_str("\r\n\r\n");

                writer.clone().async_write_all(buf).wait().expect(
                    "Failed to write messsage",
                );
            }
            Ok(())
        }));

        // Reader: It outputs data received from the server to the standard output stream.
        let stream = vec![0; 256].allow_partial().into_stream(reader);
        stream.map_err(|e| e.into_error()).for_each(
            |(mut buf, len)| {
                buf.truncate(len);
                println!("{}", String::from_utf8(buf).expect("Invalid UTF-8"));
                Ok(())
            },
        )
    }));

    // Runs until the above fiber is terminated (i.e., The TCP stream is disconnected).
    let result = executor.run_fiber(monitor).expect("Execution failed");
    println!("# Disconnected: {:?}", result);
}
