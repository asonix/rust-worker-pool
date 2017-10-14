extern crate worker_pool;

extern crate serde;
extern crate serde_json;
extern crate hyper;
extern crate futures;
extern crate tokio_core;

use std::io::{self, Write};
use futures::{Future, Stream};
use hyper::{Client, Method, Request};
use hyper::header::{ContentLength, ContentType};
use tokio_core::reactor::Core;

fn make_request(value: i32, uri: hyper::Uri) -> Request {
    let mut req = Request::new(Method::Post, uri);
    req.headers_mut().set(ContentType::json());
    let msg = worker_pool::Message::new("one".to_owned(), Some(value));

    let msg_str = serde_json::to_string(&msg).expect("Failed to serialize JSON");

    req.headers_mut().set(ContentLength(msg_str.len() as u64));
    req.set_body(msg_str);

    req
}

fn main() {
    let mut core = Core::new().expect("Failed to create core");
    let client = Client::new(&core.handle());

    let uri: hyper::Uri = "http://127.0.0.1:3000".parse().expect(
        "Failed to parse URI",
    );

    let requests = futures::future::join_all((0..300).map(|value| {
        let req = make_request(value, uri.clone());

        client.request(req).and_then(|res| {
            res.body().for_each(|chunk| {
                io::stdout().write_all(&chunk).map(|_| ()).map_err(
                    From::from,
                )
            })
        })
    }));

    let res = core.run(requests);
    println!("Result: {:?}", res);
}
