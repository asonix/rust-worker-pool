extern crate worker_pool;

extern crate fibers;
extern crate serde;
extern crate serde_json;
extern crate futures;
extern crate hyper;

use std::fmt::Debug;
use futures::{Future, Sink, Stream};
use futures::sync::mpsc;
use fibers::fiber::Spawn;
use hyper::server::{Http, Request, Response, Service};
use hyper::{Chunk, Method, StatusCode};

#[derive(Debug)]
pub struct OneHandler;

impl worker_pool::Handler<i32> for OneHandler {
    fn handle_present(&self, h: worker_pool::ExecHandle, msg: &i32) -> Result<(), ()> {
        let msg = *msg;
        h.spawn(futures::lazy(move || {
            println!("Got a value: {}", msg);
            Ok(()) as Result<(), ()>
        }));

        Ok(()) as Result<(), ()>
    }

    fn handle_missing(&self, h: worker_pool::ExecHandle) -> Result<(), ()> {
        h.spawn(futures::lazy(move || {
            println!("Got nothing!");
            Ok(()) as Result<(), ()>
        }));

        Ok(()) as Result<(), ()>
    }
}

pub static ONE_HANDLER: OneHandler = OneHandler {};

#[derive(Debug)]
struct HelloWorld<T>
where
    T: Send + Sync + Clone + Debug,
{
    sender: mpsc::Sender<worker_pool::Message<T>>,
}

impl<T> HelloWorld<T>
where
    T: Send + Sync + Clone + Debug,
{
    pub fn new(sender: mpsc::Sender<worker_pool::Message<T>>) -> Self {
        HelloWorld { sender }
    }

    pub fn sender(&self) -> mpsc::Sender<worker_pool::Message<T>> {
        self.sender.clone()
    }
}

impl<T> Service for HelloWorld<T>
where
    T: 'static + Send + Sync + Clone + Debug + serde::de::DeserializeOwned,
{
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let sender = self.sender();

        match (req.method(), req.path()) {
            (&Method::Post, "/") => {
                Box::new(req.body().concat2().map(|chunk: Chunk| {
                    let res: Result<worker_pool::Message<T>, _> = serde_json::from_slice(&chunk);

                    if let Ok(msg) = res {
                        sender.send(msg).wait().expect("Failed to send");
                    }

                    Response::new().with_body("Hewwo???\n")
                }))
            }
            _ => {
                Box::new(futures::future::ok(
                    Response::new().with_status(StatusCode::NotFound),
                ))
            }
        }
    }
}

fn main() {
    let addr = "127.0.0.1:3000".parse().expect(
        "Failed to parse server addr",
    );
    let mut config = worker_pool::Config::new();
    config.register_handler("one", &ONE_HANDLER).expect(
        "Failed to register handler",
    );

    let handles = worker_pool::run(config);
    let sender = handles.sender();

    let server = Http::new()
        .bind(&addr, move || Ok(HelloWorld::new(sender.clone())))
        .expect("Failed to create server");
    server.run().expect("Failed to start server");

    let result = handles.join();
    println!("Manager Thread Closed: {:?}", result);
}
