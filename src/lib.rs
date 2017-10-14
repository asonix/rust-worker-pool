#[macro_use]
extern crate serde_derive;

extern crate futures;
extern crate fibers;
extern crate serde;
extern crate serde_json;

use std::collections::HashMap;
use std::thread;
use std::fmt::Debug;
use fibers::{Spawn, Executor};
use fibers::executor::{ThreadPoolExecutor, ThreadPoolExecutorHandle};
use futures::sync::mpsc;
use futures::{Future, Sink, Stream};

pub use fibers::executor::ThreadPoolExecutorHandle as ExecHandle;

pub static EXIT_STR: &'static str = "exit";

#[derive(Debug, Clone)]
pub struct Config<'a, T>
where
    T: 'a + Debug + Clone + serde::de::DeserializeOwned,
{
    handlers: HashMap<&'a str, &'a Handler<T>>,
}

impl<'a, T> Default for Config<'a, T>
where
    T: 'a + Debug + Clone + serde::de::DeserializeOwned,
{
    fn default() -> Self {
        Config { handlers: HashMap::new() }
    }
}

impl<'a, T> Config<'a, T>
where
    T: 'a + Debug + Clone + serde::de::DeserializeOwned,
{
    pub fn new() -> Self {
        Default::default()
    }

    pub fn handlers(self) -> HashMap<&'a str, &'a Handler<T>> {
        self.handlers
    }

    pub fn register_handler(&mut self, name: &'a str, handler: &'a Handler<T>) -> Result<(), ()> {
        if name == EXIT_STR {
            return Err(());
        }

        if self.handlers.contains_key(name) {
            return Err(());
        }

        self.handlers.insert(name, handler);

        Ok(())
    }
}

pub trait Handler<T>: Send + Sync + Debug
where
    T: Send + Sync + Clone + Debug,
{
    fn handle_present(&self, handle: ThreadPoolExecutorHandle, msg: &T) -> Result<(), ()>;
    fn handle_missing(&self, handle: ThreadPoolExecutorHandle) -> Result<(), ()>;

    fn handle(&self, handle: ThreadPoolExecutorHandle, msg: &Option<T>) -> Result<(), ()> {
        match *msg {
            Some(ref msg) => self.handle_present(handle, msg),
            None => self.handle_missing(handle),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<T>
where
    T: Debug + Clone,
{
    name: String,
    message: Option<T>,
    retries: i32,
}

impl<T> Message<T>
where
    T: Debug + Clone,
{
    pub fn new(name: String, message: Option<T>) -> Message<T> {
        Message::<T> {
            name: name,
            message: message,
            retries: 10,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn message(&self) -> &Option<T> {
        &self.message
    }

    pub fn retries(&self) -> i32 {
        self.retries
    }

    pub fn retry(&self) -> Self {
        Message {
            name: self.name.clone(),
            message: self.message.clone(),
            retries: self.retries - 1,
        }
    }
}

pub fn manager_thread<T>(
    config: Config<'static, T>,
    msg_sender: mpsc::Sender<Message<T>>,
    msg_receiver: mpsc::Receiver<Message<T>>,
) -> thread::JoinHandle<()>
where
    T: 'static + Send + Sync + Clone + Debug + serde::de::DeserializeOwned,
{
    println!("Creating Manager Thread");
    let mut executor = ThreadPoolExecutor::new().expect("Failed to create executor");
    let exec_handle = executor.handle();

    thread::spawn(move || {
        let handlers = config.handlers();

        let monitor = executor.spawn_monitor(futures::lazy(|| {
            println!("Executing stuff");

            msg_receiver
                .for_each(move |msg: Message<T>| {
                    let handler = match handlers.get(msg.name()) {
                        Some(handler) => handler,
                        None => return Err(()),
                    };

                    let result = handler.handle(exec_handle.clone(), msg.message());

                    if result.is_err() {
                        msg_sender.clone().send(msg.retry()).wait().expect(
                            "Failed to send retry",
                        );
                    }

                    Ok(())
                })
                .wait()
        }));

        let result = executor.run_fiber(monitor).expect("Failed to run executor");

        println!("Fiber result: {:?}", result);
    })
}

#[derive(Debug)]
pub struct Handles<T>
where
    T: Send + Sync + Clone + Debug + serde::de::DeserializeOwned,
{
    msg_sender: mpsc::Sender<Message<T>>,
    manager_thread: thread::JoinHandle<()>,
}

impl<T> Handles<T>
where
    T: Send + Sync + Clone + Debug + serde::de::DeserializeOwned,
{
    pub fn new(
        msg_sender: mpsc::Sender<Message<T>>,
        manager_thread: thread::JoinHandle<()>,
    ) -> Self {
        Handles {
            msg_sender,
            manager_thread,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.manager_thread.join()
    }

    pub fn sender(&self) -> mpsc::Sender<Message<T>> {
        self.msg_sender.clone()
    }
}

pub fn run<T>(config: Config<'static, T>) -> Handles<T>
where
    T: 'static + Send + Sync + Clone + Debug + serde::de::DeserializeOwned,
{
    let (tx, rx) = mpsc::channel::<Message<T>>(1000);

    let thread_handle = manager_thread(config, tx.clone(), rx);

    Handles::new(tx, thread_handle)
}
