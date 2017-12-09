//! A slog adapter for retrying on errors.
//!
//! The [slog](https://crates.io/crates/slog) infrastructure is very powerful and can be bent to
//! many scenarios. Many of the loggers there may fail, for example because they log over the
//! network ‒ like [slog-json](https://crates.io/crates/slog-json) logging into a TCP stream.
//!
//! The basic framework allows for three options:
//!
//! * Handle the error manually, which is uncomfortable.
//! * Ignore the error, but then all the future records are lost.
//! * Fuse the drain, making it explode on the first error and killing the whole application.
//!
//! This crate brings an adapter that initializes the drain anew each time an error happens, adding
//! the ability to recover from errors.
//!
//! # Warning
//!
//! The adapter blocks the current thread on reconnects. Therefore, you want to wrap it inside
//! [slog-async](https://crates.io/crates/slog-async) and not it directly as the root drain.
//!
//! # Examples
//!
//! ```rust,no_run
//! #[macro_use]
//! extern crate slog;
//! extern crate slog_async;
//! extern crate slog_json;
//! extern crate slog_retry;
//!
//! use std::net::TcpStream;
//!
//! use slog::Drain;
//!
//! fn main() {
//!     let retry = slog_retry::Retry::new(|| -> Result<_, std::io::Error> {
//!             let connection = TcpStream::connect("127.0.0.1:1234")?;
//!             Ok(slog_json::Json::default(connection))
//!         }, None, true)
//!         // Kill the application if the initial connection fails
//!         .unwrap()
//!         // Ignore if it isn't possible to log some of the messages, we'll try again
//!         .ignore_res();
//!     let async = slog_async::Async::default(retry)
//!         .fuse();
//!     let root = slog::Logger::root(async, o!());
//!     info!(root, "Everything is set up");
//! }
//! ```

extern crate failure;
extern crate slog;

use std::cell::{Cell, RefCell, RefMut};
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::iter;
use std::thread;
use std::time::Duration;

use failure::Fail;
use slog::{Drain, Record, OwnedKVList};

/// An error when the retry adaptor fails.
///
/// It wasn't possible to log the record (or initialize it when starting). Usually that means it
/// wasn't possible to create the drain at all or that each newly created drain failed.
#[derive(Debug)]
pub struct Error<FactoryError: Fail + Debug, SlaveError: Fail + Debug> {
    /// The last error during creation of a new drain, if any.
    pub factory: Option<FactoryError>,
    /// The last error during logging attempt, if any.
    pub slave: Option<SlaveError>,
}

impl<FactoryError, SlaveError> Fail for Error<FactoryError, SlaveError>
where
    FactoryError: Fail + Debug,
    SlaveError: Fail + Debug,
{
    fn cause(&self) -> Option<&Fail> {
        if let Some(ref slave) = self.slave {
            return Some(slave);
        }
        if let Some(ref fact) = self.factory {
            return Some(fact);
        }
        None
    }
}

impl<FactoryError, SlaveError> Display for Error<FactoryError, SlaveError>
where
    FactoryError: Fail + Debug,
    SlaveError: Fail + Debug,
{
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        let factory = self.factory
            .as_ref()
            .map(|f| format!("{}", f))
            .unwrap_or_else(|| "()".to_owned());
        let slave = self.slave
            .as_ref()
            .map(|s| format!("{}", s))
            .unwrap_or_else(|| "()".to_owned());
        write!(fmt, "Failed to reconnect the logging drain: {}/{}", factory, slave)
    }
}

/// A retry strategy.
///
/// The iterator describes how long to wait before reconnection attempts. Once the iterator runs
/// out of items, the adapter gives up trying to reconnect. Therefore, it specifies both the
/// waiting intervals and number of retries.
pub type Strategy = Box<Iterator<Item = Duration>>;

/// A constructor of a new instance of a retry strategy.
///
/// Every time the adapter needs to retry connection, it calls the constructor and gets a fresh
/// retry strategy.
pub type NewStrategy = Box<Fn() -> Strategy + Send>;

/// The retry adapter.
///
/// This wraps another drain and forwards log records into that. However, if the drain returns an
/// error, it discards it and tries to create a new one and log the message into it.
///
/// It uses the [retry strategy](type.Strategy.html) to decide how long to wait before retrying and
/// how many times. If the retry strategy runs out of items, it gives up, returns an error and the
/// log record is lost.
///
/// However, it is not destroyed by the error and if it is called to log another record, it tries
/// to reconnect again (using a fresh instance of the strategy).
///
/// # Warning
///
/// This adapter is *synchronous* and *blocks* during the retry attempts. Unless you provide a
/// retry strategy with a single zero item, you don't want to use it directly. Wrap it inside
/// [slog-async](https://crates.io/crates/slog-async), where it'll only slow down the logging
/// thread and the channel into that thread will be used as a buffer for messages waiting to be
/// written after the reconnect.
pub struct Retry<Slave, Factory> {
    slave: RefCell<Option<Slave>>,
    factory: Factory,
    strategy: NewStrategy,
    initialized: Cell<bool>,
}

impl<Slave, FactoryError, Factory> Retry<Slave, Factory>
where
    Slave: Drain,
    FactoryError: Fail + Debug,
    Slave::Err: Fail + Debug,
    Factory: Fn() -> Result<Slave, FactoryError>,
{
    /// Creates a new retry adapter.
    ///
    /// # Parameters
    ///
    /// * `factory`: A factory function that is used to produce new instance of the slave drain on
    ///   every (re)connection attempt.
    /// * `strategy`: A reconnect strategy, describing how long to wait between attempts and how
    ///   many attempts to make. If set to `None` a default strategy with 4 increasingly delayed
    ///   attemps is used.
    /// * `connect_now`: Should a connection be made right away. If it is set to `true`, it may
    ///   block (it uses the reconnect strategy provided) and it may return an error. If set to
    ///   `false`, the connection is made on the first logged message. No matter if connecting now
    ///   or later, the first connection attempt is without waiting.
    pub fn new(factory: Factory, strategy: Option<NewStrategy>, connect_now: bool)
        -> Result<Self, Error<FactoryError, Slave::Err>>
    {
        let result = Self {
            slave: RefCell::new(None),
            factory,
            strategy: strategy.unwrap_or_else(|| Box::new(|| default_new_strategy())),
            initialized: Cell::new(false),
        };
        if connect_now {
            result.init(&mut result.slave.borrow_mut(), &mut (result.strategy)())
                .map_err(|factory| Error { factory, slave: None })?;
        }
        Ok(result)
    }
    fn init(&self, slave: &mut RefMut<Option<Slave>>, strategy: &mut Strategy)
        -> Result<(), Option<FactoryError>>
    {
        let prefix: Strategy = if self.initialized.get() {
            Box::new(iter::empty())
        } else {
            self.initialized.set(true);
            Box::new(iter::once(Duration::from_secs(0)))
        };
        let mut last_err = None;
        for sleep in prefix.chain(strategy) {
            thread::sleep(sleep);
            match (self.factory)() {
                Ok(ok) => {
                    **slave = Some(ok);
                    return Ok(());
                },
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err)
    }
}

impl<Slave, FactoryError, Factory> Drain for Retry<Slave, Factory>
where
    Slave: Drain,
    FactoryError: Fail + Debug,
    Slave::Err: Fail + Debug,
    Factory: Fn() -> Result<Slave, FactoryError>,
{
    type Ok = Slave::Ok;
    type Err = Error<FactoryError, Slave::Err>;
    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        let mut borrowed = self.slave.borrow_mut();
        let mut slave_err = None;

        if let Some(ref slave) = *borrowed {
            match slave.log(record, values) {
                Ok(ok) => return Ok(ok),
                Err(err) => slave_err = Some(err),
            }
        }
        // By now there was no slave to start with or it failed, so we recreate it.
        borrowed.take();

        // Try creating a new one and retry with that.
        let mut strategy = (self.strategy)();
        loop {
            match self.init(&mut borrowed, &mut strategy) {
                Err(factory) => return Err(Error {
                    factory,
                    slave: slave_err,
                }),
                Ok(()) => match borrowed.as_ref().unwrap().log(record, values) {
                    Ok(ok) => return Ok(ok),
                    Err(err) => slave_err = Some(err),
                },
            }
        }
    }
}

fn default_new_strategy() -> Strategy {
    let iterator = (1..5)
        .map(Duration::from_secs);
    Box::new(iterator)
}
