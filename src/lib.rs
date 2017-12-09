#[macro_use]
extern crate failure;
extern crate slog;

use std::cell::{RefCell, RefMut};
use std::fmt::Debug;
use std::iter;
use std::thread;
use std::time::Duration;

use failure::Fail;
use slog::{Drain, Record, OwnedKVList};

#[derive(Debug, Fail)]
#[fail(display = "Run out of retries with last errors {}/{}", factory, slave)]
pub struct Error<FactoryError: Fail + Debug, SlaveError: Fail + Debug> {
    pub factory: Option<FactoryError>,
    pub slave: Option<SlaveError>,
}

pub type Strategy = Box<Iterator<Item = Duration>>;
pub type NewStrategy = Box<Fn() -> Strategy>;

pub struct Retry<Slave, Factory> {
    slave: RefCell<Option<Slave>>,
    factory: Factory,
    strategy: NewStrategy,
}

impl<Slave, FactoryError, Factory> Retry<Slave, Factory>
where
    Slave: Drain,
    FactoryError: Fail + Debug,
    Slave::Err: Fail + Debug,
    Factory: Fn() -> Result<Slave, FactoryError>,
{
    pub fn new(factory: Factory, strategy: Option<NewStrategy>)
        -> Result<Self, Error<FactoryError, Slave::Err>>
    {
        let result = Self {
            slave: RefCell::new(None),
            factory,
            strategy: strategy.unwrap_or_else(|| Box::new(|| default_new_strategy())),
        };
        let strategy = iter::once(Duration::from_secs(0)).chain((result.strategy)());
        let mut strategy: Strategy = Box::new(strategy);
        result.init(&mut result.slave.borrow_mut(), &mut strategy)
            .map_err(|factory| Error { factory, slave: None })?;
        Ok(result)
    }
    fn init(&self, slave: &mut RefMut<Option<Slave>>, strategy: &mut Strategy)
        -> Result<(), Option<FactoryError>>
    {
        let mut last_err = None;
        for sleep in strategy {
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
