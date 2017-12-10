//! Tests of the retry handling
//!
//! To be able to test it, we create a fake logger and a fake factory that check against a scenario
//! if they should return a success or error each time.

#[macro_use]
extern crate failure;
#[macro_use]
extern crate slog;
extern crate slog_retry;

use std::iter;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use slog::{Drain, Logger, OwnedKVList, Record};
use slog_retry::{NewStrategy, Retry};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Action {
    FactoryError,
    FactorySuccess,
    LogError,
    LogSuccess,
}

use Action::*;
use Part::*;

impl Action {
    fn part(&self) -> Part {
        match *self {
            FactoryError | FactorySuccess => Factory,
            LogError | LogSuccess => Log,
        }
    }

    fn result(&self) -> Result<(), ()> {
        match *self {
            FactoryError | LogError => Err(()),
            FactorySuccess | LogSuccess => Ok(()),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Part {
    Factory,
    Log,
}

/// A list of action to be taken during the test. It both checks that the factory and logger ask
/// for actions in the right order and directs them if they should fail or succeed.
///
/// All the provided actions must be consumed before the end of the test (or, before the scenario
/// is dropped).
#[derive(Debug)]
struct Scenario {
    // The planned, expected actions
    actions: Mutex<Vec<Action>>,
}

impl Scenario {
    fn new(mut actions: Vec<Action>) -> Arc<Self> {
        // We're going to pop the items, so we want the first one at the end
        actions.reverse();
        Arc::new(Self {
            actions: Mutex::new(actions),
        })
    }
    fn take_action(&self, part: Part) -> Action {
        let next_action = self.actions.lock().unwrap().pop().expect(&format!(
            "No more actions are expected, but {:?} asked for one",
            part
        ));
        assert_eq!(part, next_action.part());
        next_action
    }
}

impl Drop for Scenario {
    fn drop(&mut self) {
        // Don't double-panic. This is OK, if we are already panicking, the test will fail anyway
        // (and will likely not have exhausted all the actions due to that).
        if !thread::panicking() {
            assert!(
                self.actions.lock().unwrap().is_empty(),
                "Test didn't take all expected actions"
            );
        }
    }
}

#[derive(Debug, Fail)]
#[fail(display = "Injected logger error")]
struct LoggerError;

struct FailLogger(Arc<Scenario>);

impl Drain for FailLogger {
    type Ok = ();
    type Err = LoggerError;
    fn log(&self, _: &Record, _: &OwnedKVList) -> Result<(), LoggerError> {
        self.0.take_action(Log).result().map_err(|()| LoggerError)
    }
}

fn strategy(len: usize) -> Option<NewStrategy> {
    Some(Box::new(move || {
        let iter = iter::repeat(0).map(Duration::from_secs).take(len);
        Box::new(iter)
    }))
}

#[derive(Debug, Fail)]
#[fail(display = "Injected factory error")]
struct CreateError;

fn factory(scenario: &Arc<Scenario>) -> Result<FailLogger, CreateError> {
    scenario
        .take_action(Factory)
        .result()
        .map_err(|()| CreateError)
        .map(|()| FailLogger(Arc::clone(scenario)))
}

fn logger(scenario: Arc<Scenario>, strategy: Option<NewStrategy>, connect_now: bool) -> Logger {
    let retry = Retry::new(move || factory(&scenario), strategy, connect_now)
        .unwrap()
        .ignore_res();
    Logger::root(Mutex::new(retry).fuse(), o!())
}

/// Nothing ever fails
///
/// We set the strategy to do no retries at all, to check it is not used.
#[test]
fn no_fail() {
    let scenario = Scenario::new(vec![FactorySuccess, LogSuccess, LogSuccess]);
    let root = logger(scenario, strategy(0), true);
    info!(root, "Msg 1");
    info!(root, "Msg 2");
}

/// Like `no_fail`, but with delayed creation
#[test]
fn no_fail_delayed() {
    let scenario = Scenario::new(vec![FactorySuccess, LogSuccess, LogSuccess]);
    let root = logger(scenario, strategy(0), false);
    info!(root, "Msg 1");
    info!(root, "Msg 2");
}

/// When we ask for it, it creates a connection eagerly.
#[test]
fn connect_now() {
    let scenario = Scenario::new(vec![FactorySuccess]);
    let _root = logger(scenario, strategy(0), true);
}

/// No connection is made if nothing is logged.
#[test]
fn no_connect() {
    let scenario = Scenario::new(vec![]);
    let _root = logger(scenario, strategy(0), false);
}

#[test]
fn retries() {
    let scenario = Scenario::new(vec![
        // First message
        FactoryError,
        FactorySuccess,
        LogError,
        FactorySuccess,
        LogSuccess,
        // Second message
        LogError,
        FactoryError,
        FactorySuccess,
        LogSuccess,
        // Third message
        LogError,
        FactoryError,
        FactorySuccess,
        LogError,
        FactorySuccess,
        LogSuccess,
    ]);
    let root = logger(scenario, strategy(3), false);
    info!(root, "Msg 1");
    info!(root, "Msg 2");
    info!(root, "Msg 3");
}

/// Gives up connecting at creation
#[test]
fn give_up_initial() {
    let scenario = Scenario::new(vec![FactoryError, FactoryError, FactoryError]);
    assert!(Retry::new(move || factory(&scenario), strategy(2), true).is_err());
}

/// Give up when logging. But try again on the next message.
#[test]
fn give_up() {
    let scenario = Scenario::new(vec![
        // Initial connect
        FactorySuccess,
        // Fail on first message
        LogError,
        FactoryError,
        FactoryError,
        // Second message
        FactorySuccess,
        LogSuccess,
    ]);
    let root = logger(scenario, strategy(2), true);
    info!(root, "Failed message");
    info!(root, "Successful message");
}

/// Give up when logging. But try again on the next message.
///
/// Checks the giving up works sane even when we delay the first connection and that one is
/// unsuccesfull when logging.
#[test]
fn give_up_delayed() {
    let scenario = Scenario::new(vec![
        // Initial connect
        FactorySuccess,
        // Fail on first message
        LogError,
        FactoryError,
        FactoryError,
        // Second message
        FactorySuccess,
        LogSuccess,
    ]);
    let root = logger(scenario, strategy(2), false);
    info!(root, "Failed message");
    info!(root, "Successful message");
}
