#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_json;
extern crate slog_retry;

use std::io::Result as IoResult;
use std::net::TcpStream;

use slog::Drain;
use slog_json::Json;

fn create_logger() -> IoResult<Json<TcpStream>> {
    let connection = TcpStream::connect("127.0.0.1:1234")?;
    let drain = slog_json::Json::new(connection)
        .add_default_keys()
        .set_newlines(true)
        .build();
    Ok(drain)
}

fn main() {
    let retry = slog_retry::Retry::new(create_logger, None, true)
        // Kill the application if the initial connection fails
        .unwrap()
        // Ignore if it isn't possible to log some of the messages, we'll try again
        .ignore_res();
    let async = slog_async::Async::default(retry)
        .fuse();
    let root = slog::Logger::root(async, o!(
        "version" => env!("CARGO_PKG_VERSION"),
        "application" => env!("CARGO_PKG_NAME"),
    ));
    info!(root, "Everything is set up");
}
