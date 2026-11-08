// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A session error carries the session's error in its message, and the error chain goes on from
//! that error's own cause: nothing of it is lost, and no reporter prints it twice.

use std::error::Error;
use std::{fmt, io, iter};

use grammers_mtsender::InvocationError;

/// A session storage's error that names what it was doing and leaves its cause to `source()`.
#[derive(Debug)]
struct StoreError(io::Error);

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("storing the session failed")
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

/// The message of every error in `error`'s chain, outermost first, as a chain-aware reporter
/// (`anyhow`'s `{:#}`, `std::error::Report`) prints them.
fn chain(error: &(dyn Error + 'static)) -> Vec<String> {
    iter::successors(Some(error), |&error| error.source())
        .map(ToString::to_string)
        .collect()
}

#[test]
fn session_error_keeps_its_cause() {
    let error = InvocationError::from(
        Box::new(StoreError(io::Error::other("disk full"))) as Box<dyn Error + Send + Sync>
    );
    assert_eq!(
        chain(&error),
        ["session error: storing the session failed", "disk full"]
    );
    // The cause is the session error's own, so a caller can still look for it by type.
    let cause = error.source().unwrap();
    assert!(cause.downcast_ref::<io::Error>().is_some());
}

#[test]
fn session_error_that_renders_its_cause_says_it_once() {
    // As grammers' own errors do: the cause in the message, and no `source()`.
    let error = InvocationError::Session("no datacenter 9 in the session".into());
    assert_eq!(
        chain(&error),
        ["session error: no datacenter 9 in the session"]
    );
}
