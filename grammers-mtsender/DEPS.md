# Dependencies

## grammers-mtproto

Contains the actual implementation of the protocol without performing any IO. This crate's job
is to make use of said protocol over an actual network, and to coordinate sending messages.

## grammers-crypto

For the type of the servers' RSA keys, which the connection parameters carry to the generation of an
authorization key.

## grammers-tl-types

Used to be able to execute certain protocol functions and to refer to the items produced by it.

## grammers-session

Update handling is very spread across Telegram's entire protocol.
Notable exceptions are types which aren't updates but affect them,
and partial updates that depend on the request that produced them.

Depending on the session means the bulk of that logic can remain separate.

## os_info

Telegram requires clients to send some basic system information when connecting to the server,
such as OS type or system version. If these values are not explicitly provided by the user, the
crate is used to load the expected values. Optional, behind the default `os-detect` feature.

## locate-locale

Similar rationale to `os_info`, Telegram expects a system language code used by the client
(presumably for things such as localized service messages among others). Optional, behind the
default `os-detect` feature.

## tokio

Used for its asynchronous `TcpStream` in the default dialer, its I/O traits for the streams any
dialer returns, its timers, and its channels, which communicate with the sender. The tests pause its
clock (`test-util`) to fire the ping timer without waiting.

## bytes

Used for input and output buffers.

## tracing

Used to log what's going on during the lifetime of the sender.

## tracing-subscriber

Used in the tests in order to debug with more information when things go wrong.

## toml

Used to test that this file lists all dependencies from `Cargo.toml`.

## url

Used to parse the optional proxy URL.

## percent-encoding

Used to decode the proxy URL's username and password, which `url` leaves percent-encoded.

## hickory-resolver

Used to look up the IP address of the proxy host if a domain is provided.

## tokio-socks

SOCKS5 proxy support.

## socks5-server

Used to test for SOCKS5 proxy support.

## sha2

Used in the tests to play a server that holds an authorization key: the server side of MTProto 2.0's
message encryption, which `grammers-crypto` implements only for the client.
