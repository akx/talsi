# Talsi

Talsi is a SQLite-backed key-value storage library for Python, written in Rust (so you can tell it's
obviously blazing fast).

It grew out of my repeated need for a key-value storage library for various projects over the years
that have needed to store (temporary) data of some sort, and writing the same SQLite stuff over and
over again was getting old. Also, it's a good excuse to write some Rust + PyO3 code.

> [!NOTE]
> The project is pretty raw and new still, so things (e.g. the API) are likely to change.
> Hopefully for the better, though.

## Features

* Namespaced key-value storage. Keys are UTF-8, values can be anything (either pickleable or JSONable).
* Fast, thanks to SQLite and Rust.
* API support for multi-set/multi-get/multi-delete operations
* Transparent compression for large data (Snappy at present).
* Support for [`orjson`](https://github.com/ijl/orjson/); if it is installed in your Python environment,
  it will be used for fast JSON (de)serialization.

## Usage

For the time being, please see the tests in `tests/` for usage examples.

## License

* Licensed under the MIT license. See the `LICENSE` file for details.
* Builds embed SQLite, [which is in the public domain](https://sqlite.org/copyright.html)
