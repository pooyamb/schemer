# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).


<!-- next-header -->
## [Unreleased]
### Changed
- `RusqliteAdapter` is now generic over an error type `E` so that migrations can return error types other than `rusqlite::Error`.
- Migrated error handling from `failure` to `thiserror`.
- Updated crate to Rust 2018 Edition.


<!-- next-url -->
[Unreleased]: https://github.com/aschampion/schemer/compare/schemer-rusqlite=v0.1.0...HEAD