# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.2.3

>[!NOTE] Versions before v0.2.3 may have caused missing data in your database, it is recommented that you reprocess it.

* Persist the last saved block number to a file on every flush, so cold restarts resume from the correct point

## v0.2.2

### Added
- `--addr` flag now accepts a comma-separated list of listen addresses, allowing the server to bind on multiple ports simultaneously (e.g. `:9000,*:9001`). A `*` in an address enables TLS using a built-in self-signed (snakeoil) certificate; without `*` the server uses plain-text gRPC.

## v0.2.1
- Fix issues with Posgresql and new Key format 

## v0.2.0

### Added
- v2 protobuf definitions for improved foundational store API (sf.substreams.foundational_store.service.v2 and sf.substreams.foundational_store.model.v2)
- Comprehensive documentation comments to all proto files in the proto/ directory
- Unit tests for the IfNotExist feature across all store implementations (badger, badger_time_traversal, postgres, postgres_time_traversal)
- Deprecation notices for proto service v1 with prominent warnings and migration guidance

### Fixed
- ForkAwareStore SetAll and Set methods now properly check both cache and underlying storage for key existence when IfNotExist=true
- Fixed interface compliance issues in sink tests by updating mock implementations to match the Store interface

## v0.1.0

* Initial first release
