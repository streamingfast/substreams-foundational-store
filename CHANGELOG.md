# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- Optional internal gRPC listener via `--internal-addr` that authenticates with a configurable plugin (`--internal-auth-plugin`, default `trust://?allowed=x-organization-id,x-user-id,x-api-key-id`). This lets internal callers such as Substreams tier1 authorize hosted-store requests using forwarded trusted identity headers instead of an end-user JWT (which is consumed upstream and never reaches tier1). Organization scoping via `--organization-id` still applies to the trusted `x-organization-id` header.

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