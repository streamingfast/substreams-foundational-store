# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed
- Update the Remote Feed Hosted Store client guide: Graph Market create flow, API-key auth, endpoint warmup, and that a Substreams querying a not-ready store hangs until `SetReady`.

### Added
- Optional internal gRPC listener via `--internal-addr` that authenticates with a configurable plugin (`--internal-auth-plugin`, default `trust://?allowed=x-organization-id,x-user-id,x-api-key-id`). This lets internal callers such as Substreams tier1 authorize hosted-store requests using forwarded trusted identity headers instead of an end-user JWT (which is consumed upstream and never reaches tier1). Organization scoping via `--organization-id` still applies to the trusted `x-organization-id` header.

### Security
- Bump `google.golang.org/grpc` to v1.81.1 to fix authorization bypass via missing leading slash in `:path` (GHSA-p77j-4mvh-x3m3)
- Bump `golang.org/x/crypto` to v0.54.0 to fix multiple critical SSH advisories pulled in transitively by the grpc bump (GHSA-vgwf-h737-ff37, GHSA-89gr-r52h-f8rx, GHSA-rm3j-f69w-wqmq, GHSA-5cgq-3rg8-m6cv, GHSA-x527-x647-q7gg, GHSA-jppx-rxg9-jmrx, GHSA-f5wc-c3c7-36mc)

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