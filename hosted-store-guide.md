# Hosted Store — Client Guide

A **Hosted Store** is a managed, block‑aware key/value store that StreamingFast runs for
you. You write key/value entries into it, and your Substreams modules — or any gRPC
client — read those entries back at a given block height.

This guide shows you how to:

1. Understand what a hosted store is and when to use it
2. Create one and get its endpoint
3. Authenticate
4. Write entries into it (`Feed.Set`)
5. Read entries back, either directly (`Store.Get` / `Store.GetFirst`) or from a Substreams module
6. Look up the proto reference

---

## 1. Concepts

A hosted store is a **key → value** map with a few blockchain‑specific properties:

- **Keys** are raw `bytes` — they can be a string, a hash, an address, or any composite key you choose.
- **Values** are a `google.protobuf.Any` — i.e. any protobuf message you define, tagged with its `@type`.
- **Block‑aware reads** — every read is performed *at a block number*. The store tells you whether it has reached that block yet (`block_reached`), so your reads stay deterministic alongside the chain.
- **Readiness** — a store can be marked not‑ready while it's still being populated; reads then return `block_reached = false` until you flip it ready.

You populate a hosted store by writing entries directly over gRPC with the `Feed.Set` call
(the **remote feed** flow). Data written this way is treated as **already final**. Reading is
the same whether you query the store directly or from a Substreams module.

---

## 2. Create a hosted store

Hosted stores are provisioned through [**The Graph Market**](http://thegraph.market), under the
**Hosted Sink** section:

- [https://thegraph.market/sinks/new](https://thegraph.market/sinks/new) — create a new sink. Click the **Hosted Store** button.
- [https://thegraph.market/sinks](https://thegraph.market/sinks) — list your sinks and copy a store's sink ID (the deployment ID).

Creating a hosted store only requires two fields:

- **Name** — a label for the store.
- **Type URL** — the protobuf type of the values you'll store (e.g. `sf.substreams.example.v1.TestValue`).

When you create one you get back a **deployment ID** (a UUID, e.g. `fdfa1b15-8b20-438c-a0b7-41278b4fdacf`). That ID is used in two places:

- as the **hostname** of the store's gRPC endpoint, and
- as the **store reference** in a Substreams manifest.

### Endpoint convention

Once deployed, the store is reachable at:

```
<deployment-id>.hs.streamingfast.io:443
```

- TLS on port **443**, gRPC over HTTP/2.
- Authenticated with a **Bearer JWT** (see next section).

---

## 3. Authentication

Calls to the store carry a StreamingFast **JWT** in the gRPC `authorization` metadata:

```
authorization: Bearer <jwt>
```

An API key for the hosted store is **created automatically** when you create the store — you don't
have to make one yourself.

You get your API token (JWT) from [https://thegraph.market/api-keys](https://thegraph.market/api-keys):
find the key and click its **Refresh** button to mint a fresh token.

---

## 4. Write entries — `Feed.Set`

The remote‑feed ingest service is `sf.substreams.foundational_store.feed.v2.Feed`. Its `Set`
method writes a batch of entries. Data written this way is treated as **final** and persisted
immediately (latest‑value semantics, no fork handling).

### Entry shape

```protobuf
message SinkEntries {
  repeated Entry entries = 1;
  bool if_not_exist = 2;   // if true, skip keys that already exist
}

message Entry {
  Key key = 2;                  // key.bytes — raw bytes
  google.protobuf.Any value = 4; // any protobuf message, tagged with @type
}
```

### Example with `grpcurl`

This writes a single entry — key `test-key-1` (base64‑encoded bytes), value a
`sf.substreams.example.v1.TestValue`:

```bash
grpcurl \
  --import-path "./proto" \
  --proto sf/substreams/foundational-store/feed/v2/feed.proto \
  --proto sf/substreams/foundational-store/model/v2/model.proto \
  --proto sf/substreams/example/v1/example.proto \
  -H "authorization: Bearer $SF_TOKEN" \
  -d '{
    "entries": {
      "entries": [{
        "key": { "bytes": "dGVzdC1rZXktMQ==" },
        "value": {
          "@type": "type.googleapis.com/sf.substreams.example.v1.TestValue",
          "value": "my_super_value"
        }
      }]
    }
  }' \
  <deployment-id>.hs.streamingfast.io:443 \
  sf.substreams.foundational_store.feed.v2.Feed/Set
```

Notes:

- `key.bytes` is **base64** in JSON (gRPC bytes encoding). `dGVzdC1rZXktMQ==` decodes to `test-key-1`.
- `value` is a `google.protobuf.Any`: the `@type` must be the fully‑qualified type URL of your value message, and the remaining fields are that message's fields.
- Use `"if_not_exist": true` at the `entries` level to avoid overwriting existing keys.

### Marking the store ready

While a store is being populated you can hold reads off with `Feed.SetReady`:

```bash
grpcurl ... -d '{ "ready": true }' \
  <endpoint> sf.substreams.foundational_store.feed.v2.Feed/SetReady
```

Until `ready = true`, `Get`/`GetFirst` return `block_reached = false`. A Substreams module that
queries a store marked as not ready will **wait** until the store is made ready before proceeding.

---

## 5. Read entries

### Option A — query the store directly (`Store.Get`)

The query service is `sf.substreams.foundational_store.service.v2.Store`. Reads are performed
**at a block** and return one result per requested key, in order.

```protobuf
message GetRequest {
  uint64 block_number = 1;        // query at this block
  bytes  block_hash   = 2;        // optional, for fork verification
  repeated Key keys   = 3;        // keys to look up
}

message GetResponse {
  bool block_reached = 1;         // false if the store hasn't reached block_number yet
  QueriedEntries entries = 2;     // one QueriedEntry per requested key
}
```

Example:

```bash
grpcurl \
  --import-path "./proto" \
  --proto sf/substreams/foundational-store/service/v2/service.proto \
  --proto sf/substreams/foundational-store/model/v2/model.proto \
  -H "authorization: Bearer $SF_TOKEN" \
  -d '{
    "block_number": "100",
    "keys": [{ "bytes": "dGVzdC1rZXktMQ==" }]
  }' \
  <deployment-id>.hs.streamingfast.io:443 \
  sf.substreams.foundational_store.service.v2.Store/Get
```

- **`Get`** returns exact key matches.
- **`GetFirst`** returns, for each requested key, the first key **≥** it (lexicographic order) — useful for range scans / "next key" lookups.

### Response codes

Each `QueriedEntry` carries a `code`:

| Code | Meaning |
|------|---------|
| `RESPONSE_CODE_FOUND` (1) | Key exists; `entry` holds the value. |
| `RESPONSE_CODE_NOT_FOUND` (2) | Key does not exist at the requested block. |
| `RESPONSE_CODE_NOT_FOUND_FINALIZE` (4) | Key was deleted after finality (historical reference). |
| `RESPONSE_CODE_UNSPECIFIED` (0) | Should not occur. |

Always check `block_reached` first: if it's `false`, the store hasn't ingested the block you
asked about yet (or it isn't marked ready), and a `NOT_FOUND` doesn't mean the key is truly absent.

### Option B — read from a Substreams module

This is the most common consumption path: your Substreams module declares the hosted store as
an input and queries it per block.

#### 1. Declare it in `substreams.yaml`

Add the store as a module input using `foundational-store: <deployment-id>@<version>`:

```yaml
modules:
  - name: map_query_test_store
    kind: map
    initialBlock: 1
    inputs:
      - source: sf.acme.type.v1.Block
      - foundational-store: <deployment-id>@v0.1.0
    output:
      type: proto:sf.substreams.example.v1.TestValue
```

> **Note:** the `@<version>` suffix (e.g. `@v0.1.0`) is **ignored by the system** — the store is
> resolved by its deployment ID alone — but it must be present to pass manifest validation.

Add the proto dependency in `buf.yaml` so the hosted store types are available:

```yaml
deps:
  - buf.build/streamingfast/substreams
  - buf.build/streamingfast/substreams-foundational-store
  - buf.build/googleapis/googleapis
```

#### 2. Query it from Rust

The store is injected into your handler as a `FoundationalStore`. Call `get` with a slice of
keys; you get back a `QueriedEntries`.

```rust
use substreams::errors::Error;
use substreams::store::FoundationalStore;
use substreams::pb::sf::substreams::foundational_store::model::v2::ResponseCode;

mod pb;
use pb::sf::substreams::example::v1::TestValue;

#[substreams::handlers::map]
fn map_query_test_store(
    _block: pb::sf::acme::r#type::v1::Block,
    store: FoundationalStore,
) -> Result<TestValue, Error> {
    let key: &[u8] = b"test-key-1";
    let response = store.get(&[key]);

    let value = if let Some(entry) = response.entries.first() {
        let bytes = entry
            .entry
            .as_ref()
            .and_then(|e| e.value.as_ref().map(|v| v.value.clone()))
            .unwrap_or_default();

        if entry.code == ResponseCode::Found as i32 {
            String::from_utf8_lossy(&bytes).into_owned()
        } else {
            format!("code={}", entry.code)
        }
    } else {
        "NO_ENTRY".to_string()
    };

    Ok(TestValue { value })
}
```

The read is automatically performed at the block currently being processed, so results stay in
sync with the stream.

#### 3. Build and run

```bash
substreams build
substreams run substreams.yaml map_query_test_store \
  -e <substreams-endpoint>
```

(During local development against a dev stack you might use
`-e localhost:9088 --insecure --limit-processed-blocks=0`.)

---

## 6. End‑to‑end checklist

1. **Create** a remote‑feed hosted store in [The Graph Market](http://thegraph.market) → note the **deployment ID**.
2. **Get a JWT** from your API key.
3. **Write** your entries with `Feed.Set` (and `Feed.SetReady` when ready to be consumed).
4. **Read** them either directly with `Store.Get` / `Store.GetFirst`, or by adding
   `foundational-store: <deployment-id>@<version>` to a Substreams module and calling
   `store.get(...)`.

---

## 7. Proto reference

All messages live under `sf.substreams.foundational_store.*` (package
`substreams-foundational-store`).

### Model — `model/v2/model.proto`

```protobuf
message Key { bytes bytes = 1; }
message Keys { repeated Key keys = 1; }

message Entry {
  Key key = 2;
  google.protobuf.Any value = 4;
}

message SinkEntries {
  repeated Entry entries = 1;
  bool if_not_exist = 2;
}

message QueriedEntry {
  ResponseCode code = 1;
  Entry entry = 2;
}
message QueriedEntries { repeated QueriedEntry entries = 2; }

enum ResponseCode {
  RESPONSE_CODE_UNSPECIFIED = 0;
  RESPONSE_CODE_FOUND = 1;
  RESPONSE_CODE_NOT_FOUND = 2;
  RESPONSE_CODE_NOT_FOUND_FINALIZE = 4;
}
```

### Feed (write) — `feed/v2/feed.proto`

```protobuf
service Feed {
  rpc Set(SetRequest) returns (SetResponse);            // write a batch of entries (final)
  rpc SetReady(SetReadyRequest) returns (SetReadyResponse); // toggle read readiness
}

message SetRequest { model.v2.SinkEntries entries = 1; }
message SetResponse {}
message SetReadyRequest { bool ready = 1; }
message SetReadyResponse {}
```

### Store (read) — `service/v2/service.proto`

```protobuf
service Store {
  rpc Get(GetRequest) returns (GetResponse);       // exact key match
  rpc GetFirst(GetRequest) returns (GetResponse);  // first key >= requested
}

message GetRequest {
  uint64 block_number = 1;
  bytes  block_hash   = 2;
  repeated model.v2.Key keys = 3;
}
message GetResponse {
  bool block_reached = 1;
  model.v2.QueriedEntries entries = 2;
}
```
