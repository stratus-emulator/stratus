<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo/stratus-mark-dark.svg">
    <img src="assets/logo/stratus-mark.svg" alt="stratus" width="112">
  </picture>
</p>

<h1 align="center">stratus</h1>

<p align="center">
  <a href="https://github.com/stratus-emulator/stratus/actions/workflows/ci.yml"><img src="https://github.com/stratus-emulator/stratus/actions/workflows/ci.yml/badge.svg" alt="ci"></a>
  <a href="https://hub.docker.com/r/stratusemulator/stratus"><img src="https://img.shields.io/docker/v/stratusemulator/stratus?sort=semver&label=docker" alt="Docker Hub"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License: Apache 2.0"></a>
  <a href="https://github.com/stratus-emulator/stratus/discussions"><img src="https://img.shields.io/github/discussions/stratus-emulator/stratus?label=discussions" alt="Discussions"></a>
  <a href="https://github.com/sponsors/ODudek"><img src="https://img.shields.io/badge/sponsor-%E2%9D%A4-db61a2" alt="Sponsor"></a>
</p>

```sh
docker run --rm -p 4566:4566 stratusemulator/stratus
```

<p align="center"><img src="assets/screenshots/panel-tour.gif" alt="The stratus web panel: the overview, a Kinesis stream whose KPL-aggregated records are decoded, and a DynamoDB item being edited" width="960"></p>

A local emulator of the AWS services an application needs in development and
end-to-end tests. One static Go binary, ready in milliseconds; S3 object bytes are kept on disk,
all other state in memory.

- **One port, no signature checks.** Every service is served on `:4566`. A request is routed by
  its `X-Amz-Target` header or, failing that, by the service in its SigV4 credential scope. The
  signature itself is never verified, so any credentials work, including a placeholder written by
  hand in a test tool.
- **Resources are declared, not scripted.** A YAML manifest lists what exists at start. The
  emulator is ready only once all of it exists, and a key it does not understand stops the start.
- **Built for tests.** `POST /_stratus/reset` returns to the manifest's state between suites, and
  the `stratustest` package runs the emulator inside a Go test with no container.

## Services

| Service | Covers | Not emulated |
|---|---|---|
| **SQS** | standard and FIFO queues, delays, visibility timeouts, long polling, message attributes, dead-letter queues, batch operations, retention, tags; JSON and query protocols | message move tasks (redrive) |
| **Kinesis** | streams and shards, `PutRecord`/`PutRecords`, every shard iterator type with expiry (a checkpoint of an earlier stream of the name, deleted or from before a restart, is refused as `InvalidArgumentException`), `GetRecords` with `MillisBehindLatest`, retention, tags | enhanced fan-out (`SubscribeToShard`), shard split and merge, encryption |
| **S3** | objects, multipart uploads, versioning, range and conditional requests, all checksum algorithms, `aws-chunked` bodies, presigned URLs, lifecycle expiry, object tagging, event notifications to queues | ACLs, policies, bucket tagging, object lock, replication |
| **DynamoDB** | tables, items, `Query`/`Scan`, the full expression language, local and global secondary indexes, batches, transactions, TTL | streams, `UpdateTable` |
| **Step Functions** | Amazon States Language with `Pass`, `Task`, `Wait`, `Choice`, `Succeed`, `Fail`, `Retry`/`Catch`, task tokens; the `sqs:sendMessage` integration | `Parallel`, `Map`, `.sync` integrations, JSONata, activities |

An operation that is not emulated answers with an error that names it. Some optional request
settings are accepted and have no effect: server-side encryption, and DynamoDB stream and tag settings
on `CreateTable`.

Time-driven clean-up runs in the background only when asked to, so tests stay deterministic. SQS
drops a message past its retention when its queue is next used. DynamoDB TTL deletes expired items
every `-ttl-interval` (`stratustest.WithTTLInterval`), or once on `POST /_stratus/expire-ttl`
(`Server.ExpireTTL`); without either, expired items stay, as in LocalStack by default. Like
DynamoDB it ignores a TTL attribute that is not a number, and one more than five years in the past.
S3 lifecycle expiry and Kinesis trimming do nothing until a test asks for them through the Go API.

S3 keeps each object's bytes in a file of a directory of its own, created at start under
`-data-dir` and removed on exit, so large objects do not hold memory; a file goes once no version
refers to it. Without `-data-dir` that directory is in the user's cache directory, else `/var/tmp`,
never `/tmp`, which is a tmpfs held in memory on many systems. The Docker image uses `/data`;
mount a volume there to keep the bytes off the container's writable layer. Without `-state-dir`
nothing survives a restart: buckets and object metadata are in memory. With it, data survives
restarts: [Keeping data across restarts](#keeping-data-across-restarts).

## Run

The image is on Docker Hub as `stratusemulator/stratus` and on GHCR as
`ghcr.io/stratus-emulator/stratus`, for `linux/amd64` and `linux/arm64`. Tags: a release's version
(`1.2.3`, `1.2`, `1`), `latest` for the newest release, `edge` for `main`.

```sh
docker run --rm -p 4566:4566 \
  -v "$PWD/examples/manifest.yaml:/manifest.yaml:ro" \
  stratusemulator/stratus -manifest /manifest.yaml
```

The image runs as an unprivileged user (uid 65532) and keeps S3 object bytes in `/data`. A
named or anonymous volume there works as it is (`-v stratus-data:/data`); a directory of the host
needs to be writable by that uid, or run the container with `--user "$(id -u)"`.

From source, with Go:

```sh
go install github.com/stratus-emulator/stratus/cmd/stratus@latest
stratus -manifest examples/manifest.yaml
```

Point a client at it with any credentials:

```sh
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=eu-central-1
aws --endpoint-url http://localhost:4566 sqs list-queues
```

| Flag | Default | Meaning |
|---|---|---|
| `-listen` | `:4566` | address to listen on |
| `-version` | | print the version and exit |
| `-manifest` | none | YAML file with the resources to create at start |
| `-region` | `eu-central-1` | region of every resource |
| `-account` | `000000000000` | account id of every resource |
| `-healthcheck` | | check that a running stratus on `-listen` is ready, then exit |
| `-ttl-interval` | `0` (off) | how often to delete expired DynamoDB items in the background, for example `1m` |
| `-data-dir` | user cache dir, else `/var/tmp` | existing directory under which S3 object bytes are kept, in a directory removed on exit |
| `-state-dir` | none | directory to keep data in across restarts, S3 object bytes included; the manifest's `persistence` says which resources |
| `-state-interval` | `10s` | how often to save the state to `-state-dir`, besides on exit |
| `-tls-listen` | none | address to also serve HTTPS on, for example `:4567` |
| `-tls-cert`, `-tls-key` | none | certificate and key for HTTPS; a self-signed pair is generated when empty |
| `-tls-hosts` | `localhost,127.0.0.1,::1` | names and addresses a generated certificate is valid for |
| `-tls-cert-out` | none | file to write a generated certificate to, for clients to trust |

HTTPS matters for one case: the AWS SDKs upload a request body they cannot rewind (a pipe, a
network stream) only over HTTPS. Over plain HTTP `PutObject` needs a seekable body, or the
transfer manager.

The image has no shell, so a container health check calls the binary:

```yaml
healthcheck:
  test: ["CMD", "/stratus", "-healthcheck"]
  interval: 1s
```

## Manifest

```yaml
sqs:
  queues:
    - name: orders
      visibilityTimeoutSeconds: 60
      deadLetter: {queue: orders-dlq, maxReceiveCount: 5}
    - name: orders-dlq
    - name: uploads
    - name: ledger.fifo            # a name ending in .fifo makes a FIFO queue
      contentBasedDeduplication: true

kinesis:
  streams:
    - name: events
      shards: 2                    # default 1

s3:
  buckets:
    - name: packages
      versioning: true
      lifecycle:
        - {prefix: tmp/, expirationDays: 7}
      notifications:               # s3:ObjectCreated:* unless events are listed
        - {queue: uploads, prefix: incoming/, suffix: .zip}

dynamodb:
  tables:
    - name: orders
      partitionKey: {name: customer}            # type S unless given
      sortKey: {name: order_id, type: N}
      ttlAttribute: expires_at
      localIndexes:
        - name: by-status
          sortKey: {name: status}
          projection: INCLUDE                   # ALL (default), KEYS_ONLY or INCLUDE
          nonKeyAttributes: [total]

stepFunctions:
  stateMachines:
    - name: delayed-notification
      definitionFile: delayed-notification.asl.json   # relative to the manifest; or `definition:` inline
      substitutions:                                  # replaces ${QueueUrl} in the definition
        QueueUrl: http://localhost:4566/000000000000/orders
```

Resources may be listed in any order. An unknown key, a duplicate name, or a reference to a queue
that is not declared is an error at start. `examples/manifest.yaml` is a complete one.

## Keeping data across restarts

By default stratus starts from the manifest every time, which suits tests. A local stack that lives
for days next to a database with a volume needs its queues, tables, streams and objects to survive a
restart too. Give stratus a state directory, a volume in Docker:

```sh
docker run -p 4566:4566 -v stratus-state:/data stratusemulator/stratus:0.2.0 \
  -manifest /work/manifest.yaml -state-dir /data/state
```

stratus saves the data every `-state-interval` (10 s) and on exit (`docker stop` sends SIGTERM),
one JSON file per service, each replaced at once so that a crash leaves either the last save or the
one before. S3 object bytes stay in files under the directory. At start it brings the data back,
then applies the manifest over it:

- a resource the manifest adds is created; one it no longer lists stays;
- a setting that can change in place is taken from the manifest: queue attributes, dead-letter
  links, bucket versioning, lifecycle and notifications, a table's TTL attribute, a state machine's
  definition;
- a change that cannot be made in place, such as a table's keys or indexes, a stream's shard count
  or a state machine's type, stops the start and says which resource to delete;
- Kinesis sequence numbers go on where they stopped, so a consumer's checkpoint stays valid;
- a running Step Functions execution resumes in the state it was in, and a `Wait` ends when it
  would have. A Task that was waiting is called again.

The manifest says which resources keep their data, at three levels; the nearest one decides:

```yaml
persistence: true            # every resource (the default, when -state-dir is given)
sqs:
  persistence: false         # every queue...
  queues:
    - name: orders
      persistence: true      # ...but this one
    - name: scratch          # not kept: empty after every restart
```

A resource created through the API, not declared in the manifest, follows its service. Without
`-state-dir` the setting is ignored, so the same manifest serves tests and a long-lived stack. The
state of a resource the manifest stops keeping is dropped at the next start. `POST /_stratus/save`
saves at once; `POST /_stratus/reset` empties every resource, and the next save writes that.

## Control endpoints

| Endpoint | Meaning |
|---|---|
| `GET /_stratus/ready` | `200` once the manifest's resources exist, with the list of services |
| `POST /_stratus/reset` | drop all state and create the manifest's resources again |
| `POST /_stratus/expire-ttl` | delete the DynamoDB items past their TTL now; answers `{"deleted": n}` |
| `POST /_stratus/save` | save the state to `-state-dir` now; does nothing without one |
| `GET /_localstack/health` | the same answer in LocalStack's shape, for existing health checks |
| `GET /_stratus/ui` | the web panel, below |

## Web panel

Open `http://localhost:4566/_stratus/ui` in a browser. The overview shows a card per queue,
stream, bucket, table and state machine with its counts, refreshed every 2 seconds, and creates
resources of each kind. **Reset** returns to the manifest's state, **Expire TTL** deletes expired
DynamoDB items.

<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/screenshots/panel-overview-light.png">
  <img src="assets/screenshots/panel-overview-dark.png" alt="The overview of the web panel: a card per queue, stream, bucket, table and state machine with its counts">
</picture>

A resource's name opens its page: its settings, a search suited to the service, its operations and
its data, refreshed every 3 seconds (not while you edit it).

| Resource | Settings | Search | Change |
|---|---|---|---|
| Queue | attributes, dead-letter queue and its sources | text in bodies and attributes | send; edit (a new message replaces the old: SQS cannot change one); delete; purge; redrive a DLQ to its source |
| Stream | ARN, mode, retention, shards with hash ranges and record counts | partition key, shard, text in the decoded data | put a record (records cannot be changed or deleted) |
| Bucket | versioning, lifecycle rules, notifications | key prefix and text | upload a file or store text; edit a text object; delete; empty |
| Table | key schema, indexes, TTL, item count and size | partition key value, in the table or an index; text | put, edit and delete items as DynamoDB JSON |
| State machine | ARN, type, role, definition | execution status and name | start an execution; stop a running one |

Every resource can also be deleted from its page. Changes go through the emulator's own AWS API,
so they are validated and have their usual effects, such as S3 notifications to queues.

**Kinesis data is decoded.** A record aggregated by the Kinesis Producer Library is split into its
user records, each with its partition key; gzip and zlib payloads are decompressed (up to 1 MiB)
and JSON is indented, so a record reads `KPL → gzip → JSON`. A search by partition key finds an
aggregate by the keys inside it. zstd is recognised but not decompressed. The same decoding shows
S3 objects, SQS bodies and execution input and output.

| A Kinesis record aggregated by the KPL, found by an inner partition key | A DynamoDB item, edited in place |
|---|---|
| ![Three gzip-compressed JSON records decoded from one KPL aggregate](assets/screenshots/panel-kinesis-kpl.png) | ![The items of a table searched by partition key, one of them open for editing](assets/screenshots/panel-dynamodb-edit.png) |

The panel is part of the binary (server-rendered with [htmx](https://htmx.org) 2.0.4, embedded),
needs no network, and has no login, like the rest of the endpoint: anyone who can reach the port
can use it. Its changes only accept requests sent by the panel itself (the `HX-Request` header),
so a page on another site cannot make them in a browser that has the panel open. Downloads are
sent as attachments, so an uploaded page never runs in the panel.

## In a Go test

```go
srv := stratustest.Start(t, stratustest.WithManifestFile("testdata/manifest.yaml"))

client := sqs.New(sqs.Options{
    Region:       stratustest.Region,
    BaseEndpoint: aws.String(srv.URL),
    Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
})
```

The emulator stops with the test. `srv.Reset()` returns it to the manifest's state.
`stratustest.WithTLS()` serves it over HTTPS; pass `srv.Client()` to the AWS client as its
`HTTPClient`.

## In a test with Testcontainers

The [`testcontainers`](testcontainers) module runs stratus in a container from any Go test, with the
resources the test declares, in well under a second:

```go
ctr, err := stratus.Run(ctx, stratus.DefaultImage, stratus.WithManifestYAML(manifest))
testcontainers.CleanupContainer(t, ctr)
endpoint, err := ctr.BaseEndpoint(ctx) // BaseEndpoint for any AWS SDK client
```

Coming from the LocalStack module, its [README](testcontainers/README.md#coming-from-the-localstack-module)
maps one to the other.

## Compared with LocalStack and moto

All three in Docker with 1 CPU and 2 GB each, started from the same manifest, driven by the AWS SDK
for Go, in one run. Method, every operation and its caveats: [`bench/README.md`](bench/README.md);
rerun with `bench/run.sh`.

| | LocalStack (`community-archive`) | moto (`5.2.6`, server) | stratus |
|---|---|---|---|
| Ready with 63 resources | 10.30 s | 6.05 s | **0.13 s** |
| Memory once ready | 646 MiB | 90 MiB | **4 MiB** |
| Image | 1.81 GB | 578 MB | **16.1 MB** |
| DynamoDB PutItem, 32 clients | 235 ops/s, p99 1 093 ms | 679 ops/s¹, p99 72 ms | **19 222 ops/s, p99 17 ms** |
| S3 GetObject 4 KiB, 32 clients | 1 300 ops/s, p99 51 ms | 582 ops/s¹, p99 82 ms | **13 490 ops/s, p99 17 ms** |
| S3 PutObject 256 KiB, 32 clients | 539 ops/s, **p99 106 ms** | 310 ops/s, p99 229 ms | **832 ops/s**, p99 125 ms |
| SQS SendMessage, 32 clients | 1 176 ops/s, p99 92 ms | 78 ops/s, p99 483 ms | **23 268 ops/s, p99 2.9 ms** |
| Kinesis PutRecord, 32 clients | 119 ops/s, p99 812 ms | 580 ops/s, p99 87 ms | **17 823 ops/s, p99 6.2 ms** |

¹ Some of moto's calls failed (dropped connections) and are not counted.

This compares speed, not behaviour: see each service's "Not emulated" column above. moto mocks AWS
for unit tests; its server mode is a side use.

## Develop

```sh
make test   # go test -race ./...
make lint   # gofmt and go vet
```

Every service is tested through the real AWS SDK for Go, in its own package and end to end in
`stratustest/`: a behaviour is considered emulated when the SDK accepts it, including its typed
errors. None of it has been compared against live AWS yet.

Layout: `cmd/stratus` is the binary, `internal/gateway` routes requests, each service lives in
`internal/<service>`, `internal/manifest` reads the resource manifest and `internal/server` puts
them together.

## Contributing

Questions and ideas are welcome in [Discussions](https://github.com/stratus-emulator/stratus/discussions).
Issues and pull requests are welcome: read the
[contributing guide](https://github.com/stratus-emulator/.github/blob/main/CONTRIBUTING.md) first,
and report security problems privately as its [security policy](https://github.com/stratus-emulator/.github/blob/main/SECURITY.md) says.

## Supporting the project

stratus is free and open source. If it saves you time, you can
[sponsor its maintainer](https://github.com/sponsors/ODudek), star the repository, or tell others about it.

## License

Apache License 2.0: see [LICENSE](LICENSE) and [NOTICE](NOTICE).
