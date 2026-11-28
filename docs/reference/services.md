# Services and operations

Every service is served on `:4566`. A request reaches its service by its `X-Amz-Target` header
(the JSON protocols), else by the service named in its SigV4 credential scope, else S3. Signatures
are never checked.

An operation that is not listed answers with an error that names it, such as `NotImplemented` for
`GetBucketPolicy`. Some optional request settings are accepted and have no effect:
server-side encryption, DynamoDB stream and tag settings on `CreateTable`, and consumed-capacity
reporting.

## SQS

JSON protocol (current SDKs) and query protocol (older SDKs and tools).

`ChangeMessageVisibility` · `ChangeMessageVisibilityBatch` · `CreateQueue` · `DeleteMessage` ·
`DeleteMessageBatch` · `DeleteQueue` · `GetQueueAttributes` · `GetQueueUrl` ·
`ListDeadLetterSourceQueues` · `ListQueues` · `ListQueueTags` · `PurgeQueue` · `ReceiveMessage` ·
`SendMessage` · `SendMessageBatch` · `SetQueueAttributes` · `TagQueue` · `UntagQueue`

**Covers:** standard and FIFO queues, delays, visibility timeouts, long polling, message and system
attributes, dead-letter queues, batches, retention, tags.

**Not emulated:** message move tasks (`StartMessageMoveTask`; the [web panel](../guides/web-panel.md)
can redrive a DLQ), permissions (`AddPermission`).

## Kinesis

`AddTagsToStream` · `CreateStream` · `DecreaseStreamRetentionPeriod` · `DeleteStream` ·
`DescribeStream` · `DescribeStreamSummary` · `GetRecords` · `GetShardIterator` ·
`IncreaseStreamRetentionPeriod` · `ListShards` · `ListStreams` · `ListTagsForStream` · `PutRecord` ·
`PutRecords` · `RemoveTagsFromStream` · `StartStreamEncryption` · `StopStreamEncryption`

**Covers:** streams and shards, every shard iterator type with expiry, `MillisBehindLatest`,
retention, tags, server-side encryption settings (stream status stays `ACTIVE` without an `UPDATING`
transition, and `StopStreamEncryption` clears encryption regardless of which `KeyId` is supplied).
Sequence numbers are 56 digits, like Kinesis's, and grow across streams of the same name: a checkpoint
of an earlier stream (deleted, or from before a restart without `-state-dir`) is refused with
`InvalidArgumentException`, as Kinesis does.

**Not emulated:** enhanced fan-out (`SubscribeToShard`, `RegisterStreamConsumer`), shard split and
merge, `UpdateShardCount`.

## S3

REST protocol, path-style and virtual-hosted-style (`<bucket>.localhost`, `<bucket>.s3.<domain>`).

| Buckets | Objects | Multipart |
|---|---|---|
| `ListBuckets` | `PutObject` | `CreateMultipartUpload` |
| `CreateBucket` | `GetObject` (ranges, conditions, `partNumber`) | `UploadPart` |
| `HeadBucket` | `HeadObject` | `UploadPartCopy` |
| `DeleteBucket` | `CopyObject` | `CompleteMultipartUpload` |
| `GetBucketLocation` | `DeleteObject` | `AbortMultipartUpload` |
| `Get`/`PutBucketVersioning` | `DeleteObjects` | `ListParts` |
| `Get`/`Put`/`DeleteBucketLifecycleConfiguration` | `ListObjects`, `ListObjectsV2` | `ListMultipartUploads` |
| `Get`/`PutBucketNotificationConfiguration` | `ListObjectVersions` | |
| `Get`/`Put`/`DeleteBucketTagging` | | |

**Covers:** versioning and delete markers, range and conditional requests, every checksum
algorithm (CRC32, CRC32C, CRC64NVME, SHA-1, SHA-256) with full-object and composite multipart
checksums, `aws-chunked` bodies with trailers, presigned URLs and their expiry, lifecycle expiry,
bucket tagging, event notifications to queues.

**Not emulated** (each answers `NotImplemented` with its name): ACLs, policies, CORS, object tagging,
encryption configuration, website, logging, ownership controls, public access block, object lock,
retention and legal hold, replication, `GetObjectAttributes`, `RestoreObject`,
`SelectObjectContent`.

## DynamoDB

`BatchGetItem` · `BatchWriteItem` · `CreateTable` · `DeleteItem` · `DeleteTable` · `DescribeTable` ·
`DescribeTimeToLive` · `GetItem` · `ListTables` · `PutItem` · `Query` · `Scan` ·
`TransactGetItems` · `TransactWriteItems` · `UpdateItem` · `UpdateTimeToLive`

**Covers:** the full expression language (conditions, filters, projections, updates), the legacy
parameters (`Expected`, `QueryFilter`, `ConditionalOperator`), local and global secondary indexes
with every projection, pagination, batches, transactions, TTL.

**Not emulated:** streams, `UpdateTable`, backups, consumed-capacity reporting, reserved-word
checks.

TTL deletes expired items every `-ttl-interval`, or on `POST /_stratus/expire-ttl`. Like DynamoDB,
it ignores a TTL attribute that is not a number, and one more than five years in the past.

## Step Functions

`CreateStateMachine` · `DeleteStateMachine` · `DescribeExecution` · `DescribeStateMachine` ·
`GetExecutionHistory` · `ListExecutions` · `ListStateMachines` · `SendTaskFailure` ·
`SendTaskHeartbeat` · `SendTaskSuccess` · `StartExecution` · `StopExecution` · `UpdateStateMachine`

**Covers:** Amazon States Language with `Pass`, `Task`, `Wait`, `Choice`, `Succeed` and `Fail`;
`InputPath`, `Parameters`, `ResultSelector`, `ResultPath`, `OutputPath`; the intrinsic functions
`States.Format`, `States.StringToJson`, `States.JsonToString`, `States.Array` and `States.UUID`;
`Retry` and `Catch`; timeouts and heartbeats; task tokens (`.waitForTaskToken`); the
`arn:aws:states:::sqs:sendMessage` integration.

**Not emulated:** `Parallel`, `Map`, `.sync` integrations, integrations other than SQS, JSONata,
activities, versions and aliases.
