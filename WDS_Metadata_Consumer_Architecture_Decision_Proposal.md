# WDS Metadata Consumer Architecture Decision Proposal

## Objective

Design a reliable, scalable, and maintainable consumer architecture for WDS that:

- Consumes document events from Kafka
- Stores all incoming events in WDS
- Supports retry within WDS
- Supports audit and replay
- Decouples Kafka consumption from metadata processing

---

# Option 1 – Kafka Consumer calls Metadata Service

## Architecture

```text
Kafka Topic
    │
    ▼
kafka-consumer-service
    │
    ▼
KAFKA_INBOX_EVENT
    │
    ▼
Call Metadata Update Service (API/Event)
    │
    ▼
Update Metadata
```

## Pros

- Clear separation of responsibilities
- Metadata service owns business logic
- Consumer remains lightweight

## Cons

- Creates runtime dependency
- If Metadata Service is unavailable, consumer must retry
- Retry logic becomes more complicated
- Tighter coupling between services

## Recommendation

Not preferred unless communication is asynchronous (Kafka/event-based).

---

# Option 2 – Inbox Pattern (Recommended)

## Architecture

```text
Kafka Topic
    │
    ▼
kafka-consumer-service
    │
    ▼
KAFKA_INBOX_EVENT
    │
    ▼
wds-metadata-update-service
    │
 (Scheduled Polling)
    │
    ▼
WDS Metadata Database
```

## Responsibilities

### kafka-consumer-service

- Consume Kafka messages
- Validate basic message format
- Store original event into Inbox table
- Commit Kafka offset

### wds-metadata-update-service

- Poll Inbox table
- Process metadata
- Retry failed events
- Update Inbox status
- Move permanently failed records to DLQ

---

## Inbox Status Flow

```text
RECEIVED
    │
    ▼
PROCESSING
    │
    ├──────────────┐
    │              │
    ▼              ▼
SUCCESS         RETRY
                   │
                   ▼
            Retry Count < Max ?
                   │
          ┌────────┴────────┐
          │                 │
          ▼                 ▼
       Retry Again         DLQ
```

---

## Retry Logic

Scheduler runs every few seconds.

Example query:

```sql
SELECT *
FROM KAFKA_INBOX_EVENT
WHERE STATUS IN ('RECEIVED','RETRY')
  AND NEXT_RETRY_TIME <= CURRENT_TIMESTAMP
FOR UPDATE SKIP LOCKED;
```

### Processing

1. Lock record
2. STATUS = PROCESSING
3. PROCESSING_NODE = current ECS task
4. Process metadata
5. SUCCESS → STATUS = SUCCESS
6. Failure → RETRY_COUNT++
7. Calculate NEXT_RETRY_TIME
8. Retry until MAX_RETRY
9. Move to DLQ

---

## Handling Pod Failure

Recommended columns:

| Column | Purpose |
|--------|---------|
| PROCESSING_NODE | ECS task currently processing |
| LOCKED_AT | Processing start time |
| LOCK_EXPIRES_AT | Lock timeout |
| RETRY_COUNT | Retry attempts |
| NEXT_RETRY_TIME | Next eligible retry |

If a pod crashes:

- STATUS remains PROCESSING
- LOCK_EXPIRES_AT eventually expires
- Another ECS task safely reprocesses the record

No dependency on pod name.

---

## Pros

- WDS owns retry logic
- Full audit trail
- Easy replay
- No dependency on Metadata Service availability
- Scales horizontally
- Enterprise Inbox Pattern

## Cons

- Requires polling
- Additional Inbox table
- Requires lock management

---

# Option 3 – Bridge Service

## Architecture

```text
Kafka Topic
    │
    ▼
kafka-consumer-service
    │
    ▼
KAFKA_INBOX_EVENT
    │
    ▼
inbox-dispatcher-service
    │
    ▼
wds-metadata-update-service
```

## Pros

- Separates polling from metadata logic
- Metadata service remains focused

## Cons

- Additional microservice
- More operational complexity
- Another point of failure
- Still requires retry and locking

## Recommendation

Not recommended unless multiple downstream services must be orchestrated.

---

# Final Recommendation

## Selected Architecture

```text
Kafka Topic
    │
    ▼
kafka-consumer-service
    │
    ▼
KAFKA_INBOX_EVENT
    │
    ▼
wds-metadata-update-service
    │
 (Scheduler + Retry Engine)
    │
    ▼
WDS Metadata Database
```

## Why

- Full control of retries inside WDS
- Inbox Pattern provides audit and replay
- Metadata service remains focused on business logic
- Consumer remains lightweight
- No synchronous dependency between services
- Reliable and scalable on AWS ECS

---

# Future Enhancements

- Manual replay API
- DLQ dashboard
- Exponential backoff retry
- Automatic archive of processed Inbox records
- Metrics (retry count, processing latency, DLQ size)
