# Bonus Architecture Brief: LLM Observability at 1B Requests/Day

Full name: Nguyen Tuan Dung

## 1. Problem Statement

We operate a foundation-model API platform that logs every request and response for reliability, billing, safety review, and customer dashboards. The platform receives **1 billion requests per day**, with an average raw event size of **5 KB**, producing about **5 TB/day of raw logs**. Product teams need cost, latency, token usage, and error-rate dashboards refreshed every **5 minutes** by tenant and model. Incident reviewers need full prompts and responses for only **7 days**, while aggregated metrics must be retained for **1 year**. All personally identifiable information must be redacted or tokenized before any human or analyst can read the data. The hardest constraint is FinOps: total storage spend must stay under **$5,000/month** while still keeping the hot analytics path fast enough for operational dashboards.

## 2. Architecture Diagram

```text
API Gateway / Model Serving
        |
        | JSON events: tenant_id, request_id, ts, model, token usage,
        | latency_ms, status, prompt, response, possible PII
        v
Kafka / Kinesis stream
        |
        v
+---------------------------+
| Bronze: raw_llm_events    |
| Delta, append-only        |
| PII tokenized on landing  |
| Retention: 7 days full    |
+---------------------------+
        |
        | streaming parse, schema checks, quarantine invalid records
        v
+---------------------------+
| Silver: llm_calls_clean   |
| typed columns             |
| dedup by request_id       |
| partition: event_date     |
| cluster/Z-order: tenant   |
+---------------------------+
        |
        | 5-minute aggregations
        v
+---------------------------+
| Gold: tenant_model_5m     |
| cost, p50/p95 latency     |
| error_rate, token totals  |
| Retention: 1 year         |
+---------------------------+
        |
        +--> BI dashboards / alerts
        +--> billing export
        +--> incident review UI (7-day restricted access to Bronze)
```

## 3. Key Decisions and Rejected Alternatives

### Decision 1: Table Format

I chose **Delta Lake** for Bronze, Silver, and Gold tables because this workload needs ACID writes, schema enforcement, time travel, MERGE-based deduplication, and restore after bad pipeline deployments. Delta also fits the Day 18 lab pattern and supports file compaction plus Z-order/clustering for dashboard filters.

I rejected **plain Parquet on S3** because it has no transaction log, no reliable rollback, and weak protection against partial writes. At 1B events/day, one broken job could leave analysts reading mixed old and new data. I rejected **CSV/JSON-only object storage** because storage size and query cost would be too high, and schema drift would only be discovered after dashboards break.

### Decision 2: Medallion Layout

I chose a **Bronze -> Silver -> Gold** layout. Bronze preserves tokenized raw events for audit and incident review. Silver becomes the contract table with typed columns, deduplicated `request_id`, validated status values, and separated token counts. Gold stores small dashboard-ready aggregates by `event_time_5m`, `tenant_id`, and `model`.

I rejected querying **Bronze directly** for dashboards because raw JSON parsing on every query would be slow and risky. I rejected writing **only Gold aggregates** because incident review requires temporary access to full prompt/response data for the last 7 days.

### Decision 3: Partitioning and Clustering

I chose to partition Bronze and Silver by **event_date** and cluster or Z-order Silver/Gold by **tenant_id, model**. Most operations filter by recent time windows, tenant, and model. Date partitioning keeps retention simple, while clustering improves file skipping for the hot dashboard path.

I rejected partitioning by **tenant_id** because large enterprise tenants would create skew and small tenants would create many tiny partitions. I rejected partitioning by **model only** because dashboards usually start with tenant filters, and model cardinality is too low to prune enough data by itself.

### Decision 4: PII Handling

I chose **tokenization at Bronze landing**. The ingestion job detects emails, phone numbers, user IDs, and other sensitive fields inside prompt/response payloads, stores tokenized text in Bronze, and writes a restricted mapping table to a separate security-controlled store. Human analysts only see tokenized data by default.

I rejected redacting PII only in **Silver** because raw Bronze would still expose sensitive data to storage readers and incident responders. I rejected deleting all prompts and responses immediately because it would make incident investigation impossible for abuse, billing disputes, and model-quality debugging.

### Decision 5: Retention and Lifecycle

I chose **7 days retention for full Bronze payloads**, **90 days for Silver clean request-level records**, and **1 year for Gold aggregates**. Bronze is the most expensive and sensitive layer, so it expires quickly. Gold is tiny compared with raw events and supports long-term reporting.

I rejected keeping full raw logs for **1 year** because 5 TB/day would produce about 1.8 PB/year, far beyond the $5K/month target. I rejected keeping only 24 hours of raw logs because many customer incidents and billing disputes are discovered several days later.

### Decision 6: Catalog and Governance

I chose a centralized catalog with table ownership, column descriptions, retention tags, and access policies. Bronze access is restricted to on-call incident responders. Silver is available to data engineers and platform teams. Gold is available to analysts, billing, and customer-success dashboards.

I rejected managing access through **S3 paths only** because permissions become hard to audit across many teams. I rejected giving analysts direct access to **all layers** because it increases PII risk and makes it too easy to build dashboards on unstable raw schemas.

## 4. Failure Modes

### Failure Mode 1: Bad Schema Deployment

A model-serving release adds a new nested JSON shape or changes `usage.output` from integer to string. The Silver streaming job starts failing or silently writes null metrics. Detection: schema enforcement failures, null-rate alerts, and dashboard row-count drops. Rollback: stop the Silver job, use Delta time travel to compare the last good version, patch the parser, then reprocess Bronze events from the affected time window.

### Failure Mode 2: Duplicate Request Events

Network retries or producer bugs emit the same `request_id` multiple times. If not handled, billing and cost dashboards overcount tokens. Detection: duplicate-rate metric in Silver, grouped by producer service and tenant. Rollback: Silver uses MERGE/dedup by `request_id`, keeping the earliest successful event or the latest terminal status. Gold is rebuilt for affected 5-minute windows from the corrected Silver table.

### Failure Mode 3: PII Tokenization Failure

A tokenizer version misses a new phone-number format in prompts. Detection: daily PII scanner samples Bronze and Silver text columns and reports any un-tokenized patterns. Rollback: immediately revoke Bronze access, restore the last safe tokenizer version, re-tokenize the affected Bronze partitions, and regenerate Silver/Gold from those partitions. The old unsafe files are vacuumed only after legal approval and audit logging.

### Failure Mode 4: Small-File Explosion

Streaming ingestion writes too many tiny files during a traffic spike, causing dashboard latency to miss the 5-minute SLA. Detection: file-count and average-file-size alerts per partition. Rollback: run emergency compaction on the latest partitions, then tune streaming trigger interval and target file size. For long-term prevention, schedule OPTIMIZE on hot partitions every hour and Z-order by tenant/model.

## 5. Cost Back-of-Envelope

Assumptions:

- Raw input: **5 TB/day**
- Bronze full payload retention: **7 days**
- Silver request-level data is compressed/columnar to about **25% of raw**, retained **90 days**
- Gold aggregates are about **50 GB/month**
- S3 Standard: approximately **$23/TB-month**
- S3 Infrequent Access: approximately **$12.5/TB-month**
- Compute budget: streaming + compaction + dashboard refresh estimated separately

Storage math:

```text
Bronze hot:
5 TB/day * 7 days = 35 TB
35 TB * $23/TB-month = $805/month

Silver warm:
5 TB/day * 25% * 90 days = 112.5 TB
112.5 TB * $12.5/TB-month = $1,406/month

Gold aggregates:
0.05 TB * $23/TB-month = $1.15/month

Estimated storage total = about $2,212/month
```

Compute estimate:

```text
Streaming ingestion + Silver parsing: about $1,200/month
Gold aggregation every 5 minutes: about $500/month
OPTIMIZE/compaction jobs: about $600/month
BI/dashboard query warehouse: about $500/month

Estimated compute total = about $2,800/month
```

Total estimated monthly cost is **about $5,012/month**. This is slightly above the $5K target, so the practical adjustment is to reduce Silver retention from 90 days to **85 days** or move older Silver partitions to colder storage. Reducing Silver by 5 days saves:

```text
5 TB/day * 25% * 5 days = 6.25 TB
6.25 TB * $12.5/TB-month = $78/month
```

That brings the estimate to about **$4,934/month**, leaving a small buffer.

## 6. One-Week MVP Slice

The first one-week MVP should prove the riskiest path end to end for one high-volume tenant:

1. Ingest synthetic or sampled LLM events into Bronze with tokenized prompt/response fields.
2. Build Silver parsing with schema enforcement, quarantine for invalid records, and dedup by `request_id`.
3. Build a Gold table with 5-minute aggregates: request count, token totals, p50/p95 latency, error rate, and estimated cost.
4. Add one dashboard query filtered by tenant and model.
5. Add one rollback drill: inject bad records, restore or reprocess from Bronze, and show Gold metrics corrected.

This MVP does not need every tenant, every model, or full governance automation. It only needs to prove that the medallion path, PII handling, dashboard freshness, and rollback strategy are technically feasible before scaling to the full 1B requests/day workload.
