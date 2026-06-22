# Reflection
Full name: Nguyen Tuan Dung

The anti-pattern my team would be most at risk of is **treating the data lake as a dumping ground**: storing raw logs without clear contracts for schema, deduplication, partitioning, and quality checks. For LLM observability data, logs may come from many services, JSON formats can change quickly, retries can create duplicate `request_id`s, and metrics such as latency, cost, and error rate can easily become wrong if analysts query raw data directly.

This lab shows how to avoid that anti-pattern with medallion architecture. Bronze keeps raw data for auditability, Silver parses and standardizes the schema while removing duplicates, and Gold stores trusted aggregated metrics for dashboards. Delta transaction logs, schema enforcement, time travel, and restore also make failures controllable instead of relying on unsafe file overwrites. In a real team, I would require a Silver table as the data contract before any analytics or model monitoring workload uses the lakehouse data.
