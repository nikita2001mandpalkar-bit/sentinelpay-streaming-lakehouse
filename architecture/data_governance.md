# SentinelPay Data Governance

## Purpose
This document defines the lightweight data governance controls used in the SentinelPay lakehouse project.

## Ownership
- Bronze, Silver, and Gold pipelines are owned by the data engineering function.
- dbt marts and reporting-facing semantic layers are owned by analytics engineering.

## Layer Policy
- Bronze stores raw ingested records with minimal transformation.
- Silver stores validated, standardized, and deduplicated records.
- Gold stores analytics-ready fact and dimension models.

## Data Classification
- financial: payment and refund datasets used for transaction analytics.
- internal: operational support and curated analytics datasets.
- pii: identifiers that may require stricter handling in future enterprise deployments.

## Quality Controls
- Required field validation is enforced in Silver and dbt staging models.
- Duplicate protection is enforced through streaming deduplication and dbt uniqueness tests.
- Invalid records are routed to quarantine paths for investigation.

## Retention
- Bronze is retained for replay and audit support.
- Silver is retained for curated operational analytics.
- Gold is retained for reporting and business consumption.
- Quarantine data is retained until issue triage and remediation are complete.

## Schema Change Policy
- Backward-compatible schema additions are reviewed before promotion.
- Critical downstream models must be validated before enabling new fields in analytics outputs.
- Unexpected schema drift is investigated before production rollout.

## Access and Secrets
- Infrastructure credentials are managed through environment and service configuration.
- Raw operational storage should not be directly exposed to business users.
- Gold and dbt marts are the preferred consumption layer for analytics use cases.

## Lineage and Audit Evidence
- Airflow DAG history provides orchestration audit trails.
- dbt docs provide model-level lineage for active analytics branches.
- Spark checkpoints support restart safety for streaming jobs.
