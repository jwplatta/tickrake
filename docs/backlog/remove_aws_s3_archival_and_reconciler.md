---
type: chore
tags: [storage, s3, minio, maintenance, reconciler]
title: Remove AWS S3 cold archival, manifest reconciler, and obsolete S3 storage code
description: Remove AWS S3 archival pipeline, reconciler job, and Glacier storage classes while preserving MinIO intraday publishing
created: 2026-10-16
updated: 2026-10-29
status: complete
priority: medium
source: antigravity/tickrake
---

# Summary

Tickrake is no longer responsible for long-term durable archival and persistence in AWS S3 (Glacier storage classes, cold archive, S3 manifests, and manifest reconciler), as durable archival and analytics will be handled downstream by Apache Iceberg. Phase 1 (PR #112) disabled S3 uploads across jobs so that data collection and compaction remain strictly local.

This chore tracks Phase 2: the complete removal of all dead AWS S3 archival code, the S3 manifest reconciler pipeline, obsolete S3 classes, and manifest guards, while keeping MinIO (S3-compatible) support intact for `IntradayPublisherJob`.

## Requirements

### 1. Maintenance Pipeline Cleanup
- Delete S3 candle archiver: `lib/tickrake/maintenance/candles/archiver.rb`.
- Delete S3 candle manifest writer: `lib/tickrake/maintenance/candles/manifest_writer.rb`.
- Delete S3 option sample archiver: `lib/tickrake/maintenance/option_samples/artifact_archiver.rb`.
- Delete S3 option sample manifest writer: `lib/tickrake/maintenance/option_samples/manifest_writer.rb`.
- Delete S3 local artifact manager: `lib/tickrake/maintenance/option_samples/local_artifact_manager.rb`.
- In `lib/tickrake/maintenance/option_samples/source_sample_cleaner.rb`:
  - Remove `manifest_writer` and `s3_archive` dependencies.
  - Remove `guard_manifest!` and `uri_to_key` (local CSV deletion is already safeguarded by `Validator`).
- In `lib/tickrake/maintenance/candles/results.rb` and `lib/tickrake/maintenance/option_samples/results.rb`:
  - Remove `ArchiveResult` and `RetentionResult` structs.
- In `lib/tickrake/maintenance_job.rb`:
  - Delete `run_archive_step`, `archive_s3_archive_for_destination`, `archive_s3_archive`, and the `when "archive"` branches.
- In `lib/tickrake/dsl/maintenance_builder.rb`:
  - Remove `def archive(...)`.

### 2. S3 Manifest Reconciler Deletion
- Delete `lib/tickrake/reconciler_job.rb`.
- Delete `lib/tickrake/reconciler_runner.rb`.
- Delete `lib/tickrake/dsl/reconcile_builder.rb`.
- In `lib/tickrake/dsl/job_builder.rb`:
  - Remove `reconcile` block, `@reconcile_builder`, and `build_reconcile_job!`.
- In `lib/tickrake/job_runner.rb`:
  - Remove `when "reconciler"` dispatch cases.
- In `lib/tickrake/config.rb`:
  - Remove `"reconciler"` from `provider_names_for_job`.
- In `lib/tickrake.rb`:
  - Remove requires for reconciler and deleted maintenance classes.

### 3. Events Ingestion Dead Code Cleanup
- In `lib/tickrake/events_ingestor_job.rb`:
  - Remove unused `@datastore_name` instance variable and `upload_to_s3` method.
- In `lib/tickrake/dsl/events_ingest_builder.rb`:
  - Remove `def datastore(name)`.

### 4. Configuration & Datastore Refactoring
- In `lib/tickrake/config_loader.rb`:
  - Remove `VALID_ARCHIVE_DESTINATIONS`, `VALID_ARCHIVE_ARTIFACTS`, and `VALID_S3_STORAGE_CLASSES`.
  - Set `VALID_MAINTENANCE_ACTIONS = %w[compact].freeze`.
  - Reject `archive` tasks with a clear `ConfigError`.
  - Update `VALID_DATASTORE_TYPES` to focus on MinIO / S3-compatible stores (`%w[minio s3_compatible]`).
- Refactor `lib/tickrake/storage/s3_archive.rb` into a clean MinIO / S3-compatible client (e.g. `MinioStore` or `S3CompatibleStore`) used by `IntradayPublisherJob`, retaining `aws-sdk-s3` for the MinIO S3 API.

### 5. Spec & Script Cleanup
- Delete `spec/manifest_writer_spec.rb`.
- Update `spec/maintenance_option_samples_spec.rb` to remove unit tests for `ArtifactArchiver` and S3 mocks.
- Update `spec/dsl/maintenance_builder_spec.rb` and `spec/dsl/job_builder_spec.rb` to remove `archive` and `reconcile` specs.
- Remove obsolete scripts (`scripts/backfill_manifests.rb`, `scripts/rearchive_candles_to_s3.rb`).

## Dependencies & Resources

- Phase 1 PR: [#112](https://github.com/jwplatta/tickrake/pull/112) (`chore: disable S3 uploads across jobs while preserving MinIO intraday publishing`)
- Architecture documentation: `docs/architecture.md`, `docs/storage_layout.md`
- MinIO Intraday Specification: `docs/plans/minio_intraday_storage.md`
