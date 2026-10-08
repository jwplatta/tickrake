---
type: chore
tags: [s3, archive, options, parquet, maintenance]
title: Archive only compacted parquet files to S3 instead of CSV
description: Stop uploading compacted CSV option artifacts to S3 archives, retaining only compacted Parquet files in remote storage
created: 2026-10-08
updated: 2026-10-08
status: complete
priority: medium
source: antigravity/tickrake
---

# Summary

During maintenance jobs, option samples are compacted locally into both CSV and Parquet files (`compact_option_samples`). Currently, the archival step (`archive_option_samples` / `ArtifactArchiver`) supports and defaults to uploading both CSV and Parquet artifacts to S3 (`[csv, parquet]`). 

To reduce storage footprint, transfer costs, and S3 object overhead, we no longer want to upload the CSV files to the S3 bucket; we only want to archive the compacted Parquet file. Compacted Parquet is more compact, typed, and the primary artifact for analytical querying (`duckdb`, `quant_rb`), whereas CSV was only kept for redundant human readability.

## Requirements

- **Maintenance Archiving Default / Configuration**:
  - Update option maintenance archival behavior so that only compacted `.parquet` artifacts are uploaded to remote S3 storage by default.
  - Deprecate or remove uploading compacted `.csv` files to S3 in `ArtifactArchiver` (or ensure default `artifacts` list is `["parquet"]`).
  - Update example configuration files (e.g. `config/tickrake.example.yml`) and DSL builder defaults where `artifacts: [csv, parquet]` is specified for option maintenance archive tasks.
- **Manifest Updates**:
  - Ensure `ManifestWriter` handles manifests containing only the `parquet` artifact without requiring or expecting `csv`.
- **Local Retention Policy Alignment**:
  - Verify that `LocalArtifactManager` and safe deletion / retention rules (`retain_local`) handle keeping/removing local CSVs and Parquets appropriately when only Parquet exists remotely.
- **Specs & Documentation**:
  - Update relevant specs (`spec/maintenance_option_samples_spec.rb`, DSL specs, etc.) to reflect Parquet-only remote archival.
  - Update any documentation or notes referencing S3 option archival expectations.

## Dependencies & Resources

- `lib/tickrake/maintenance/option_samples/artifact_archiver.rb`
- `lib/tickrake/maintenance/option_samples/manifest_writer.rb`
- `lib/tickrake/maintenance/option_samples/local_artifact_manager.rb`
- `lib/tickrake/maintenance_job.rb`
- `config/tickrake.example.yml`
- `spec/maintenance_option_samples_spec.rb`
- `docs/notes/s3_compacted_option_archive_plan.md`
