# Feature Requests

- [x] Move runtime logs into a dedicated logs folder under the Tickrake root and add a log retention plan so the home directory does not keep accumulating log files indefinitely.
- [x] Improve Schwab job resilience and restart behavior so overlapping failures or timeout cascades do not require frequent manual restarts.
- [ ] Add a second maintenance task for uploading compacted option artifacts to S3.
- [ ] Extend `file_metadata_cache` so compacted artifact rows can track upload lifecycle state like local-only, ready-to-upload, uploaded, and failed.
- [ ] Add manual upload runs over a date range, parallel to the existing manual compaction flow.
- [ ] Add scheduler support for the upload maintenance task once the S3 path is implemented.
- [x] Add a dedicated `tickrake delete-compacted-option-samples` command with `--dry-run` so validated source snapshot CSVs can be removed intentionally after a successful check.
