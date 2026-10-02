-- Full model rebuilds are deliberate at demo size, not an incremental production claim.
CREATE OR REPLACE TABLE fact_print_job AS
WITH jobs AS (
 SELECT device_id, job_id, arg_min(source, event_ts) AS source, arg_min(model, event_ts) AS model,
        arg_min(firmware, event_ts) AS firmware, arg_min(material, event_ts) AS material,
        max(layers) AS layers,
        min(event_ts) FILTER (WHERE kind = 'job_start') AS start_ts,
        max(event_ts) FILTER (WHERE kind = 'job_end') AS end_ts,
        min(event_ts) AS first_observed_ts, max(event_ts) AS last_observed_ts,
        arg_max(outcome, event_ts) FILTER (WHERE kind = 'job_end') AS end_outcome,
        count(*) FILTER (WHERE kind = 'status') AS readings,
        max(bed_c) FILTER (WHERE kind = 'status') AS max_bed_c,
        max(nozzle_c) FILTER (WHERE kind = 'status') AS max_nozzle_c,
        max(chamber_c) FILTER (WHERE kind = 'status') AS max_chamber_c,
        count(*) FILTER (WHERE kind = 'error') AS error_count,
        arg_min(timestamp_quality, event_ts) AS timestamp_quality
 FROM clean_events WHERE job_id IS NOT NULL GROUP BY device_id, job_id
)
SELECT * EXCLUDE (end_outcome),
       CASE WHEN start_ts IS NOT NULL AND end_ts >= start_ts THEN epoch(end_ts - start_ts) END AS duration_seconds,
       CASE WHEN end_ts IS NULL THEN 'no_end_event' ELSE coalesce(end_outcome, 'unknown') END AS outcome,
       CASE WHEN start_ts IS NULL AND end_ts IS NULL THEN 'observations_only'
            WHEN start_ts IS NULL THEN 'no_start_event' WHEN end_ts IS NULL THEN 'no_end_event'
            WHEN end_ts < start_ts THEN 'end_before_start' ELSE 'complete' END AS lifecycle_quality
FROM jobs;

CREATE OR REPLACE TABLE agg_status_hourly AS
SELECT device_id, source, model, firmware, date_trunc('hour', event_ts) AS hour_ts,
       count(*) AS samples, count(*) FILTER (WHERE state = 'PRINTING') AS printing_samples,
       count(*) FILTER (WHERE state = 'IDLE') AS idle_samples,
       avg(bed_c) AS avg_bed_c, avg(nozzle_c) AS avg_nozzle_c,
       max(nozzle_c) AS max_nozzle_c, avg(chamber_c) AS avg_chamber_c
FROM clean_events WHERE kind = 'status' GROUP BY device_id, source, model, firmware, hour_ts;

CREATE OR REPLACE TABLE dim_printer AS
SELECT device_id, arg_max(source, event_ts) AS source, arg_max(model, event_ts) AS model,
       min(event_ts) AS first_seen_ts, max(event_ts) AS last_seen_ts,
       arg_max(firmware, event_ts) AS current_firmware,
       count(DISTINCT firmware) AS firmware_versions_seen, count(*) AS event_count,
       count(DISTINCT job_id) AS jobs_observed
FROM clean_events GROUP BY device_id;

CREATE OR REPLACE VIEW failure_rates AS
SELECT model, firmware, material, count(*) AS jobs,
       count(*) FILTER (WHERE outcome IN ('success', 'failure')) AS labelled_jobs,
       count(*) FILTER (WHERE outcome = 'failure') AS failures,
       count(*) FILTER (WHERE outcome = 'unknown') AS unknown_jobs,
       count(*) FILTER (WHERE outcome = 'no_end_event') AS no_end_jobs,
       count(*) FILTER (WHERE outcome = 'failure')::DOUBLE /
          nullif(count(*) FILTER (WHERE outcome IN ('success', 'failure')), 0) AS failure_rate
FROM fact_print_job GROUP BY model, firmware, material;
