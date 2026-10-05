-- Inputs: uploads(filename, content, source, received_at, header), settings(parser_version).
-- Each original line retains file/line lineage. Headers and blank lines are metadata, not events.
CREATE OR REPLACE TEMP TABLE decoded_lines AS
SELECT filename AS path, source, received_at AS upload_received_at, header,
       line_number::BIGINT AS line_number, trim(line) AS line, try_cast(line AS JSON) AS j
FROM uploads CROSS JOIN unnest(string_split(content, chr(10))) WITH ORDINALITY AS lines(line, line_number)
WHERE trim(line) <> '' AND NOT (source = 'genA' AND starts_with(trim(line), '#UPLOAD'));

CREATE OR REPLACE TEMP TABLE extracted AS
WITH fields AS (
 SELECT *,
  CASE source WHEN 'genA' THEN regexp_extract(header, 'device=([^ ]+)', 1)
   WHEN 'genB' THEN json_extract_string(j, '$.dev')
   ELSE json_extract_string(j, '$.device.id') END AS device_id,
  CASE source WHEN 'genA' THEN regexp_extract(header, 'model=([^ ]+)', 1)
   WHEN 'genB' THEN json_extract_string(j, '$.model')
   ELSE json_extract_string(j, '$.device.model') END AS model,
  CASE source WHEN 'genA' THEN regexp_extract(header, 'fw=([^ ]+)', 1)
   WHEN 'genB' THEN json_extract_string(j, '$.fw')
   ELSE json_extract_string(j, '$.device.fw') END AS firmware,
  CASE source WHEN 'genA' THEN CASE
    WHEN regexp_matches(line, '^.{19} (THERM|IDLE) ') THEN 'status'
    WHEN regexp_matches(line, '^.{19} JOB start ') THEN 'job_start'
    WHEN regexp_matches(line, '^.{19} JOB end ') THEN 'job_end'
    WHEN regexp_matches(line, '^.{19} ERR ') THEN 'error' ELSE 'unknown' END
   WHEN 'genB' THEN json_extract_string(j, '$.k')
   WHEN 'genC' THEN json_extract_string(j, '$.type')
   WHEN 'prusalink' THEN 'status' ELSE 'unknown' END AS kind,
  CASE source WHEN 'genA' THEN substr(line, 1, 19)
   WHEN 'genB' THEN json_extract_string(j, '$.t')
   WHEN 'genC' THEN json_extract_string(j, '$.ts')
   ELSE json_extract_string(j, '$.received_at') END AS device_ts_raw,
  CASE source WHEN 'prusalink' THEN try_cast(json_extract_string(j, '$.received_at') AS TIMESTAMPTZ)
   ELSE upload_received_at END AS received_at,
  regexp_extract(header, 'device_time=(.{19})', 1) AS header_device_ts,
  CASE source WHEN 'genA' THEN coalesce(nullif(regexp_extract(line, ' id=([^ ]+)', 1), ''),
                                           nullif(nullif(regexp_extract(line, ' job=([^ ]+)', 1), '-'), ''))
   WHEN 'genB' THEN json_extract_string(j, '$.job')
   ELSE coalesce(json_extract_string(j, '$.job.id'), json_extract_string(j, '$.job.job.id')) END AS job_id,
  CASE source WHEN 'genA' THEN CASE WHEN regexp_matches(line, '^.{19} THERM ') THEN 'PRINTING' ELSE 'IDLE' END
   WHEN 'prusalink' THEN json_extract_string(j, '$.status.printer.state')
   ELSE json_extract_string(j, '$.state') END AS state,
  CASE source WHEN 'genA' THEN nullif(regexp_extract(line, 'material="([^"]+)"', 1), '')
   WHEN 'genB' THEN json_extract_string(j, '$.material')
   ELSE json_extract_string(j, '$.job.material') END AS material,
  try_cast(CASE source WHEN 'genA' THEN regexp_extract(line, ' layers=([^ ]+)', 1)
   WHEN 'genB' THEN json_extract_string(j, '$.layers')
   ELSE json_extract_string(j, '$.job.layers') END AS INTEGER) AS layers,
  try_cast(CASE source WHEN 'genA' THEN regexp_extract(line, ' bed=([^ ]+)', 1)
   WHEN 'genB' THEN coalesce(json_extract_string(j, '$.bed_temp'), json_extract_string(j, '$.temp_bed'))
   WHEN 'prusalink' THEN json_extract_string(j, '$.status.printer.temp_bed')
   ELSE json_extract_string(j, '$.temps.bed_c') END AS DOUBLE) AS bed_temp,
  try_cast(CASE source WHEN 'genA' THEN regexp_extract(line, ' nozzle=([^ ]+)', 1)
   WHEN 'genB' THEN coalesce(json_extract_string(j, '$.nozzle_temp'), json_extract_string(j, '$.temp_nozzle'))
   WHEN 'prusalink' THEN json_extract_string(j, '$.status.printer.temp_nozzle')
   ELSE json_extract_string(j, '$.temps.nozzle_c') END AS DOUBLE) AS nozzle_temp,
  try_cast(json_extract_string(j, '$.temps.chamber_c') AS DOUBLE) AS chamber_temp,
  try_cast(CASE source WHEN 'genA' THEN regexp_extract(line, ' tgt_bed=([^ ]+)', 1)
   WHEN 'genB' THEN json_extract_string(j, '$.target_bed')
   WHEN 'prusalink' THEN json_extract_string(j, '$.status.printer.target_bed')
   ELSE json_extract_string(j, '$.targets.bed_c') END AS DOUBLE) AS target_bed_temp,
  try_cast(CASE source WHEN 'genA' THEN regexp_extract(line, ' tgt_nozzle=([^ ]+)', 1)
   WHEN 'genB' THEN json_extract_string(j, '$.target_nozzle')
   WHEN 'prusalink' THEN json_extract_string(j, '$.status.printer.target_nozzle')
   ELSE json_extract_string(j, '$.targets.nozzle_c') END AS DOUBLE) AS target_nozzle_temp,
  CASE source WHEN 'genB' THEN coalesce(json_extract_string(j, '$.temp_unit'), 'C') ELSE 'C' END AS temp_unit,
  CASE source WHEN 'genA' THEN CASE regexp_extract(line, ' result=([^ ]+)', 1)
     WHEN 'ok' THEN 'success' WHEN 'fail' THEN 'failure' ELSE 'unknown' END
   WHEN 'genB' THEN CASE json_extract_string(j, '$.ok')
     WHEN 'true' THEN 'success' WHEN 'false' THEN 'failure' ELSE 'unknown' END
   ELSE CASE json_extract_string(j, '$.outcome')
     WHEN 'success' THEN 'success' WHEN 'failure' THEN 'failure' ELSE 'unknown' END END AS outcome,
  CASE source WHEN 'genA' THEN nullif(regexp_extract(line, ' code=([^ ]+)', 1), '')
   WHEN 'genB' THEN json_extract_string(j, '$.code')
   ELSE json_extract_string(j, '$.error.code') END AS error_code
 FROM decoded_lines
), timestamps AS (
 SELECT *,
  CASE source
   WHEN 'genA' THEN received_at - (try_cast(header_device_ts AS TIMESTAMP) - try_cast(device_ts_raw AS TIMESTAMP))
   WHEN 'genB' THEN try(to_timestamp(try_cast(device_ts_raw AS DOUBLE) / 1000))
   ELSE CASE WHEN regexp_matches(device_ts_raw, '(Z|[+-][0-9]{2}:[0-9]{2})$')
             THEN try_cast(device_ts_raw AS TIMESTAMPTZ) END END AS event_ts,
  CASE source WHEN 'genA' THEN 'upload_offset_estimate' WHEN 'prusalink' THEN 'received_only'
   ELSE 'device_utc' END AS timestamp_quality,
  CASE WHEN source = 'genA' THEN epoch(received_at) - epoch(try_cast(header_device_ts AS TIMESTAMP)) END AS clock_offset_seconds
 FROM fields
)
SELECT *, CASE
 WHEN source NOT IN ('genA', 'genB', 'genC', 'prusalink') THEN 'unsupported_source'
 WHEN source <> 'genA' AND (j IS NULL OR json_type(j) <> 'OBJECT') THEN 'invalid_json'
 WHEN nullif(device_id, '') IS NULL THEN 'missing_device'
 WHEN received_at IS NULL OR NOT isfinite(received_at) THEN 'invalid_received_time'
 WHEN event_ts IS NULL OR NOT isfinite(event_ts) THEN 'invalid_device_time'
 WHEN event_ts > received_at + INTERVAL '5 minutes' THEN 'future_event_time'
 WHEN kind IS NULL OR kind NOT IN ('status', 'job_start', 'job_end', 'error', 'maintenance') THEN 'unsupported_kind'
 WHEN kind = 'maintenance' AND (SELECT parser_version FROM settings) < 2 THEN 'unsupported_kind'
 WHEN kind IN ('job_start', 'job_end') AND nullif(job_id, '') IS NULL THEN 'missing_job_id'
 WHEN kind = 'status' AND (bed_temp IS NULL OR nozzle_temp IS NULL) THEN 'missing_temperature'
 WHEN kind = 'status' AND temp_unit NOT IN ('C', 'F') THEN 'unsupported_temperature_unit'
 WHEN kind = 'status' AND (NOT isfinite(bed_temp) OR NOT isfinite(nozzle_temp)
   OR CASE WHEN temp_unit = 'F' THEN (bed_temp - 32) / 1.8 ELSE bed_temp END NOT BETWEEN -50 AND 200
   OR CASE WHEN temp_unit = 'F' THEN (nozzle_temp - 32) / 1.8 ELSE nozzle_temp END NOT BETWEEN -50 AND 500)
   THEN 'invalid_temperature'
 ELSE NULL END AS reason
FROM timestamps;

CREATE OR REPLACE TEMP TABLE batch_events AS
SELECT path, line_number, (SELECT parser_version FROM settings)::INTEGER AS parser_version, source,
       device_id, model, firmware, kind, device_ts_raw, received_at, event_ts, event_ts::DATE AS event_date,
       timestamp_quality, clock_offset_seconds, job_id, CASE WHEN kind = 'status' THEN state END AS state,
       material, layers, bed_temp, nozzle_temp, chamber_temp, target_bed_temp, target_nozzle_temp, temp_unit,
       CASE WHEN kind = 'job_end' THEN outcome END AS outcome, error_code,
       sha256(to_json(list_value(device_id, kind, device_ts_raw, coalesce(job_id, ''),
                                 CASE WHEN kind = 'status' THEN coalesce(state, '') ELSE '' END))) AS dedupe_key
FROM extracted WHERE reason IS NULL;

-- Rejected lines keep their device and firmware so reject rates can be tracked by version. A line too
-- broken to read borrows both from its neighbours in the same upload (one upload comes from one printer).
CREATE OR REPLACE TEMP TABLE batch_unparsed AS
WITH located AS (
 SELECT *,
  coalesce(last_value(nullif(device_id, '') IGNORE NULLS) OVER earlier,
           first_value(nullif(device_id, '') IGNORE NULLS) OVER later) AS neighbour_device,
  coalesce(last_value(nullif(firmware, '') IGNORE NULLS) OVER earlier,
           first_value(nullif(firmware, '') IGNORE NULLS) OVER later) AS neighbour_firmware
 FROM extracted
 WINDOW earlier AS (PARTITION BY path ORDER BY line_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        later AS (PARTITION BY path ORDER BY line_number ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
)
SELECT path, line_number, (SELECT parser_version FROM settings)::INTEGER AS parser_version,
       source, received_at, line, reason,
       coalesce(nullif(device_id, ''), neighbour_device) AS device_id,
       coalesce(nullif(firmware, ''), neighbour_firmware) AS firmware,
       CASE WHEN nullif(firmware, '') IS NOT NULL THEN CASE WHEN source = 'genA' THEN 'header' ELSE 'line' END
            WHEN neighbour_firmware IS NOT NULL THEN 'same_upload'
            ELSE 'unknown' END AS firmware_source
FROM located WHERE reason IS NOT NULL;
