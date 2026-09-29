-- 05_update_ctl.sql
-- Writes one audit row for the current DAG run.
INSERT INTO lemmy_control_table
    (run_id, status, rows_loaded, start_ts, end_ts, error_message, rows_to_be_loaded)
VALUES
    (%(run_id)s, %(status)s, %(rows_loaded)s, %(start_ts)s, %(end_ts)s, %(error_message)s, %(rows_to_be_loaded)s);
