-- 00_create_tables.sql
-- Creates the stage, dice and control tables in the LEMMY database.
-- Safe to run more than once: nothing is dropped, and missing columns are added.

-- Stage: raw landing zone. Every run adds its rows, tagged with the Airflow run_id.
CREATE TABLE IF NOT EXISTS stg_lemmy_posts (
    stg_id     BIGSERIAL   PRIMARY KEY,
    run_id     TEXT        NOT NULL,
    post_id    BIGINT      NOT NULL,
    raw_json   JSONB       NOT NULL,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_lemmy_posts_run_id ON stg_lemmy_posts (run_id);

-- Dice: clean data, one row per post. Re-runs update existing posts.
CREATE TABLE IF NOT EXISTS dice_lemmy_posts (
    post_id       BIGINT      PRIMARY KEY,
    title         TEXT,
    score         INTEGER,
    num_comments  INTEGER,
    url           TEXT,
    created_at    TIMESTAMPTZ,
    first_run_id  TEXT,
    last_run_id   TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dice_lemmy_posts_last_run_id ON dice_lemmy_posts (last_run_id);

-- Control: one audit row per DAG run.
CREATE TABLE IF NOT EXISTS lemmy_control_table (
    ctl_id         BIGSERIAL   PRIMARY KEY,
    run_id         TEXT        NOT NULL,
    status         TEXT        NOT NULL,
    rows_loaded    INTEGER,
    start_ts       TIMESTAMPTZ,
    end_ts         TIMESTAMPTZ,
    error_message  TEXT
);

-- How many posts the next run fetches (added separately so older tables get it too).
ALTER TABLE lemmy_control_table
    ADD COLUMN IF NOT EXISTS rows_to_be_loaded INTEGER NOT NULL DEFAULT 100;
