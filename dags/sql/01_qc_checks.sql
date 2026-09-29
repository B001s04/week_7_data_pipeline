-- 01_qc_checks.sql
-- Creates the qc_checks table and adds the starting check.
-- A check passes when src_query and tgt_query return the same single value.

CREATE TABLE IF NOT EXISTS qc_checks (
    qc_id       TEXT        PRIMARY KEY,
    src_query   TEXT        NOT NULL,
    tgt_query   TEXT        NOT NULL,
    is_active   BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO qc_checks (qc_id, src_query, tgt_query)
VALUES (
    'LEMMY_COUNT_CHECK',
    'select count (*) from stg_lemmy_posts',
    'select count (*) from dice_lemmy_posts'
)
ON CONFLICT (qc_id) DO NOTHING;
