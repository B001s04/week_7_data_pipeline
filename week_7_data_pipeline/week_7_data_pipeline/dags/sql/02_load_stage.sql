-- 02_load_stage.sql
-- Inserts one raw post into the stage table (run once per post).
INSERT INTO stg_lemmy_posts (run_id, post_id, raw_json)
VALUES (%(run_id)s, %(post_id)s, %(raw_json)s::jsonb);
