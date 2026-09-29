-- 03_stage_to_dice.sql
-- Parses this run's stage rows into the clean dice table (one row per post).
-- Posts that already exist are updated (upsert).
INSERT INTO dice_lemmy_posts
    (post_id, title, score, num_comments, url, created_at, first_run_id, last_run_id, updated_at)
SELECT DISTINCT ON (s.post_id)
    s.post_id,
    s.raw_json ->> 'title',
    (s.raw_json ->> 'score')::integer,
    (s.raw_json ->> 'num_comments')::integer,
    s.raw_json ->> 'url',
    (s.raw_json ->> 'published')::timestamptz,
    s.run_id,
    s.run_id,
    now()
FROM stg_lemmy_posts s
WHERE s.run_id = %(run_id)s
ORDER BY s.post_id, s.stg_id DESC
ON CONFLICT (post_id) DO UPDATE SET
    title        = EXCLUDED.title,
    score        = EXCLUDED.score,
    num_comments = EXCLUDED.num_comments,
    url          = EXCLUDED.url,
    created_at   = EXCLUDED.created_at,
    last_run_id  = EXCLUDED.last_run_id,
    updated_at   = now();
