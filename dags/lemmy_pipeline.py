"""
lemmy_pipeline
==============

Fetches the hottest posts from the Lemmy community c/books (lemmy.world)
and loads them into the PostgreSQL database LEMMY.

    lemmy_api_call -> load_stage_data -> load_dice_data -> qc_check -> update_control_table

Needs an Airflow connection named ``lemmy_db`` that points at the LEMMY database.
Optional Airflow variable ``lemmy_user_agent`` sets the User-Agent sent to Lemmy.
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

CONN_ID = "lemmy_db"
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")

API_URL = "https://lemmy.world/api/v3/post/list"
COMMUNITY = "books@lemmy.world"
SORT = "Hot"
PAGE_SIZE = 50  # Lemmy's maximum per request
DEFAULT_ROWS_TO_BE_LOADED = 100
DEFAULT_USER_AGENT = "turingiq-training/1.0"


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def read_sql(name: str) -> str:
    """Read one of the SQL files in dags/sql/."""
    with open(os.path.join(SQL_DIR, name), encoding="utf-8") as f:
        return f.read()


def get_rows_to_be_loaded(hook: PostgresHook) -> int:
    """rows_to_be_loaded from the most recent control row (100 if the table is empty)."""
    row = hook.get_first(
        "SELECT rows_to_be_loaded FROM lemmy_control_table "
        "ORDER BY start_ts DESC NULLS LAST, ctl_id DESC LIMIT 1"
    )
    if row is None or row[0] is None:
        return DEFAULT_ROWS_TO_BE_LOADED
    value = int(row[0])
    if value < 1:
        raise AirflowException(
            f"rows_to_be_loaded in lemmy_control_table must be at least 1 (found {value})"
        )
    return value


def fetch_lemmy(rows_to_be_loaded: int, user_agent: str = DEFAULT_USER_AGENT) -> list[dict]:
    """Call the public Lemmy API and return up to rows_to_be_loaded posts (50 per request)."""
    headers = {"User-Agent": user_agent}
    posts: list[dict] = []
    seen_ids: set[str] = set()
    cursor = None
    max_pages = math.ceil(rows_to_be_loaded / PAGE_SIZE) + 5  # safety limit

    for _ in range(max_pages):
        params = {"community_name": COMMUNITY, "sort": SORT, "limit": PAGE_SIZE}
        if cursor:
            params["page_cursor"] = cursor  # ask for the next page

        response = requests.get(API_URL, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        page = data.get("posts", [])
        if not page:
            break

        for item in page:
            post = item.get("post", {})
            post_id = str(post.get("id"))
            if post_id in seen_ids:
                continue  # the same post can appear on two pages
            seen_ids.add(post_id)
            counts = item.get("counts", {})
            posts.append(
                {
                    "id": post_id,
                    "title": post.get("name"),
                    "author": (item.get("creator") or {}).get("name"),
                    "score": counts.get("score"),
                    "upvotes": counts.get("upvotes"),
                    "downvotes": counts.get("downvotes"),
                    "num_comments": counts.get("comments"),
                    "url": post.get("ap_id"),
                    "published": post.get("published"),
                    "body": post.get("body"),
                    "nsfw": post.get("nsfw"),
                    "featured": post.get("featured_community"),
                    "community": (item.get("community") or {}).get("name"),
                }
            )
            if len(posts) >= rows_to_be_loaded:
                return posts

        cursor = data.get("next_page")
        if not cursor:
            break

    return posts


# --------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------
@dag(
    dag_id="lemmy_pipeline",
    description="Lemmy c/books -> stage -> dice -> QC -> control table",
    schedule=None,  # run it by hand with "Trigger DAG"
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "turingiq", "retries": 0},
    tags=["turingiq", "lemmy", "week_7"],
)
def lemmy_pipeline():
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def lemmy_api_call(ti=None) -> list[dict]:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        rows_to_be_loaded = get_rows_to_be_loaded(hook)
        user_agent = Variable.get("lemmy_user_agent", default_var=DEFAULT_USER_AGENT)

        posts = fetch_lemmy(rows_to_be_loaded, user_agent)
        if not posts:
            raise AirflowException(f"The Lemmy API returned no posts for {COMMUNITY}")

        ti.xcom_push(key="rows_to_be_loaded", value=rows_to_be_loaded)
        log.info("Fetched %d posts from %s", len(posts), COMMUNITY)
        return posts

    @task
    def load_stage_data(posts: list[dict], run_id=None) -> int:
        rows = [
            {"run_id": run_id, "post_id": int(p["id"]), "raw_json": json.dumps(p)}
            for p in posts
        ]
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        try:
            with conn, conn.cursor() as cur:
                # If this run is retried, replace its rows instead of adding duplicates.
                cur.execute("DELETE FROM stg_lemmy_posts WHERE run_id = %s", (run_id,))
                cur.executemany(read_sql("02_load_stage.sql"), rows)
        finally:
            conn.close()
        log.info("Loaded %d raw rows into stg_lemmy_posts", len(rows))
        return len(rows)

    @task
    def load_dice_data(run_id=None) -> int:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(read_sql("03_stage_to_dice.sql"), {"run_id": run_id})
                upserted = cur.rowcount
        finally:
            conn.close()
        log.info("Upserted %d rows into dice_lemmy_posts", upserted)
        return upserted

    @task
    def qc_check(run_id=None) -> None:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        conn.autocommit = True  # one failing query must not block the others
        failures = []
        try:
            with conn.cursor() as cur:
                cur.execute(read_sql("04_qc_check.sql"))
                checks = cur.fetchall()
                if not checks:
                    log.warning("No active QC checks in qc_checks")

                for qc_id, src_query, tgt_query in checks:
                    try:
                        values = []
                        for query in (src_query, tgt_query):
                            if "%(run_id)s" in query:
                                cur.execute(query, {"run_id": run_id})
                            else:
                                cur.execute(query)
                            row = cur.fetchone()
                            values.append(row[0] if row else None)
                        src, tgt = values
                    except Exception as exc:  # bad SQL in the qc_checks row
                        log.error("[FAIL] %s: query error: %s", qc_id, exc)
                        failures.append(qc_id)
                        continue

                    if src == tgt:
                        log.info("[PASS] %s: src=%s, tgt=%s", qc_id, src, tgt)
                    else:
                        log.error("[FAIL] %s: src=%s, tgt=%s", qc_id, src, tgt)
                        failures.append(qc_id)
        finally:
            conn.close()

        if failures:
            raise AirflowException(f"QC failed: {', '.join(failures)}")
        log.info("QC passed")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def update_control_table(dag_run=None, ti=None, run_id=None) -> None:
        """Runs even if an earlier task failed, and records SUCCESS or FAILED."""
        failed_tasks = sorted(
            t.task_id
            for t in dag_run.get_task_instances()
            if t.task_id != ti.task_id
            and t.state in (TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED)
        )
        status = "FAILED" if failed_tasks else "SUCCESS"

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        rows_to_be_loaded = ti.xcom_pull(task_ids="lemmy_api_call", key="rows_to_be_loaded")
        if rows_to_be_loaded is None:
            try:
                rows_to_be_loaded = get_rows_to_be_loaded(hook)
            except AirflowException:
                rows_to_be_loaded = DEFAULT_ROWS_TO_BE_LOADED
        rows_loaded = ti.xcom_pull(task_ids="load_stage_data") or 0

        error_message = None
        if failed_tasks:
            error_message = (
                f"Failed task(s): {', '.join(failed_tasks)}. See those tasks' logs for details."
            )

        params = {
            "run_id": run_id,
            "status": status,
            "rows_loaded": rows_loaded,
            "start_ts": dag_run.start_date,
            "end_ts": datetime.now(timezone.utc),
            "error_message": error_message,
            "rows_to_be_loaded": rows_to_be_loaded,
        }
        conn = hook.get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(read_sql("05_update_ctl.sql"), params)
        finally:
            conn.close()
        log.info("Recorded %s for run %s", status, run_id)

        if failed_tasks:
            # Keep the DAG run red in the UI when something upstream failed.
            raise AirflowException(error_message)

    posts = lemmy_api_call()
    staged = load_stage_data(posts)
    diced = load_dice_data()
    qc = qc_check()
    ctl = update_control_table()

    staged >> diced >> qc >> ctl


lemmy_pipeline()
