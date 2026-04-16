"""Entry point for the GitLab repo scanner.

Edit the CONFIG variables below, then run:

    python -m mega_manager.main

The script reads a list of GitLab repo URLs, hits the GitLab REST API v4
for each one, and saves a set of related tables:

  projects   — one row per repo (star count, visibility, latest pipeline, etc.)
  issues     — one row per issue (title, state, assignees, labels, cycle time, etc.)
  events     — project activity stream (opens, closes, comments, pushes)
  members    — direct project members
  notes      — issue comments (only when FETCH_NOTES is True)

All output is written as CSV.
Logs are written via structlog to stdout.
"""

import json
import sys
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import structlog

from mega_manager.auth import load_base_url, load_token
from mega_manager.gitlab_api import GitLabClient
from mega_manager.models import (
    EventRecord,
    IssueNoteRecord,
    IssueRecord,
    MemberRecord,
    ProjectSnapshot,
)
from mega_manager.reader import parse_repo_path, read_repos
from mega_manager.storage import save_tables


# ---------------------------------------------------------------------------
# CONFIG — edit these before running
# ---------------------------------------------------------------------------

REPOS_FILE: str = "repos.txt"          # path to the file listing GitLab repo URLs
OUTPUT_DIR: str = "output"             # directory where CSV files are written
ISSUES_STATE: str = "all"              # "opened", "closed", or "all"
ISSUES_SINCE: str | None = None        # e.g. "2026-01-01" to filter by updated date
ISSUES_LIMIT: int | None = 100         # max issues per repo; None = fetch all
FETCH_NOTES: bool = False              # fetch issue comments (1 extra call per issue)
FETCH_RELATED_MRS: bool = False        # check each issue for linked MRs
EVENTS_LIMIT: int | None = 500         # max activity events per repo; None = fetch all
DEBUG: bool = False                    # True for verbose HTTP-level logging
LOG_FILE: str = "output/run.log"       # path for structured JSON log file


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

def _configure_logging(debug: bool = False, log_file: str = "output/run.log") -> None:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    _log_fh = open(log_file, "a", encoding="utf-8")  # noqa: SIM115

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    def _tee_renderer(logger, method, event_dict):
        """Write JSON to file, then hand off to ConsoleRenderer for stdout."""
        json.dump(event_dict, _log_fh)
        _log_fh.write("\n")
        _log_fh.flush()
        return structlog.dev.ConsoleRenderer()(logger, method, event_dict)

    structlog.configure(
        processors=shared_processors + [_tee_renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            10 if debug else 20  # 10=DEBUG, 20=INFO
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _fetch_snapshot(client: GitLabClient, repo_url: str) -> ProjectSnapshot:
    """Fetch all top-level project fields for one repo."""
    snapshot = ProjectSnapshot(repo_url=repo_url)
    encoded = parse_repo_path(repo_url)
    bound = log.bind(repo=repo_url)

    bound.info("fetch.project_info")
    project = client.get_project(encoded)
    if project is None:
        snapshot.fetch_errors.append("project info unavailable")
        bound.warning("fetch.project_info_failed")
        return snapshot

    snapshot.project_id = project.get("id")
    snapshot.name_with_namespace = project.get("name_with_namespace", "")
    snapshot.description = project.get("description") or ""
    snapshot.web_url = project.get("web_url", "")
    snapshot.visibility = project.get("visibility", "")
    snapshot.archived = project.get("archived")
    snapshot.default_branch = project.get("default_branch", "")
    snapshot.star_count = project.get("star_count")
    snapshot.forks_count = project.get("forks_count")
    snapshot.open_issues_count = project.get("open_issues_count")
    snapshot.created_at = project.get("created_at", "")
    snapshot.last_activity_at = project.get("last_activity_at", "")
    snapshot.topics = ", ".join(project.get("topics") or [])

    project_id = project["id"]

    bound.info("fetch.open_mr_count", project_id=project_id)
    mr_count = client.get_open_mr_count(project_id)
    if mr_count is None:
        snapshot.fetch_errors.append("open MR count unavailable")
    else:
        snapshot.open_mr_count = mr_count

    bound.info("fetch.latest_pipeline", project_id=project_id)
    pipeline = client.get_latest_pipeline(project_id)
    if pipeline is None:
        snapshot.fetch_errors.append("latest pipeline unavailable")
    else:
        snapshot.latest_pipeline_status = pipeline.get("status", "")
        snapshot.latest_pipeline_created_at = pipeline.get("created_at", "")

    bound.info("fetch.languages", project_id=project_id)
    languages = client.get_languages(project_id)
    if not languages:
        snapshot.fetch_errors.append("languages unavailable")
    else:
        snapshot.top_language = max(languages, key=lambda k: languages[k])
        snapshot.languages_json = json.dumps(languages)

    bound.info("fetch.latest_commit", project_id=project_id)
    commit = client.get_latest_commit(project_id)
    if commit is None:
        snapshot.fetch_errors.append("latest commit unavailable")
    else:
        snapshot.latest_commit_sha = commit.get("id", "")[:12]
        snapshot.latest_commit_message = (commit.get("title") or "").strip()
        snapshot.latest_commit_date = commit.get("committed_date", "")
        snapshot.latest_commit_author = commit.get("author_name", "")

    return snapshot


def _fetch_issues(
    client: GitLabClient,
    project_id: int,
    project_url: str,
    issues_state: str,
    issues_since: str | None,
    issues_limit: int | None,
    fetch_notes: bool,
    fetch_related_mrs: bool,
) -> tuple[list[IssueRecord], list[IssueNoteRecord]]:
    """Fetch issues (and optionally notes / related MRs) for one project."""
    bound = log.bind(repo=project_url, project_id=project_id)

    bound.info("fetch.issues", state=issues_state, since=issues_since, limit=issues_limit)
    raw_issues = client.get_issues(
        project_id,
        state=issues_state,
        since=issues_since,
        limit=issues_limit,
    )

    issue_records: list[IssueRecord] = []
    note_records: list[IssueNoteRecord] = []

    for issue in raw_issues:
        iid = issue["iid"]
        time_stats = issue.get("time_stats") or {}
        milestone = issue.get("milestone") or {}

        has_related: bool | None = None
        if fetch_related_mrs:
            related = client.get_issue_related_mrs(project_id, iid)
            has_related = len(related) > 0

        issue_records.append(IssueRecord(
            project_id=project_id,
            project_url=project_url,
            issue_iid=iid,
            issue_id=issue.get("id"),
            title=issue.get("title", ""),
            state=issue.get("state", ""),
            created_at=issue.get("created_at", ""),
            updated_at=issue.get("updated_at", ""),
            closed_at=issue.get("closed_at") or "",
            labels=", ".join(issue.get("labels") or []),
            milestone_title=milestone.get("title", ""),
            milestone_id=milestone.get("id"),
            assignees=", ".join(
                a["username"] for a in (issue.get("assignees") or [])
            ),
            weight=issue.get("weight"),
            time_estimate=time_stats.get("time_estimate"),
            total_time_spent=time_stats.get("total_time_spent"),
            has_related_mrs=has_related,
            web_url=issue.get("web_url", ""),
        ))

        if fetch_notes:
            bound.info("fetch.issue_notes", issue_iid=iid)
            for note in client.get_issue_notes(project_id, iid):
                note_records.append(IssueNoteRecord(
                    project_id=project_id,
                    project_url=project_url,
                    issue_iid=iid,
                    note_id=note.get("id"),
                    author_username=(note.get("author") or {}).get("username", ""),
                    created_at=note.get("created_at", ""),
                    body=(note.get("body") or "")[:500],
                ))

    return issue_records, note_records


def _fetch_events(
    client: GitLabClient,
    project_id: int,
    project_url: str,
    events_limit: int | None,
) -> list[EventRecord]:
    """Fetch the activity stream for one project."""
    log.info("fetch.events", repo=project_url, project_id=project_id)
    records: list[EventRecord] = []
    for ev in client.get_events(project_id, limit=events_limit):
        records.append(EventRecord(
            project_id=project_id,
            project_url=project_url,
            event_id=ev.get("id"),
            author_username=(ev.get("author") or {}).get("username", ""),
            action_name=ev.get("action_name", ""),
            target_type=ev.get("target_type") or "",
            target_iid=ev.get("target_iid"),
            created_at=ev.get("created_at", ""),
        ))
    return records


def _fetch_members(
    client: GitLabClient,
    project_id: int,
    project_url: str,
) -> list[MemberRecord]:
    """Fetch the direct member roster for one project."""
    log.info("fetch.members", repo=project_url, project_id=project_id)
    records: list[MemberRecord] = []
    for m in client.get_members(project_id):
        records.append(MemberRecord(
            project_id=project_id,
            project_url=project_url,
            user_id=m.get("id"),
            username=m.get("username", ""),
            name=m.get("name", ""),
            access_level=m.get("access_level"),
            state=m.get("state", ""),
        ))
    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    _configure_logging(debug=DEBUG, log_file=LOG_FILE)

    token = load_token()
    base_url = load_base_url()

    log.info(
        "scanner.start",
        base_url=base_url,
        authenticated=token is not None,
        repos_file=REPOS_FILE,
        output_dir=OUTPUT_DIR,
    )

    try:
        repos = read_repos(REPOS_FILE)
    except FileNotFoundError:
        log.error("scanner.repos_file_not_found", path=REPOS_FILE)
        sys.exit(1)

    if not repos:
        log.warning("scanner.no_repos", path=REPOS_FILE)
        sys.exit(0)

    log.info("scanner.repos_loaded", count=len(repos))
    client = GitLabClient(base_url=base_url, token=token)

    # Accumulate rows across all repos
    snapshots: list[ProjectSnapshot] = []
    all_issues: list[IssueRecord] = []
    all_notes: list[IssueNoteRecord] = []
    all_events: list[EventRecord] = []
    all_members: list[MemberRecord] = []

    for repo_url in repos:
        # Top-level project snapshot
        snapshot = _fetch_snapshot(client, repo_url)
        snapshots.append(snapshot)

        if snapshot.fetch_errors:
            log.warning("fetch.partial_errors", repo=repo_url, errors=snapshot.fetch_errors)
        else:
            log.info("fetch.snapshot_complete", repo=repo_url)

        if snapshot.project_id is None:
            log.warning("fetch.skipping_detail", repo=repo_url,
                        reason="project_id not available")
            continue

        pid = snapshot.project_id

        # Issues (+ optional notes / related MRs)
        issues, notes = _fetch_issues(
            client,
            project_id=pid,
            project_url=repo_url,
            issues_state=ISSUES_STATE,
            issues_since=ISSUES_SINCE,
            issues_limit=ISSUES_LIMIT,
            fetch_notes=FETCH_NOTES,
            fetch_related_mrs=FETCH_RELATED_MRS,
        )
        all_issues.extend(issues)
        all_notes.extend(notes)
        log.info("fetch.issues_complete", repo=repo_url, issues=len(issues), notes=len(notes))

        # Events
        events = _fetch_events(client, pid, repo_url, EVENTS_LIMIT)
        all_events.extend(events)
        log.info("fetch.events_complete", repo=repo_url, events=len(events))

        # Members
        members = _fetch_members(client, pid, repo_url)
        all_members.extend(members)
        log.info("fetch.members_complete", repo=repo_url, members=len(members))

    # Build DataFrames
    project_rows = []
    for s in snapshots:
        row = asdict(s)
        row["fetch_errors"] = "; ".join(row["fetch_errors"])
        project_rows.append(row)

    tables: dict[str, pd.DataFrame] = {
        "projects": pd.DataFrame(project_rows),
    }
    if all_issues:
        tables["issues"] = pd.DataFrame([asdict(i) for i in all_issues])
    if all_notes:
        tables["notes"] = pd.DataFrame([asdict(n) for n in all_notes])
    if all_events:
        tables["events"] = pd.DataFrame([asdict(e) for e in all_events])
    if all_members:
        tables["members"] = pd.DataFrame([asdict(m) for m in all_members])

    # Save
    saved_paths = save_tables(tables, output_dir=OUTPUT_DIR)

    log.info(
        "scanner.done",
        repos_processed=len(snapshots),
        tables_written=list(tables.keys()),
        output_files=[str(p) for p in saved_paths],
    )


if __name__ == "__main__":
    main()
