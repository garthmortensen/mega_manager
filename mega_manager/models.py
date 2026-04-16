"""Data model for a single repository snapshot."""

from dataclasses import dataclass, field


@dataclass
class ProjectSnapshot:
    """All PM-relevant fields collected for one GitLab project.

    Fields that could not be fetched are left as None / empty string.
    Any non-fatal errors during fetching are concatenated into ``fetch_errors``.
    """

    # ---- Identity -------------------------------------------------------
    repo_url: str = ""
    name_with_namespace: str = ""
    description: str = ""
    web_url: str = ""

    # ---- Visibility / health --------------------------------------------
    visibility: str = ""
    archived: bool | None = None
    default_branch: str = ""
    topics: str = ""           # comma-joined list for flat CSV output

    # ---- Activity metrics -----------------------------------------------
    star_count: int | None = None
    forks_count: int | None = None
    open_issues_count: int | None = None
    open_mr_count: int | None = None

    # ---- Timestamps -----------------------------------------------------
    created_at: str = ""
    last_activity_at: str = ""

    # ---- Latest pipeline ------------------------------------------------
    latest_pipeline_status: str = ""
    latest_pipeline_created_at: str = ""

    # ---- Languages ------------------------------------------------------
    top_language: str = ""
    languages_json: str = ""   # full breakdown as JSON string

    # ---- Latest commit --------------------------------------------------
    latest_commit_sha: str = ""
    latest_commit_message: str = ""
    latest_commit_date: str = ""
    latest_commit_author: str = ""

    # ---- Internal / join key --------------------------------------------
    project_id: int | None = None

    # ---- Meta -----------------------------------------------------------
    fetch_errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Issue-level models (one row per issue / note / event / member)
# ---------------------------------------------------------------------------


@dataclass
class IssueRecord:
    """One row in the issues fact table."""

    project_id: int | None = None
    project_url: str = ""
    issue_iid: int | None = None
    issue_id: int | None = None
    title: str = ""
    state: str = ""                  # opened / closed
    created_at: str = ""
    updated_at: str = ""
    closed_at: str = ""
    labels: str = ""                 # comma-joined
    milestone_title: str = ""
    milestone_id: int | None = None
    assignees: str = ""              # comma-joined usernames
    weight: int | None = None
    time_estimate: int | None = None         # seconds
    total_time_spent: int | None = None      # seconds
    has_related_mrs: bool | None = None
    web_url: str = ""


@dataclass
class IssueNoteRecord:
    """One comment on an issue."""

    project_id: int | None = None
    project_url: str = ""
    issue_iid: int | None = None
    note_id: int | None = None
    author_username: str = ""
    created_at: str = ""
    body: str = ""          # truncated to 500 chars


@dataclass
class EventRecord:
    """One entry from the project activity stream."""

    project_id: int | None = None
    project_url: str = ""
    event_id: int | None = None
    author_username: str = ""
    action_name: str = ""            # e.g. opened, closed, commented, pushed
    target_type: str = ""            # e.g. Issue, MergeRequest, Note
    target_iid: int | None = None
    created_at: str = ""


@dataclass
class MemberRecord:
    """One direct project member."""

    project_id: int | None = None
    project_url: str = ""
    user_id: int | None = None
    username: str = ""
    name: str = ""
    access_level: int | None = None  # 10=Guest 20=Reporter 30=Developer 40=Maintainer 50=Owner
    state: str = ""
