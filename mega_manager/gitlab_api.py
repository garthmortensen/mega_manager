"""GitLab REST API v4 client.

Each public method maps directly to one API endpoint, makes the HTTP request
itself, logs what it is doing, and returns the data (or None on error).
There are no internal helpers — requests are made inline so the flow is easy
to follow.
"""

import requests
import structlog

log = structlog.get_logger(__name__)


class GitLabClient:
    """Read-only client for the GitLab REST API v4.

    Parameters
    ----------
    base_url:
        Root URL of the GitLab instance, e.g. ``https://gitlab.com``.
    token:
        Personal / project access token.  When None the client uses
        unauthenticated requests (public repos only, lower rate limits).
    """

    def __init__(self, base_url: str, token: str | None = None) -> None:
        self.base = base_url.rstrip("/") + "/api/v4"
        self.headers = {"Accept": "application/json"}
        if token:
            self.headers["PRIVATE-TOKEN"] = token

    def get_project(self, encoded_path: str) -> dict | None:
        """Fetch top-level project metadata and return it as a dict.

        GET /api/v4/projects/{encoded_path}
        Returns None if the project is not found or the request fails.
        """
        url = f"{self.base}/projects/{encoded_path}"
        log.info("gitlab_api.get_project", url=url)
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
        except requests.RequestException as exc:
            log.error("gitlab_api.request_failed", url=url, error=str(exc))
            return None

        if resp.status_code == 404:
            log.warning("gitlab_api.not_found", url=url)
            return None
        if resp.status_code == 401:
            log.warning("gitlab_api.unauthorized", url=url,
                        hint="Set GITLAB_TOKEN in .env to access private repos")
            return None
        if not resp.ok:
            log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
            return None

        return resp.json()

    def get_open_mr_count(self, project_id: int | str) -> int | None:
        """Return the total number of open merge requests.

        Reads the ``X-Total`` response header so only one page is fetched.
        GET /api/v4/projects/{id}/merge_requests?state=opened&per_page=1
        Returns None if the request fails.
        """
        url = f"{self.base}/projects/{project_id}/merge_requests"
        params = {"state": "opened", "per_page": 1}
        log.info("gitlab_api.get_open_mr_count", url=url, project_id=project_id)
        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=15)
        except requests.RequestException as exc:
            log.error("gitlab_api.request_failed", url=url, error=str(exc))
            return None

        if not resp.ok:
            log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
            return None

        # X-Total is set by GitLab pagination and reflects the full count
        total = resp.headers.get("X-Total")
        if total is not None:
            return int(total)
        return len(resp.json())

    def get_latest_pipeline(self, project_id: int | str) -> dict | None:
        """Return the most recent pipeline object, or None.

        GET /api/v4/projects/{id}/pipelines?per_page=1
        Returns None if there are no pipelines or the request fails.
        """
        url = f"{self.base}/projects/{project_id}/pipelines"
        params = {"per_page": 1}
        log.info("gitlab_api.get_latest_pipeline", url=url, project_id=project_id)
        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=15)
        except requests.RequestException as exc:
            log.error("gitlab_api.request_failed", url=url, error=str(exc))
            return None

        if not resp.ok:
            log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
            return None

        pipelines = resp.json()
        return pipelines[0] if pipelines else None

    def get_languages(self, project_id: int | str) -> dict:
        """Return the language breakdown as ``{language: percentage}``.

        GET /api/v4/projects/{id}/languages
        Returns an empty dict if the request fails.
        """
        url = f"{self.base}/projects/{project_id}/languages"
        log.info("gitlab_api.get_languages", url=url, project_id=project_id)
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
        except requests.RequestException as exc:
            log.error("gitlab_api.request_failed", url=url, error=str(exc))
            return {}

        if not resp.ok:
            log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
            return {}

        return resp.json()

    def get_latest_commit(self, project_id: int | str) -> dict | None:
        """Return the most recent commit object on the default branch.

        GET /api/v4/projects/{id}/repository/commits?per_page=1
        Returns None if the repository is empty or the request fails.
        """
        url = f"{self.base}/projects/{project_id}/repository/commits"
        params = {"per_page": 1}
        log.info("gitlab_api.get_latest_commit", url=url, project_id=project_id)
        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=15)
        except requests.RequestException as exc:
            log.error("gitlab_api.request_failed", url=url, error=str(exc))
            return None

        if not resp.ok:
            log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
            return None

        commits = resp.json()
        return commits[0] if commits else None

    def get_issues(
        self,
        project_id: int | str,
        state: str = "all",
        since: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """Return issues for a project, newest-updated-first.  All pages are fetched.

        GET /api/v4/projects/{id}/issues

        Parameters
        ----------
        state:  ``"opened"``, ``"closed"``, or ``"all"`` (default).
        since:  ISO 8601 string — only issues updated on or after this date.
        limit:  Stop after this many issues (useful for large repos).
        """
        url = f"{self.base}/projects/{project_id}/issues"
        params: dict = {
            "per_page": 100,
            "state": state,
            "order_by": "updated_at",
            "sort": "desc",
        }
        if since:
            params["updated_after"] = since

        issues: list[dict] = []
        page = 1
        while True:
            params["page"] = page
            log.info("gitlab_api.get_issues", url=url, page=page, project_id=project_id)
            try:
                resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            except requests.RequestException as exc:
                log.error("gitlab_api.request_failed", url=url, error=str(exc))
                break
            if not resp.ok:
                log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
                break
            batch = resp.json()
            if not batch:
                break
            issues.extend(batch)
            if limit and len(issues) >= limit:
                issues = issues[:limit]
                break
            next_page = resp.headers.get("X-Next-Page", "")
            if not next_page:
                break
            page = int(next_page)

        log.info("gitlab_api.get_issues_done", project_id=project_id, total=len(issues))
        return issues

    def get_issue_notes(self, project_id: int | str, issue_iid: int | str) -> list[dict]:
        """Return all notes (comments) for one issue, oldest-first.

        GET /api/v4/projects/{id}/issues/{iid}/notes
        """
        url = f"{self.base}/projects/{project_id}/issues/{issue_iid}/notes"
        params: dict = {"per_page": 100}

        notes: list[dict] = []
        page = 1
        while True:
            params["page"] = page
            log.debug("gitlab_api.get_issue_notes", url=url, page=page, issue_iid=issue_iid)
            try:
                resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            except requests.RequestException as exc:
                log.error("gitlab_api.request_failed", url=url, error=str(exc))
                break
            if not resp.ok:
                log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
                break
            batch = resp.json()
            if not batch:
                break
            notes.extend(batch)
            next_page = resp.headers.get("X-Next-Page", "")
            if not next_page:
                break
            page = int(next_page)

        return notes

    def get_issue_related_mrs(
        self, project_id: int | str, issue_iid: int | str
    ) -> list[dict]:
        """Return merge requests linked to an issue.

        GET /api/v4/projects/{id}/issues/{iid}/related_merge_requests
        Returns an empty list if the request fails.
        """
        url = f"{self.base}/projects/{project_id}/issues/{issue_iid}/related_merge_requests"
        log.debug("gitlab_api.get_issue_related_mrs", url=url, issue_iid=issue_iid)
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
        except requests.RequestException as exc:
            log.error("gitlab_api.request_failed", url=url, error=str(exc))
            return []
        if not resp.ok:
            log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
            return []
        return resp.json()

    def get_events(
        self, project_id: int | str, limit: int | None = None
    ) -> list[dict]:
        """Return the project activity stream, newest-first.

        GET /api/v4/projects/{id}/events

        Parameters
        ----------
        limit:  Stop after this many events.
        """
        url = f"{self.base}/projects/{project_id}/events"
        params: dict = {"per_page": 100}

        events: list[dict] = []
        page = 1
        while True:
            params["page"] = page
            log.info("gitlab_api.get_events", url=url, page=page, project_id=project_id)
            try:
                resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            except requests.RequestException as exc:
                log.error("gitlab_api.request_failed", url=url, error=str(exc))
                break
            if not resp.ok:
                log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
                break
            batch = resp.json()
            if not batch:
                break
            events.extend(batch)
            if limit and len(events) >= limit:
                events = events[:limit]
                break
            next_page = resp.headers.get("X-Next-Page", "")
            if not next_page:
                break
            page = int(next_page)

        log.info("gitlab_api.get_events_done", project_id=project_id, total=len(events))
        return events

    def get_members(self, project_id: int | str) -> list[dict]:
        """Return all direct members of the project.

        GET /api/v4/projects/{id}/members
        Returns an empty list if the request fails.
        """
        url = f"{self.base}/projects/{project_id}/members"
        log.info("gitlab_api.get_members", url=url, project_id=project_id)
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
        except requests.RequestException as exc:
            log.error("gitlab_api.request_failed", url=url, error=str(exc))
            return []
        if not resp.ok:
            log.error("gitlab_api.http_error", url=url, status_code=resp.status_code)
            return []
        return resp.json()
