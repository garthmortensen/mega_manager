"""Repo list reader and URL utilities."""

from pathlib import Path
from urllib.parse import quote, urlparse


def read_repos(path: str | Path = "repos.txt") -> list[str]:
    """Read repository URLs from a plain-text file (one URL per line).

    Blank lines and lines beginning with '#' are ignored.
    """
    lines: list[str] = []
    for raw in Path(path).read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            lines.append(line)
    return lines


def parse_repo_path(url: str) -> str:
    """Extract the URL-encoded ``namespace/project`` path from a GitLab URL.

    Examples
    --------
    >>> parse_repo_path("https://gitlab.com/gitlab-org/gitlab")
    'gitlab-org%2Fgitlab'
    >>> parse_repo_path("https://gitlab.com/gitlab-com/runbooks")
    'gitlab-com%2Frunbooks'
    """
    parsed = urlparse(url)
    # Strip leading slash; e.g. "/gitlab-org/gitlab" → "gitlab-org/gitlab"
    path = parsed.path.lstrip("/")
    # URL-encode the slash so it can be used as a single path segment in the API.
    return quote(path, safe="")
