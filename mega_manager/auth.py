"""Auth helpers — load GitLab credentials from .env or environment variables.

Mirrors the token_in_env() pattern from starter/determine_package_health.py
but uses python-dotenv for cleaner .env file support.
"""

import os

from dotenv import load_dotenv

_DEFAULT_BASE_URL = "https://gitlab.com"


def load_token() -> str | None:
    """Return the GitLab private token, or None if not configured.

    Resolution order:
    1. GITLAB_TOKEN in process environment (already exported)
    2. GITLAB_TOKEN from a .env file in the current working directory
    """
    load_dotenv()
    token = os.environ.get("GITLAB_TOKEN", "").strip()
    return token if token else None


def load_base_url() -> str:
    """Return the GitLab base URL (default: https://gitlab.com).

    Useful for self-managed GitLab instances.  Set GITLAB_BASE_URL in .env
    or the environment to override.
    """
    load_dotenv()
    url = os.environ.get("GITLAB_BASE_URL", "").strip().rstrip("/")
    return url if url else _DEFAULT_BASE_URL
