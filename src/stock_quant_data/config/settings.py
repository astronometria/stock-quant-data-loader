"""
Application settings.

Important design note:
- Build DB and serving release paths are intentionally separate.
- Paths are resolved relative to the project root, not the current shell cwd.
- The current release symlink path must remain the symlink path itself.
- We must NOT resolve the symlink path when we intend to atomically replace it.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def _project_root() -> Path:
    """
    Resolve the repository root from this source file location.

    File:
        src/stock_quant_data/config/settings.py

    parents[3]:
        repo_root/
    """
    return Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    """
    Centralized runtime settings.

    Raw environment values are stored as strings.
    Final filesystem paths are resolved against the project root so that
    all runtime components behave consistently regardless of cwd.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    sq_build_db_path: str = "data/build/market_build.duckdb"
    sq_releases_root: str = "data/releases"
    sq_current_release_link: str = "data/current"
    sq_env: str = "dev"
    sq_api_host: str = "127.0.0.1"
    sq_api_port: int = 8000

    @property
    def project_root(self) -> Path:
        """Return the absolute repository root."""
        return _project_root()

    @property
    def build_db_path(self) -> Path:
        """Return the absolute build DB path."""
        return (self.project_root / self.sq_build_db_path).resolve()

    @property
    def releases_root(self) -> Path:
        """Return the absolute releases root path."""
        return (self.project_root / self.sq_releases_root).resolve()

    @property
    def current_release_link(self) -> Path:
        """
        Return the absolute current-release symlink path.

        Important:
        - this must remain the path of the symlink itself
        - do NOT call .resolve() here
        - publication needs to replace this symlink atomically
        """
        return self.project_root / self.sq_current_release_link

    @property
    def current_release_target(self) -> Path | None:
        """
        Return the resolved target of the current release symlink, if it exists.

        This is useful for diagnostics and API metadata.
        """
        link_path = self.current_release_link

        if not link_path.exists():
            return None

        return link_path.resolve()

    @property
    def current_release_db_path(self) -> Path:
        """
        Return the serving DB path for the active release.

        Expected layout:
            <repo>/data/current/serving.duckdb
        """
        return self.current_release_link / "serving.duckdb"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Cached settings factory.

    Using a cache avoids reparsing environment variables repeatedly.
    """
    return Settings()
