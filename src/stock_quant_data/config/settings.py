"""
Central application settings for stock-quant-data-loader.

Design goals:
- single current repo only
- explicit canonical paths
- no hidden legacy aliases
- easy to audit
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Runtime settings for the current loader repo.

    Notes for future developers:
    - We resolve repo_root from this file location, not from cwd.
    - That avoids subtle import / execution bugs when scripts are launched
      from another directory.
    - All DB/table jobs in this repo must use these canonical paths.
    """

    model_config = SettingsConfigDict(
        env_prefix="SQD_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    repo_root: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parents[3]
    )

    # ------------------------------------------------------------------
    # Canonical data roots for the CURRENT loader repo.
    # ------------------------------------------------------------------
    data_dir: Path | None = None
    build_dir: Path | None = None
    build_db_path: Path | None = None

    # ------------------------------------------------------------------
    # Optional cross-repo inputs.
    # These point at the downloader repo outputs when the repos are checked
    # out side-by-side under ~/stock-quant-data-*.
    # ------------------------------------------------------------------
    downloader_repo_root: Path | None = None
    downloader_data_dir: Path | None = None

    def model_post_init(self, __context: object) -> None:
        """
        Fill derived paths after settings initialization.
        """
        if self.data_dir is None:
            self.data_dir = self.repo_root / "data"

        if self.build_dir is None:
            self.build_dir = self.data_dir / "build"

        if self.build_db_path is None:
            self.build_db_path = self.build_dir / "market_build.duckdb"

        if self.downloader_repo_root is None:
            self.downloader_repo_root = self.repo_root.parent / "stock-quant-data-downloader"

        if self.downloader_data_dir is None:
            self.downloader_data_dir = self.downloader_repo_root / "data"

    def ensure_directories(self) -> None:
        """
        Create only the directories owned by this repo.

        We intentionally do NOT create arbitrary downloader directories here.
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.build_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
