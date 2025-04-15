"""Command for importing user data into FOLIO."""

from dataclasses import dataclass
from pathlib import Path

from folio_user_import_manager.data import InputDataOptions
from folio_user_import_manager.folio import FolioOptions


@dataclass(frozen=True)
class ImportOptions(InputDataOptions, FolioOptions):
    """Options used for importing users into FOLIO."""

    batch_size: int
    max_concurrency: int
    retry_count: int
    failed_user_threshold: float

    deactivate_missing_users: bool
    update_all_fields: bool
    source_type: str | None


@dataclass
class ImportResults:
    """Results of importing users into FOLIO."""


def run(options: ImportOptions) -> ImportResults:  # noqa: ARG001
    """Import users into FOLIO."""
    return ImportResults()


__all__ = ["ImportOptions", "ImportResults", "run"]
