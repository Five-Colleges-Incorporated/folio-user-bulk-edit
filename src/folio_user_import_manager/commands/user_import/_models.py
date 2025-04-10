"""Models for import command."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ImportOptions:
    """Options used for importing users into FOLIO."""

    folio_url: str
    folio_tenant: str
    folio_username: str
    folio_password: str

    batch_size: int
    max_concurrency: int
    retry_count: int
    failed_user_threshold: float

    deactivate_missing_users: bool
    update_all_fields: bool
    source_type: str | None

    data_location: Path | dict[str, Path]


@dataclass
class ImportResults:
    """Results of importing users into FOLIO."""
