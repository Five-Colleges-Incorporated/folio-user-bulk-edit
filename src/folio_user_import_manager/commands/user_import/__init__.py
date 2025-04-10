"""Command for importing user data into FOLIO."""

from ._models import ImportOptions, ImportResults


def run(options: ImportOptions) -> ImportResults:  # noqa: ARG001
    """Import users into FOLIO."""
    return ImportResults()


__all__ = ["ImportOptions", "ImportResults", "run"]
