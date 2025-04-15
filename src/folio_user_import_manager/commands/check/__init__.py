"""Command for quickly checking required inputs."""

from folio_user_import_manager.folio import Folio

from ._data import run as data
from ._models import CheckOptions, CheckResults


def run(options: CheckOptions) -> CheckResults:
    """Checks for connectivity and data validity."""
    return CheckResults(Folio(options).test(), *data(options))


__all__ = ["CheckOptions", "CheckResults", "run"]
