"""Command for quickly checking required inputs."""

from folio_user_import_manager.data import InputData
from folio_user_import_manager.folio import Folio

from ._models import CheckOptions, CheckResults


def run(options: CheckOptions) -> CheckResults:
    """Checks for connectivity and data validity."""
    return CheckResults(Folio(options).test(), *InputData(options).test())


__all__ = ["CheckOptions", "CheckResults", "run"]
