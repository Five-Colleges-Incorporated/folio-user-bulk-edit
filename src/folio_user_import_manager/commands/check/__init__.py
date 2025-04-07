"""Command for quickly checking required inputs."""

from ._data import run as data
from ._folio import run as folio
from ._models import CheckOptions, CheckResults


def run(options: CheckOptions) -> CheckResults:
    """Checks for connectivity and data validity."""
    return CheckResults(folio(options), *data(options))


__all__ = ["CheckOptions", "CheckResults", "run"]
