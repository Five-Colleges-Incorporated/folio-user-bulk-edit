"""FOLIO connection related utils for managing users."""

from collections.abc import Iterator
from contextlib import contextmanager

from pyfolioclient import FolioBaseClient


class Folio:
    """The FOLIO connection factory."""

    def __init__(
        self,
        base_url: str,
        tenant: str,
        username: str,
        password: str,
    ) -> None:
        """Initializes a new instance of FOLIO."""
        self._base_url = base_url
        self._tenant = tenant
        self._username = username
        self._password = password

    @contextmanager
    def connect(self) -> Iterator[FolioBaseClient]:
        """Connects to FOLIO and returns a pyfolioclient."""
        with FolioBaseClient(
            self._base_url,
            self._tenant,
            self._username,
            self._password,
        ) as c:
            yield c

    def test(self) -> bool:
        """Test that connection to FOLIO is ok.

        It will not handle exceptions and should be called in try block.

        :returns the result of the healthcheck call
        """
        with FolioBaseClient(
            self._base_url,
            self._tenant,
            self._username,
            self._password,
        ) as folio:
            return bool(folio.get_data("/admin/health")[0] == "OK")
