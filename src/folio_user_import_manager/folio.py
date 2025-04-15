"""FOLIO connection related utils for managing users."""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import httpx
import pyfolioclient as pfc


@dataclass(frozen=True)
class FolioOptions:
    """Options used for connecting to FOLIO."""

    folio_url: str
    folio_tenant: str
    folio_username: str
    folio_password: str


class Folio:
    """The FOLIO connection factory."""

    def __init__(self, options: FolioOptions) -> None:
        """Initializes a new instance of FOLIO."""
        self._options = options

    @contextmanager
    def connect(self) -> Iterator[pfc.FolioBaseClient]:
        """Connects to FOLIO and returns a pyfolioclient."""
        with pfc.FolioBaseClient(
            self._options.folio_url,
            self._options.folio_tenant,
            self._options.folio_username,
            self._options.folio_password,
        ) as c:
            yield c

    def test(self) -> str | None:
        """Test that connection to FOLIO is ok."""
        try:
            with self.connect() as _:
                return None

        except (httpx.UnsupportedProtocol, ConnectionError):
            return "Invalid FOLIO Url"
        except pfc.BadRequestError:
            return "Invalid Tenant"
        except (pfc.UnprocessableContentError, RuntimeError):
            return "Could Not Login"
