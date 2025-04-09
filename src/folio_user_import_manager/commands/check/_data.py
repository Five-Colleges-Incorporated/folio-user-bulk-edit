import typing
from email.utils import parseaddr
from pathlib import Path
from urllib.parse import urlparse

import pandera.polars as pla
import polars as pl
import polars.selectors as cs
from pandera.engines.polars_engine import Date, DateTime

from ._models import CheckOptions

# https://dev.folio.org/guides/uuids/
_FOLIO_UUID = (
    r""
    r"^[a-fA-F0-9]{8}-"
    r"[a-fA-F0-9]{4}-"
    r"[1-5][a-fA-F0-9]{3}-"
    r"[89abAB][a-fA-F0-9]{3}-"
    r"[a-fA-F0-9]{12}$"
)


def is_url(maybe: str) -> bool:
    (scheme, netloc, *_) = urlparse(maybe)
    return all([scheme, netloc])


def val_limited_is_unique(
    vals: set[str] | None = None,
) -> typing.Callable[[str], bool]:
    def val_filter(col: str) -> bool:
        all_vals = col.split(",")
        unique_vals = set(all_vals)
        return len(unique_vals) == len(all_vals) and (
            vals is None or len(unique_vals - vals) == 0
        )

    return val_filter


class RequestPreference:
    def __init__(self) -> None:
        self.req_cols = {
            "requestPreference_holdShelf",
            "requestPreference_delivery",
        }
        self._required = cs.by_name(
            self.req_cols,
            require_all=False,
        )
        self._preferences = cs.starts_with("requestPreference_")

    def _agg(self, data: pla.PolarsData) -> dict[str, list[bool]]:
        noerr = "requestPreference_atleastonekey"
        frame = (
            data.lazyframe.with_columns(
                pl.lit(1).alias(noerr),
            )
            .group_by(self._preferences - self._required)
            .agg(self._required.has_nulls().not_())
        )

        return (
            frame.select(self._preferences - cs.by_name(noerr))
            .collect()
            .to_dict(as_series=False)
        )

    def required(self, data: pla.PolarsData) -> bool:
        pref_cols = self._agg(data)
        if len(pref_cols) == 0:
            # there are no no requestPreference columns
            return True
        return len(self.req_cols - pref_cols.keys()) == 0

    def not_nullable(self, data: pla.PolarsData) -> bool:
        pref_cols = self._agg(data)
        if len(pref_cols) == 0:
            # there are no no requestPreference columns
            return True
        return all(all(v) for (k, v) in pref_cols.items() if k in self.req_cols)


def run(
    options: CheckOptions,
) -> tuple[
    dict[str, pla.errors.SchemaErrors] | None,
    dict[str, pl.exceptions.PolarsError] | None,
]:
    schema_errors: dict[str, pla.errors.SchemaErrors] = {}
    read_errors: dict[str, pl.exceptions.PolarsError] = {}
    req_prefs = RequestPreference()
    user_data_import_schema = pla.DataFrameSchema(
        {
            "username": pla.Column(
                str,
                description="A unique name belonging to a user. "
                "Typically used for login",
                unique=True,
            ),
            "externalSystemId": pla.Column(
                str,
                description="A unique ID that corresponds to an external authority",
                unique=True,
            ),
            "id": pla.Column(
                str,
                description="A globally unique (UUID) identifier for the user",
                unique=True,
                required=False,
                nullable=True,
                checks=[pla.Check.str_matches(_FOLIO_UUID, name="folio_id")],
            ),
            "barcode": pla.Column(
                str,
                description="The unique library barcode for this user",
                unique=True,
                required=False,
                nullable=True,
            ),
            "active": pla.Column(
                bool,
                description="A flag to determine "
                "if the user's account is effective and not expired. "
                "The tenant configuration can require the user to be active for login. "
                "Active is different from the loan patron block",
                required=False,
                nullable=True,
            ),
            "type": pla.Column(
                str,
                description="The class of user like staff or patron; "
                "this is different from patronGroup; "
                "it can store shadow, system user and dcb types also",
                required=False,
                nullable=True,
                checks=[pla.Check.isin(["Patron", "Staff"])],
            ),
            "patronGroup": pla.Column(
                str,
                description="The name of the patron group the user belongs to; "
                "this is different from the patronGroup property of the /users API "
                "that is a UUID.",
                required=False,
                nullable=True,
            ),
            "departments": pla.Column(
                str,
                description="List of names of the departments the user belongs to; "
                "this is different from the departments property of the /users API"
                "this is a UUID.",  # This seems like incorrect docs
                required=False,
                nullable=True,
                checks=[
                    pla.Check(
                        val_limited_is_unique(),
                        name="unique",
                        element_wise=True,
                    ),
                ],
            ),
            "enrollmentDate": pla.Column(
                DateTime(time_zone_agnostic=True),  # type: ignore[arg-type]
                description="The date in which the user joined the organization",
                required=False,
                nullable=True,
            ),
            "expirationDate": pla.Column(
                DateTime(time_zone_agnostic=True),  # type: ignore[arg-type]
                description="The date for when the user becomes inactive",
                required=False,
                nullable=True,
            ),
            "preferredEmailCommunication": pla.Column(
                str,
                description="Preferred email communication types",
                required=False,
                nullable=True,
                checks=[
                    pla.Check(
                        val_limited_is_unique(
                            {"Support", "Programs", "Services"},
                        ),
                        element_wise=True,
                        name="unique isin",
                    ),
                ],
            ),
            "personal_lastName": pla.Column(
                str,
                description="The user's surname",
                required=False,
                nullable=True,
            ),
            "personal_firstName": pla.Column(
                str,
                description="The user's given name",
                required=False,
                nullable=True,
            ),
            "personal_middleName": pla.Column(
                str,
                description="The user's middle name (if any)",
                required=False,
                nullable=True,
            ),
            "personal_preferredFirstName": pla.Column(
                str,
                description="The user's preferred name",
                required=False,
                nullable=True,
            ),
            "personal_email": pla.Column(
                str,
                description="The user's email address",
                required=False,
                nullable=True,
                checks=[
                    pla.Check(
                        lambda e: parseaddr(e) != ("", ""),
                        element_wise=True,
                        name="invalid",
                    ),
                ],
            ),
            "personal_phone": pla.Column(
                str,
                description="The user's primary phone number",
                required=False,
                nullable=True,
            ),
            "personal_mobilePhone": pla.Column(
                str,
                description="The user's mobile phone number",
                required=False,
                nullable=True,
            ),
            "personal_dateOfBirth": pla.Column(
                Date(),  # type: ignore[arg-type]
                description="The user's birth date",
                required=False,
                nullable=True,
            ),
            "personal_preferredContactTypeId": pla.Column(
                str,
                description="Name of user's preferred contact type. "
                "One of mail, email, text, phone, mobile. "
                "This is different from the preferredContactTypeId property "
                "of the /users API that is a UUID.",
                required=False,
                nullable=True,
                checks=[pla.Check.isin(["mail", "email", "text", "phone", "mobile"])],
            ),
            "personal_profilePictureLink": pla.Column(
                str,
                description="Link to the profile picture",
                required=False,
                nullable=True,
                checks=[pla.Check(is_url, element_wise=True, name="invalid")],
            ),
            "requestPreference_id": pla.Column(
                str,
                description="Unique request preference ID",
                unique=True,
                required=False,
                nullable=True,
                checks=[pla.Check.str_matches(_FOLIO_UUID, name="folio_id")],
            ),
            "requestPreference_holdShelf": pla.Column(
                bool,
                description="Whether 'Hold Shelf' option is available to the user.",
                required=False,
                nullable=True,
            ),
            "requestPreference_delivery": pla.Column(
                bool,
                description="Whether 'Delivery' option is available to the user.",
                required=False,
                nullable=True,
            ),
            "requestPreference_defaultServicePointId": pla.Column(
                str,
                description="UUID of default service point for 'Hold Shelf' option",
                unique=True,
                required=False,
                nullable=True,
                checks=[pla.Check.str_matches(_FOLIO_UUID, name="folio_id")],
            ),
            "requestPreference_defaultDeliveryAddressTypeId": pla.Column(
                str,
                description="Name of user's address type",
                required=False,
                nullable=True,
            ),
            "requestPreference_fulfillment": pla.Column(
                str,
                description="Preferred fulfillment type. "
                "Possible values are 'Delivery', 'Hold Shelf'",
                required=False,
                nullable=True,
                checks=[pla.Check.isin(["Delivery", "Hold Shelf"])],
            ),
        },
        checks=[
            pla.Check(req_prefs.required, name="requestPreference required"),
            pla.Check(
                req_prefs.not_nullable,
                name="requestPreference SERIES_CONTAINS_NULLS",
            ),
        ],
        strict=True,
    )

    for n, p in (
        {"data": options.data_location}
        if isinstance(options.data_location, Path)
        else options.data_location
    ).items():
        try:
            pl.read_csv(p, comment_prefix="#", try_parse_dates=True)
        except pl.exceptions.PolarsError as e:
            read_errors[n] = e

        data: pl.DataFrame | None
        try:
            data = pl.read_csv(
                p,
                comment_prefix="#",
                ignore_errors=True,
                try_parse_dates=True,
            )
        except pl.exceptions.PolarsError as e:
            if n not in read_errors:
                read_errors[n] = e
            continue

        try:
            user_data_import_schema.validate(data, lazy=True)
        except pla.errors.SchemaError as se:
            schema_errors[n] = pla.errors.SchemaErrors(
                user_data_import_schema,
                [se],
                data,
            )
        except pla.errors.SchemaErrors as se:
            schema_errors[n] = se

    return (
        schema_errors if len(schema_errors) > 0 else None,
        read_errors if len(read_errors) > 0 else None,
    )
