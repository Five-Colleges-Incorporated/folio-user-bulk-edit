"""Panderas Schemas for FOLIO user data."""

import json
from datetime import UTC, datetime
from urllib.parse import urlparse

import pandera.polars as pla
import polars as pl
import polars.selectors as cs

# https://dev.folio.org/guides/uuids/
_FOLIO_UUID = (
    r""
    r"^[a-fA-F0-9]{8}-"
    r"[a-fA-F0-9]{4}-"
    r"[1-5][a-fA-F0-9]{3}-"
    r"[89abAB][a-fA-F0-9]{3}-"
    r"[a-fA-F0-9]{12}$"
)


class _SubSchema:
    def __init__(self, prefix: str, req_cols: list[str]) -> None:
        self._prefix = prefix
        self._req_cols = {f"{prefix}_{col}" for col in req_cols}
        self._cs_required = cs.by_name(
            self._req_cols,
            require_all=False,
        )
        self._cs_prefix = cs.starts_with(self._prefix)

    def _agg(self, data: pla.PolarsData) -> pl.LazyFrame:
        noerr = f"{self._prefix}_atleastonekey"
        return (
            data.lazyframe.with_columns(
                pl.lit(1).alias(noerr),
            )
            .group_by(self._cs_prefix - self._cs_required)
            .agg(self._cs_required.has_nulls().not_())
            .select(self._cs_prefix - cs.by_name(noerr))
        )

    def required(self, data: pla.PolarsData) -> bool:
        pref_cols = set(self._agg(data).collect_schema().names())
        if len(pref_cols) == 0:
            # there are no no requestPreference columns
            return True
        return len(self._req_cols - pref_cols) == 0

    def not_nullable(self, data: pla.PolarsData) -> bool:
        pref_cols = self._agg(data).collect().to_dict(as_series=False)
        if len(pref_cols) == 0:
            # there are no no requestPreference columns
            return True
        return all(all(v) for (k, v) in pref_cols.items() if k in self._req_cols)


class _RequiredUserImportSchema(pla.DataFrameModel):
    username: str = pla.Field(unique=True)
    externalSystemId: str = pla.Field(unique=True)


class _BaseUserImportSchema(pla.DataFrameModel):
    id: str | None = pla.Field(nullable=True, unique=True, str_matches=_FOLIO_UUID)
    barcode: str | None = pla.Field(nullable=True)
    active: bool | None = pla.Field(nullable=True)
    type: str | None = pla.Field(nullable=True, isin=["Patron", "Staff"])
    patronGroup: str | None = pla.Field(nullable=True)
    departments: str | None = pla.Field(nullable=True)
    enrollmentDate: pl.Date | None = pla.Field(nullable=True)
    expirationDate: pl.Date | None = pla.Field(nullable=True)
    preferredEmailCommunication: str | None = pla.Field(nullable=True)

    @pla.check("departments", element_wise=True)
    @classmethod
    def unique_departments(cls, depts: str) -> bool:
        all_vals = depts.split(",")
        unique_vals = set(all_vals)
        return len(unique_vals) == len(all_vals)

    @pla.check("preferredEmailCommunication", element_wise=True)
    @classmethod
    def valid_preferences(cls, prefs: str) -> bool:
        all_vals = prefs.split(",")
        unique_vals = set(all_vals)
        return (
            len(unique_vals) == len(all_vals)
            and len(unique_vals - {"Support", "Programs", "Services"}) == 0
        )

    @pla.dataframe_check
    @classmethod
    def active_expired(cls, data: pla.PolarsData) -> pl.LazyFrame:
        now = datetime.now(tz=UTC).date()
        cols = set(data.lazyframe.collect_schema().names())
        if len({"active", "expirationDate"} - cols) == 0:
            return data.lazyframe.select(
                pl.Expr.or_(
                    pl.col("active").not_(),
                    pl.col("expirationDate").is_null(),
                    pl.col("expirationDate").gt(pl.lit(now)),
                ),
            )
        return data.lazyframe.select(pl.lit(True))


class _RequestPreferencesSchema(pla.DataFrameModel):
    requestPreference_id: str | None = pla.Field(
        unique=True,
        nullable=True,
        str_matches=_FOLIO_UUID,
    )
    requestPreference_holdShelf: bool | None = pla.Field(nullable=True)
    requestPreference_delivery: bool | None = pla.Field(nullable=True)
    requestPreference_defaultServicePointId: str | None = pla.Field(
        nullable=True,
        str_matches=_FOLIO_UUID,
    )
    requestPreference_defaultDeliveryAddressTypeId: str | None = pla.Field(
        nullable=True,
    )
    requestPreference_fulfillment: str | None = pla.Field(
        nullable=True,
        isin=["Delivery", "Hold Shelf"],
    )

    _prefs_ss = _SubSchema("requestPreference", ["holdShelf", "delivery"])

    @pla.dataframe_check
    @classmethod
    def request_required_columns(cls, data: pla.PolarsData) -> bool:
        return cls._prefs_ss.required(data)

    @pla.dataframe_check
    @classmethod
    def request_not_nullable_columns(cls, data: pla.PolarsData) -> bool:
        return cls._prefs_ss.not_nullable(data)


class _PersonalSchema(pla.DataFrameModel):
    personal_lastName: str | None = pla.Field(nullable=True)
    personal_firstName: str | None = pla.Field(nullable=True)
    personal_middleName: str | None = pla.Field(nullable=True)
    personal_preferredFirstName: str | None = pla.Field(nullable=True)
    personal_email: str | None = pla.Field(
        nullable=True,
        # super naive regex
        str_matches=r"^[^@\s]+@[^@\s\.]+\.[^@\s\.]+$",
    )
    personal_phone: str | None = pla.Field(nullable=True)
    personal_mobilePhone: str | None = pla.Field(nullable=True)
    personal_dateOfBirth: pl.Date | None = pla.Field(nullable=True)
    personal_preferredContactTypeId: str | None = pla.Field(
        nullable=True,
        isin=["mail", "email", "text", "phone", "mobile"],
    )
    personal_profilePictureLink: str | None = pla.Field(nullable=True)

    @pla.check("personal_profilePictureLink", element_wise=True)
    @classmethod
    def valid_url(cls, data: str) -> bool:
        (scheme, netloc, *_) = urlparse(data)
        return all([scheme, netloc])

    _personal_ss = _SubSchema("personal", ["lastName"])

    @pla.dataframe_check
    @classmethod
    def personal_required_columns(cls, data: pla.PolarsData) -> bool:
        return cls._personal_ss.required(data)

    @pla.dataframe_check
    @classmethod
    def personal_not_nullable_columns(cls, data: pla.PolarsData) -> bool:
        return cls._personal_ss.not_nullable(data)


class _AddressesSchema(pla.DataFrameModel):
    personal_address_primary_id: str | None = pla.Field(
        nullable=True,
        str_matches=_FOLIO_UUID,
    )
    personal_address_primary_countryId: str | None = pla.Field(nullable=True)
    personal_address_primary_addressLine1: str | None = pla.Field(nullable=True)
    personal_address_primary_addressLine2: str | None = pla.Field(nullable=True)
    personal_address_primary_city: str | None = pla.Field(nullable=True)
    personal_address_primary_region: str | None = pla.Field(nullable=True)
    personal_address_primary_postalCode: str | None = pla.Field(nullable=True)
    personal_address_primary_addressTypeId: str | None = pla.Field(nullable=True)
    personal_address_primary_primaryAddress: bool | None = pla.Field(nullable=True)

    personal_address_secondary_id: str | None = pla.Field(
        nullable=True,
        str_matches=_FOLIO_UUID,
    )
    personal_address_secondary_countryId: str | None = pla.Field(nullable=True)
    personal_address_secondary_addressLine1: str | None = pla.Field(nullable=True)
    personal_address_secondary_addressLine2: str | None = pla.Field(nullable=True)
    personal_address_secondary_city: str | None = pla.Field(nullable=True)
    personal_address_secondary_region: str | None = pla.Field(nullable=True)
    personal_address_secondary_postalCode: str | None = pla.Field(nullable=True)
    personal_address_secondary_addressTypeId: str | None = pla.Field(nullable=True)
    personal_address_secondary_primaryAddress: bool | None = pla.Field(nullable=True)


class UserImportSchema(
    _RequiredUserImportSchema,
    _BaseUserImportSchema,
    _RequestPreferencesSchema,
    _PersonalSchema,
    _AddressesSchema,
):
    """Panderas DataFrameModel for FOLIO's user import endpoint."""

    tags: str | None = pla.Field(nullable=True)
    customFields: str | None = pla.Field(nullable=True)

    @pla.check("customFields", element_wise=True)
    @classmethod
    def _valid_json(cls, data: str) -> bool:
        try:
            json.loads(data)
        except ValueError:
            return False

        return True

    class Config:
        """Define DataFrameSchema-wide options."""

        strict = True
