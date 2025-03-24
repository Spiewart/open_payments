from .credentials import PaymentCredentials
from .ids import PaymentIDs
from .specialtys import PaymentSpecialtys
from .types import PaymentTypes


def unique_credentials() -> None:
    """Creates an Excel file containing unique credentials."""

    PaymentCredentials(nrows=None, years=2023).create_unique_credentials_excel()


def unique_MD_DO_payment_ids() -> None:
    """Creates an Excel file containing unique payment IDs."""

    PaymentIDs(nrows=None).create_unique_MD_DO_payment_ids_excel()


def unique_specialties() -> None:
    """Creates an Excel file containing unique specialties."""

    PaymentSpecialtys(nrows=None, years=2023).create_unique_specialtys_excel()


def general_payment_types() -> None:
    """Creates an Excel file containing unique payment types."""

    PaymentTypes(payment_classes="general").create_payment_types_excel()