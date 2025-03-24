import os

from typing import Union, Literal


def get_file_suffix(
    years: Union[
            list[Literal[2020, 2021, 2022, 2023]],
            Literal[2020, 2021, 2022, 2023],
        ],
        payment_classes: Union[
            list[Literal["general", "ownership", "research"]],
            Literal["general", "ownership", "research"],
            None,
        ],
) -> str:
    return (
            f"_{'_'.join(payment_classes)}_{('_'.join([str(year) for year in years] if isinstance(years, list) else [str(years)]))}"
            if (
                isinstance(years, list) and any(
                    year not in years for year in [2020, 2021, 2022, 2023]
                ) or isinstance(years, int) and years not in [2020, 2021, 2022, 2023]
            )
            or (payment_classes is not None and any(payment_class not in payment_classes for payment_class in ["general", "research", "ownership"]))
            else ""
        )


def open_payments_directory() -> str:
    return os.path.join(os.path.expanduser('~'), 'open_payments_datasets')