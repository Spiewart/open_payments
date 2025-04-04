import os
import re

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


def str_in_str(
    to_match: str,
    string: str,
    ignore_case: bool = True,
) -> bool:
    flags = [re.IGNORECASE] if ignore_case else []
    # sub() out parentheses in the search terms
    to_match = re.sub(r"\(|\)", "", to_match)
    # sub() out brackets in the search terms
    to_match = re.sub(r"\[|\]", "", to_match)

    for i, _ in enumerate(to_match):
        if (
            # Check for a substitution
            re.search(f"{to_match[:i]}.{to_match[i+1:]}", string, *flags)
            # Check for an addition
            or re.search(f"{to_match[:i]}.{to_match[i:]}", string, *flags)
            # Check for a deletion
            or re.search(f"{to_match[:i]}{to_match[i+1:]}", string, *flags)
        ):
            return True
    return False