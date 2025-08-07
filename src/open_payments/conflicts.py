import pandas as pd


class Conflicts:
    """Parent class to analyze conflicts of interest
    or disclosures in the context of OpenPayments payments."""

    def __init__(
        self,
        conflicts: pd.DataFrame,
    ):
        self.conflicts = conflicts
