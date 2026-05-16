from enum import StrEnum


class Credentials(StrEnum):
    """Provider credentials recognized by the matcher.

    The first 12 values mirror CMS Open Payments `Covered_Recipient_Primary_Type`
    taxonomy. `REGISTERED_NURSE` is added so child apps can faithfully record
    RN-credentialed providers in their conflicted input even though CMS does
    not currently report RN as a primary type — RN-only providers will simply
    fail to match any payment row (the correct outcome).
    """

    MEDICAL_DOCTOR = "Medical Doctor"
    DOCTOR_OF_DENTISTRY = "Doctor of Dentistry"
    DOCTOR_OF_OSTEOPATHY = "Doctor of Osteopathy"
    DOCTOR_OF_OPTOMETRY = "Doctor of Optometry"
    CHIROPRACTOR = "Chiropractor"
    DOCTOR_OF_PODIATRIC_MEDICINE = "Doctor of Podiatric Medicine"
    NURSE_PRACTITIONER = "Nurse Practitioner"
    PHYSICIAN_ASSISTANT = "Physician Assistant"
    CERTIFIED_REGISTERED_NURSE_ANAESTHETIST = "Certified Registered Nurse Anesthetist"
    CLINICAL_NURSE_SPECIALIST = "Clinical Nurse Specialist"
    CERTIFIED_NURSE_MIDWIFE = "Certified Nurse-Midwife"
    ANESTHESIOLOGIST_ASSISTANT = "Anesthesiologist Assistant"
    REGISTERED_NURSE = "Registered Nurse"


class PaymentFilters(StrEnum):
    """Enum class for the various stages at which a conflicted provider
    can be identified from the list of unique OpenPayment IDs."""

    LASTNAME = "LASTNAME"
    # 1-edit-distance lastname match (insertion/deletion/substitution).
    # Tagged separately from LASTNAME so the SELECTION layer can treat
    # partial-match hits as lower-confidence than exact hits. Fires from
    # the third fallback path in `merge_by_last_name` when an exact match
    # and a contains-match both return empty. Naming intentionally mirrors
    # FIRSTNAME_PARTIAL so non-coder analysts see a consistent
    # exact / _PARTIAL pair across name components.
    LASTNAME_PARTIAL = "LASTNAME_PARTIAL"
    FIRSTNAME = "FIRSTNAME"
    FIRSTNAME_PARTIAL = "FIRSTNAME_PARTIAL"  # Matches first name with a partial match
    FIRST_MIDDLE_NAME = "FIRST_MIDDLE_NAME"  # Matches first name and middle name
    CREDENTIAL = "CREDENTIAL"
    SPECIALTY = "SPECIALTY"
    SUBSPECIALTY = "SUBSPECIALTY"
    FULLSPECIALTY = "FULLSPECIALTY"  # Matches specialty and subspecialty
    MIDDLE_INITIAL = "MIDDLE_INITIAL"
    MIDDLENAME = "MIDDLENAME"
    CITY = "CITY"
    STATE = "STATE"
    CITYSTATE = "CITYSTATE"  # Matches city and state
    # Identifier filter. Application is the same as the others (accumulate
    # the label in `filters` when the NPI matches); the SELECTION-layer
    # interpretation that "NPI in filters → unique match" is a study-
    # specific concern handled by a MatchSelector strategy, not here.
    NPI = "NPI"
    # Name-suffix filter. HIT-ONLY signal: only fires when both sides have a
    # whitelisted suffix value (JR/SR/II/III/IV/V) and they match strictly
    # post-normalization. CMS empirically populates this column on only
    # ~1% of rows with significant data quality issues (credential leaks
    # like "MD"/"DDS" appearing as suffix); the whitelist prevents false
    # matches between two such credential-leaks agreeing.
    NAME_SUFFIX = "NAME_SUFFIX"


class States(StrEnum):
    """USPS state / territory codes recognized by the matcher.

    Includes the 50 states + DC, plus the 8 US territory / military codes that
    CMS Open Payments actually emits in `Recipient_State` columns (AE/AP for
    Armed Forces Europe/Pacific, FM/GU/MP/PR/PW/VI for territories).
    Member name is the USPS abbreviation, value is the full name.
    """

    AL = "Alabama"
    AK = "Alaska"
    AZ = "Arizona"
    AR = "Arkansas"
    CA = "California"
    CO = "Colorado"
    CT = "Connecticut"
    DC = "District of Columbia"
    DE = "Delaware"
    FL = "Florida"
    GA = "Georgia"
    HI = "Hawaii"
    ID = "Idaho"
    IL = "Illinois"
    IN = "Indiana"
    IA = "Iowa"
    KS = "Kansas"
    KY = "Kentucky"
    LA = "Louisiana"
    ME = "Maine"
    MD = "Maryland"
    MA = "Massachusetts"
    MI = "Michigan"
    MN = "Minnesota"
    MS = "Mississippi"
    MO = "Missouri"
    MT = "Montana"
    NE = "Nebraska"
    NV = "Nevada"
    NH = "New Hampshire"
    NJ = "New Jersey"
    NM = "New Mexico"
    NY = "New York"
    NC = "North Carolina"
    ND = "North Dakota"
    OH = "Ohio"
    OK = "Oklahoma"
    OR = "Oregon"
    PA = "Pennsylvania"
    RI = "Rhode Island"
    SC = "South Carolina"
    SD = "South Dakota"
    TN = "Tennessee"
    TX = "Texas"
    UT = "Utah"
    VT = "Vermont"
    VA = "Virginia"
    WA = "Washington"
    WV = "West Virginia"
    WI = "Wisconsin"
    WY = "Wyoming"
    # Territories + military post codes that CMS Open Payments emits.
    AE = "Armed Forces Europe"
    AP = "Armed Forces Pacific"
    FM = "Federated States of Micronesia"
    GU = "Guam"
    MP = "Northern Mariana Islands"
    PR = "Puerto Rico"
    PW = "Palau"
    VI = "U.S. Virgin Islands"


class Unmatcheds(StrEnum):
    """Enum class to represent the various ways a conflicted can
    remain unmatched throughout / after the filtering process."""

    NOLASTNAME = "NOLASTNAME"  # No matches for the last name in OpenPayments
    UNFILTERABLE = "UNFILTERABLE"  # Multiple OpenPayments IDs that can't be matched


class FilterOutcome(StrEnum):
    """Tri-state result of applying a single filter to one payments-x-conflicted row
    (Section 5.8).

    The legacy bool return type conflated three distinct cases:

    - ``MATCH``: both sides have data for this dimension AND they agree.
      Accumulates the filter into the row's ``filters`` list.
    - ``DISAGREE``: both sides have data AND they conflict (e.g. conflicted
      middle = "Marie", payment middle = "Anne"). Accumulates into the
      ``negative_filters`` list — strong evidence this is NOT the same provider.
    - ``NO_DATA``: at least one side lacks data for this dimension (or a
      stronger same-domain filter already fired). No signal of any kind;
      neither list accumulates.

    Selectors can read both ``filters`` and ``negative_filters`` columns to
    distinguish "missing information" from "active disagreement" when ranking
    candidates. The default cascade ignores ``negative_filters`` to preserve
    legacy behavior; tier-based selectors use them as confidence demoters.
    """

    MATCH = "MATCH"
    DISAGREE = "DISAGREE"
    NO_DATA = "NO_DATA"
