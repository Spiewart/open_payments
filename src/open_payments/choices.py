from enum import StrEnum


class Credentials(StrEnum):
    """Enum class for credentials."""

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


class PaymentFilters(StrEnum):
    """Enum class for the various stages at which a conflicted provider
    can be identified from the list of unique OpenPayment IDs."""

    LASTNAME = "LASTNAME"
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


class States(StrEnum):
    """StrEnum class for the different states in the United States.
    Member name is the state's abbreviation, value is the full name."""

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


class Unmatcheds(StrEnum):
    """Enum class to represent the various ways a conflicted can
    remain unmatched throughout / after the filtering process."""

    NOLASTNAME = "NOLASTNAME"  # No matches for the last name in OpenPayments
    UNFILTERABLE = "UNFILTERABLE"  # Multiple OpenPayments IDs that can't be matched
