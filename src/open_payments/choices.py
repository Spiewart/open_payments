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