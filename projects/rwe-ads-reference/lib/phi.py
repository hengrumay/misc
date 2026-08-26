"""PHI (Protected Health Information) masking utilities.

Masks common PHI patterns (SSN, MRN, phone, email, date of birth) from strings.
Used for gateway request/response masking + audit logging.
"""
from __future__ import annotations

import re


# Patterns for common PHI
PHI_PATTERNS = {
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),  # 123-45-6789
    "mrn": re.compile(r"\b[MR]{1,3}\d{6,12}\b"),  # MR123456 or MRN123456
    "phone": re.compile(r"\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b"),
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    "dob": re.compile(r"\b(?:0[1-9]|1[0-2])[/-](?:0[1-9]|[12][0-9]|3[01])[/-](?:19|20)\d{2}\b"),  # MM/DD/YYYY or MM-DD-YYYY
    "name": re.compile(r"\b(?:Mr|Mrs|Ms|Dr)\.\s+[A-Z][a-z]+\b"),  # Common name patterns
}


def mask_phi(text: str, mask_char: str = "*") -> str:
    """Mask common PHI patterns in a string.

    Args:
        text: Input string potentially containing PHI
        mask_char: Character to use for masking (default: '*')

    Returns:
        String with PHI patterns replaced with mask_char
    """
    result = text
    for pattern_name, pattern in PHI_PATTERNS.items():
        # Replace matches with mask of equal length
        def replacer(match: re.Match) -> str:
            return mask_char * len(match.group(0))

        result = pattern.sub(replacer, result)

    return result


def contains_phi(text: str) -> bool:
    """Check if a string contains any PHI patterns.

    Args:
        text: Input string to check

    Returns:
        True if any PHI pattern detected, False otherwise
    """
    for pattern in PHI_PATTERNS.values():
        if pattern.search(text):
            return True
    return False


def phi_patterns_list() -> list[str]:
    """Return list of PHI pattern names being masked.

    Returns:
        List of pattern keys
    """
    return list(PHI_PATTERNS.keys())


if __name__ == "__main__":
    # Quick test
    test_strings = [
        "Patient SSN is 123-45-6789",
        "MRN: MR987654",
        "Contact: john.doe@example.com, 555-123-4567",
        "DOB: 01/15/1990",
        "Dr. Smith recommends",
        "No PHI here",
    ]

    print("PHI Masking Test")
    for test_str in test_strings:
        masked = mask_phi(test_str)
        has_phi = contains_phi(test_str)
        print(f"  Input:  {test_str}")
        print(f"  Masked: {masked}")
        print(f"  Has PHI: {has_phi}")
        print()
