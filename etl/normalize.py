import re

def normalize_name(name: str) -> str:
    if not name:
        return ""

    name = name.lower()

    # remove company suffixes
    suffixes = [
        r"\bpty ltd\b",
        r"\bpty\b",
        r"\bltd\b",
        r"\blimited\b",
        r"\bproprietary\b",
        r"\binc\b",
        r"\bcorporation\b",
        r"\bcorp\b",
        r"\bco\b"
    ]

    for s in suffixes:
        name = re.sub(s, "", name)

    # remove punctuation
    name = re.sub(r"[^a-z0-9\s]", " ", name)

    # collapse spaces
    name = re.sub(r"\s+", " ", name)

    return name.strip()
