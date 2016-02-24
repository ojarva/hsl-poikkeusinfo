"""
Settings for poikkeusinfo.py
"""

LINES = {
    "6(T)": {
        "line_type": "tram",
        "numbers": ["6", "6T"],
        "directions": ["to_centrum"],
    },
    "7B": {
        "line_type": "tram",
        "numbers": ["7B"],
        "directions": ["from_centrum"],
    },
    "7A": {
        "line_type": "tram",
        "numbers": ["7A"],
        "directions": ["to_centrum"],
    },
    "metro": {
        "line_type": "metro",
        "directions": ["to_centrum"],
    },
    "64": {
        "directions": ["from_centrum"],
        "line_type": "helsinki",
        "numbers": ["64"]
    },
    "65A/66A": {
        "directions": ["to_centrum"],
        "line_type": "helsinki",
        "numbers": ["65A", "66A"],
    }
}

FETCH_INTERVAL = 60 * 3  # seconds
