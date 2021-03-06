
COUPLE = [
    ('winstate_inc', 'winstate_chal', 100),
    ('voteshare_inc', 'voteshare_chal', 1)
]

INDIVIDUAL = [
    ('win_EC_if_win_state_inc', 100),
    ('win_EC_if_win_state_chal', 100)
]

CASTTYPE = [
    "winstate_inc",
    "winstate_chal",
    "voteshare_inc",
    "voteshare_chal",
    "win_EC_if_win_state_inc",
    "win_EC_if_win_state_chal",
    "margin",
    "vpi",
    "tipping"
]

COLS = [
    "winstate_inc",
    "winstate_chal",
    "voteshare_inc",
    "voteshare_chal",
    "win_EC_if_win_state_inc",
    "win_EC_if_win_state_chal",
    "margin",
    "vpi",
    "state",
    "tipping",
    "modeldate"
]

COLS_GRAPH = [
    "winstate_inc",
    "winstate_chal",
    "voteshare_inc",
    "voteshare_chal",
    "win_EC_if_win_state_inc",
    "win_EC_if_win_state_chal",
    "margin",
    "vpi",
    "state",
    "tipping",
    "modeldate",
    "pagerank"
]

DIVISION = {
    "INC": [
        "winstate_inc", 
        "voteshare_inc", 
        "win_EC_if_win_state_inc", 
        "margin", 
        "vpi", 
        "state", 
        "tipping",
        "modeldate"
    ],
    "CHAL": [
        "winstate_chal", 
        "voteshare_chal", 
        "win_EC_if_win_state_chal",
        "vpi", 
        "state",
        "tipping",
        "modeldate"
    ],
    "PREDICTION": {
        "INC": {
            "LABEL": "winstate_inc",
            "FEATURES": [
                "pagerank"
            ],
            "ADJUST": ["modeldate"]
        },
        "CHAL": {
            "LABEL": "winstate_chal",
            "FEATURES": [
                "pagerank"
            ],
            "ADJUST": ["modeldate"]
        }
    }
}

DIVISION_GRAPH = {
    "INC": [
        "winstate_inc", 
        "voteshare_inc", 
        "win_EC_if_win_state_inc", 
        "margin", 
        "vpi", 
        "state", 
        "tipping",
        "modeldate",
        "pagerank"
    ],
    "CHAL": [
        "winstate_chal", 
        "voteshare_chal", 
        "win_EC_if_win_state_chal",
        "vpi", 
        "state",
        "tipping",
        "modeldate",
        "pagerank"
    ],
    "PREDICTION": {
        "INC": {
            "LABEL": "winstate_inc",
            "FEATURES": [
                "pagerank"
            ],
            "ADJUST": ["modeldate"]
        },
        "CHAL": {
            "LABEL": "winstate_chal",
            "FEATURES": [
                "pagerank"
            ],
            "ADJUST": ["modeldate"]
        }
    }
}