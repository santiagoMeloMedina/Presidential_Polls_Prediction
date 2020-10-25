
from pyspark.sql.types import StringType, FloatType

COLUMNS = [
    ("winstate_inc", FloatType()),
    ("winstate_chal", FloatType()),
    ("voteshare_inc", FloatType()),
    ("voteshare_chal", FloatType()),
    ("win_EC_if_win_state_inc", FloatType()),
    ("win_EC_if_win_state_chal", FloatType()),
    ("margin", FloatType()),
    ("vpi", FloatType()),
    ("state", StringType()),
    ("tipping", FloatType()),
    ("modeldate", StringType())
]

ACCEPTER_BATCH_PERCENTAGE = 5