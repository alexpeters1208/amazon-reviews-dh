#### This script reads all of the Amazon data from Parquet into Deephaven tables
#### Then, it simulates a table receiving the reviews at 10,000x real-time speed.

from deephaven import parquet
from deephaven import dtypes
from deephaven.table import TableDefinition
from deephaven.replay import TableReplayer
from deephaven.time import to_j_instant

from random import sample
from math import floor
from jpy import array

# create table definition for review datasets
reviews_def = TableDefinition({
    "rating": dtypes.double,
    "title": dtypes.string,
    "text": dtypes.string,
    "parent_asin": dtypes.string,
    "user_id": dtypes.string,
    "timestamp": dtypes.long
})

# read reviews into a single table
reviews = parquet.read(
    "/data/amazon/review/",
    file_layout=parquet.ParquetFileLayout.FLAT_PARTITIONED,
    table_definition=reviews_def
)

# convert timestamp to Java timestamps for replay
reviews = (
    reviews
    .update("timestamp = epochMillisToInstant(timestamp)")
    .sort("timestamp")
    .update("idx = ii")
)

# minimum and maximum times from filtered table - faster to use UI than to compute with a query
min_time = to_j_instant("2023-01-01T00:00:00.000Z")
max_time = to_j_instant("2023-09-14T13:16:54.993Z")

# create replay start time and end time
replay_start_time = to_j_instant("2024-01-01T00:00:00Z")
replay_end_time = to_j_instant("2024-01-01T00:36:00Z")

# this parameter controls the number of seconds per second that data will tick
# also controls the sparsity of the data, so that number of rows per second will be approx. real time
data_speed = 10_000

# randomly sample data to get approx. real time density
random_idx = sample(range(reviews.size), k=floor(reviews.size / data_speed))
j_random_idx = array("long", random_idx)

# randomly sample data and create a replay timestamp
reviews = (
    reviews
    .where("idx in j_random_idx")
    .update([
        "dist = (long)floor((timestamp - min_time) / data_speed)",
        "replay_timestamp = replay_start_time + dist"
    ])
    .drop_columns(["dist", "idx"])
)

# create table replayer and start replay
reviews_replayer = TableReplayer(replay_start_time, replay_end_time)
reviews_ticking = reviews_replayer.add_table(reviews, "replay_timestamp").drop_columns("replay_timestamp")
reviews_replayer.start()