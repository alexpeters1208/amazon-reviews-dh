import os
import datasets
import pyarrow.parquet as pq
import datetime as dt
datasets.logging.set_verbosity_error()

from datasets import load_dataset

# Names of all dataset categories
names = [
    'All_Beauty',
    'Toys_and_Games',
    'Cell_Phones_and_Accessories',
    'Industrial_and_Scientific',
    'Gift_Cards',
    'Musical_Instruments',
    'Electronics',
    'Handmade_Products',
    'Arts_Crafts_and_Sewing',
    'Baby_Products',
    'Health_and_Household',
    'Office_Products',
    'Digital_Music',
    'Grocery_and_Gourmet_Food',
    'Sports_and_Outdoors',
    'Home_and_Kitchen',
    'Subscription_Boxes',
    'Tools_and_Home_Improvement',
    'Pet_Supplies',
    'Video_Games',
    'Kindle_Store',
    'Clothing_Shoes_and_Jewelry',
    'Patio_Lawn_and_Garden',
    'Unknown',
    'Books',
    'Automotive',
    'CDs_and_Vinyl',
    'Beauty_and_Personal_Care',
    'Amazon_Fashion',
    'Magazine_Subscriptions',
    'Software',
    'Health_and_Personal_Care',
    'Appliances',
    'Movies_and_TV'
]

# only want to look at data in 2023 - get Jan 1 2023 and convert to millis since epoch
start_time_millis = dt.datetime(2023, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc).timestamp() * 1_000

for name in names:

    # load reviews and write as Parquet if dataset does not already exist
    if not os.path.exists(f"data/amazon/review1/{name}.parquet"):
        review_dataset = load_dataset("McAuley-Lab/Amazon-Reviews-2023", f"raw_review_{name}", trust_remote_code=True)['full']
        # select columns of interest and filter for post-2022 before writing
        filtered_review_dataset = (
            review_dataset
            .select_columns(["rating", "title", "text", "parent_asin", "user_id", "timestamp"])
            .filter(lambda timestamp: timestamp >= start_time_millis, input_columns="timestamp")
        )
        filtered_review_dataset.to_parquet(f"data/amazon/review1/{name}.parquet")

    # load metadata and write as Parquet if dataset does not already exist
    if not os.path.exists(f"data/amazon/meta1/{name}.parquet"):
        meta_dataset = load_dataset("McAuley-Lab/Amazon-Reviews-2023", f"raw_meta_{name}", trust_remote_code=True)['full']
        # select columns of interest before writing
        filtered_meta_dataset = (
            meta_dataset
            .select_columns(["main_category", "title", "average_rating", "rating_number", "parent_asin"])
        )
        filtered_meta_dataset.to_parquet(f"data/amazon/meta1/{name}.parquet")