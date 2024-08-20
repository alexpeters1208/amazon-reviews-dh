#### This script downloads the Amazon data using Huggingface datasets and writes them to Parquet files.

import os
import datasets
import pyarrow.parquet as pq
datasets.logging.set_verbosity_error()

from datasets import load_dataset

# Names of item metadata datasets
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

for name in names:

    # load reviews and write as Parquet if dataset does not already exist
    if not os.path.exists(f"data/amazon/review/{name}.parquet"):
        review_dataset = load_dataset("McAuley-Lab/Amazon-Reviews-2023", f"raw_review_{name}", trust_remote_code=True)['full']
        pq.write_table(review_dataset.data.table, f"data/amazon/review/{name}.parquet")

    # load metadata and write as Parquet if dataset does not already exist
    if not os.path.exists(f"data/amazon/meta/{name}.parquet"):
        meta_dataset = load_dataset("McAuley-Lab/Amazon-Reviews-2023", f"raw_meta_{name}", trust_remote_code=True)['full']
        pq.write_table(meta_dataset.data.table, f"data/amazon/meta/{name}.parquet")
