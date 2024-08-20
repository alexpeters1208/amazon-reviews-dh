# ML imports
import torch
import numpy as np
from transformers import BertTokenizer, BertForSequenceClassification

# DH imports
from deephaven.table_listener import listen, TableUpdate
from deephaven.stream.table_publisher import table_publisher
from deephaven.stream import blink_to_append_only
from deephaven import new_table
import deephaven.column as dhcol
import deephaven.dtypes as dtypes

# other imports
import concurrent.futures
import logging

# suppress transformer parameter name warnings
loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
for logger in loggers:
    if "transformers" in logger.name.lower():
        logger.setLevel(logging.ERROR)

# instantiate model and load parameters
model = BertForSequenceClassification.from_pretrained('bert-base-uncased', num_labels=2)
model.load_state_dict(torch.load("/data/model/detector.pt", weights_only=False))

# get device
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# instantiate tokenizer
tokenizer = BertTokenizer.from_pretrained(
    'bert-base-uncased',
    do_lower_case=True,
    padding=True,
    truncation=True,
    max_length=128,
    clean_up_tokenization_spaces=True
)

# prediction function
def detect_bot(text):
    # tokenize text
    tokenized_text = tokenizer(text.tolist(), padding=True, truncation=True, return_tensors='pt')

    # Move input tensor to the same device as the model
    tokenized_text = {key: value.to(device) for key, value in tokenized_text.items()}

    # Generate predictions using your trained model
    with torch.no_grad():
        outputs = model(**tokenized_text)
        logits = outputs.logits

    # Assuming the first column of logits corresponds to the negative class (non-AI-generated) 
    # and the second column corresponds to the positive class (AI-generated)
    predictions = torch.softmax(logits, dim=1)[:, 1].cpu().numpy()

    return predictions

# load ticking reviews dataset and item metadata
from data import reviews_ticking

preds_blink, preds_publisher = table_publisher(
    "DetectorOutput", {
        "rating": dtypes.double,
        "parent_asin": dtypes.string,
        "user_id": dtypes.string,
        "timestamp": dtypes.Instant,
        "gen_prob": dtypes.float32
    },
)

def compute_and_publish_inference(inputs, features):

    # get outputs from AI model
    outputs = detect_bot(inputs)

    # create new table with relevant features and outputs
    output_table = new_table(
        [
            dhcol.double_col("rating", features["rating"]),
            dhcol.string_col("parent_asin", features["parent_asin"]),
            dhcol.string_col("user_id", features["user_id"]),
            dhcol.datetime_col("timestamp", features["timestamp"]),
            dhcol.float_col("gen_prob", outputs)
        ]
    )

    # publish inference to table
    preds_publisher.add(output_table)

    return None

# use ThreadPoolExecutor to offload inference to separate threads that will do work as able
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

def on_update(update: TableUpdate, is_replay: bool) -> None:
    input_col = "text"
    feature_cols = ["rating", "parent_asin", "user_id", "timestamp"]

    # get table enries that were added or modified
    adds = update.added(cols=[input_col, *feature_cols])
    modifies = update.modified(cols=[input_col, *feature_cols])

    # collect data from this cycle into objects to feed to inference and output
    if adds and modifies:
        inputs = np.hstack([adds[input_col], modifies[input_col]])
        features = {feature_col: np.hstack([adds[feature_col], modifies[feature_col]]) for feature_col in feature_cols}
    elif adds:
        inputs = adds[input_col]
        features = {feature_col: adds[feature_col] for feature_col in feature_cols}
    elif modifies:
        inputs = modifies[input_col]
        features = {feature_col: modifies[feature_col] for feature_col in feature_cols}
    else:
        return

    # submit inference work to ThreadPoolExecutor
    executor.submit(compute_and_publish_inference, inputs, features)


handle = listen(reviews_ticking, on_update, do_replay=True)
preds = blink_to_append_only(preds_blink)