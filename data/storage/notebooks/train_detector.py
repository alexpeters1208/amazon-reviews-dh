#### Credit to this training script belongs to Vidhi1290
#### Original repository: https://github.com/Vidhi1290/LLM---Detect-AI-Generated-Text/tree/main

#### This script trains the BERT model to detect GPT-generated essays from Kaggle
#### Orignal Kaggle competition: https://www.kaggle.com/competitions/llm-detect-ai-generated-text/overview

# ML imports
import logging
import random
import numpy as np
import nltk
import re

from sklearn.metrics import accuracy_score
from transformers import BertTokenizer, BertForSequenceClassification
from torch.utils.data import DataLoader, TensorDataset
import torch

# Deephaven imports
from deephaven import read_csv
import deephaven.numpy as dhnp

# download stopwords from nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

# load the training dataset and add index column
essays = (
    read_csv("/data/detector-training/train_essays.csv")
    .update("Idx = ii")
)

# text preprocessing
stop_words = set(stopwords.words('english'))

def clean_text(text) -> str:
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuations
    words = text.split()  # Tokenize
    words = [word.lower() for word in words if word.isalpha()]  # Lowercase and remove non-alphabetic words
    words = [word for word in words if word not in stop_words]  # Remove stop words
    return ' '.join(words)

# create new column containing the cleaned text
essays = essays.update("CleanText = clean_text(text)")

# split tables into training and validation sets
random.seed(42)
train_essays = essays.where("(float)random.random() > 0.2")
val_essays = essays.where_not_in(train_essays, "Idx")

# extract features and labels into Python objects
train_labels = dhnp.to_numpy(train_essays, cols=["generated"]).squeeze().astype(np.long)
train_features = dhnp.to_numpy(train_essays, cols=["CleanText"]).squeeze()
val_labels = dhnp.to_numpy(val_essays, cols=["generated"]).squeeze().astype(np.long)
val_features = dhnp.to_numpy(val_essays, cols=["CleanText"]).squeeze()

# tokenization and Encoding for BERT
tokenizer = BertTokenizer.from_pretrained(
    'bert-base-uncased',
    do_lower_case=True,
    padding=True,
    truncation=True,
    max_length=128,
    clean_up_tokenization_spaces=True
)

encoded_train = tokenizer(train_features.tolist(), padding=True, truncation=True, return_tensors='pt')
encoded_val = tokenizer(val_features.tolist(), padding=True, truncation=True, return_tensors='pt')

# convert labels to tensors
train_labels_pt = torch.from_numpy(train_labels)
val_labels_pt = torch.from_numpy(val_labels)

# create TensorDatasets
train_dataset = TensorDataset(encoded_train['input_ids'], encoded_train['attention_mask'], train_labels_pt)
val_dataset = TensorDataset(encoded_val['input_ids'], encoded_val['attention_mask'], val_labels_pt)

# dataLoader for efficient processing
train_loader = DataLoader(train_dataset, batch_size=16, shuffle=True)
val_loader = DataLoader(val_dataset, batch_size=16, shuffle=False)

# suppress transformer parameter name warnings
loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
for logger in loggers:
    if "transformers" in logger.name.lower():
        logger.setLevel(logging.ERROR)

# define the BERT model for sequence classification
model = BertForSequenceClassification.from_pretrained('bert-base-uncased', num_labels=2)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

# define optimizer and learning rate scheduler
optimizer = torch.optim.AdamW(model.parameters(), lr=2e-5)
epochs = 10

# training loop
for epoch in range(epochs):
    model.train()
    total_loss = 0

    for batch in train_loader:
        input_ids, attention_mask, labels = batch
        input_ids, attention_mask, labels = input_ids.to(device), attention_mask.to(device), labels.to(device)

        optimizer.zero_grad()

        outputs = model(input_ids, attention_mask=attention_mask, labels=labels)
        loss = outputs.loss
        total_loss += loss.item()

        loss.backward()
        print(loss)
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)  # Gradient clipping to avoid exploding gradients
        optimizer.step()

    avg_train_loss = total_loss / len(train_loader)
    print(f"Epoch {epoch + 1}/{epochs}, Average Training Loss: {avg_train_loss:.2f}")

# validation loop
model.eval()
val_preds = []
val_labels = []

with torch.no_grad():
    for batch in val_loader:
        input_ids, attention_mask, labels = batch
        input_ids, attention_mask, labels = input_ids.to(device), attention_mask.to(device), labels.to(device)

        outputs = model(input_ids, attention_mask=attention_mask)
        logits = outputs.logits

        val_preds.extend(torch.argmax(logits, dim=1).cpu().numpy())
        val_labels.extend(labels.cpu().numpy())

# calculate validation accuracy
val_accuracy = accuracy_score(val_labels, val_preds)
print(f"Validation Accuracy: {val_accuracy:.2f}")

# save model
torch.save(model.state_dict(), "/data/model/detector.pt")