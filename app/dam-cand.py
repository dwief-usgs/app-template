"""
Geodeepdive application to find and extract candidate
sentences along with document ids and sentence ids.

Output: cand-df.csv
    3-sentence extractions that contain dam
"""
import yaml
import psycopg2
import requests
import pandas as pd
import numpy as np

from utils import connect_db, n_sents


# Placeholders for storage
doc_ids = []
sent_ids = []
passages = []
ner = []

# Database connection
documents = connect_db()

# Process sentences and build output dataframe
for doc in documents:
    for i in doc.itertuples():
        # Get surrounding sentences for scope
        surround_sents = n_sents(i[2], doc['docid'])
        before_sent = doc.iloc[surround_sents[0]]
        after_sent = doc.iloc[surround_sents[1]]

        # Sample passage
        passage = before_sent['words'] + i[4] + after_sent['words']
        full_ners = before_sent['ners'] + i[6] + after_sent['ners']

        # Cand condition
        if ('dam' in passage or 'Dam' in passage):  # and 'DATE' in full_ners:
            doc_ids.append((before_sent['docid'], i[1], after_sent['docid']))
            sent_ids.append((before_sent['sentid'], i[2], after_sent['sentid']))
            passages.append(passage)
            ner.append(full_ners)

df2 = pd.DataFrame({'docid': doc_ids,
                    'sentid': sent_ids,
                    'passage': passages,
                    'ner': ner})
print(df2.info())

# save to disk
df2.to_csv('dam-cand-df.csv')
