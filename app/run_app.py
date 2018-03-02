"""
Geodeepdive application to find and extract candidate
sentences along with document ids and sentence ids. 

Output: cand-df.csv
    3-sentence extractions that contain dam and a date
"""
import yaml
import psycopg2
import requests
import pandas as pd
import numpy as np

from utils import connect_db, get_dams, n_sents


# Placeholders for storage 
doc_ids = []
sent_ids = []
passages = []

# Get dam names
dams = get_dams()

# Database connection
df = connect_db()

# Process sentences and build output dataframe
for i in df.itertuples():
    # Get surrounding sentences for scope
    surround_sents = n_sents(i[2], df['docid'])
    before_sent = df.iloc[surround_sents[0]]
    after_sent = df.iloc[surround_sents[1]]
    
    # Sample passage
    passage = before_sent['words'] + i[4] + after_sent['words']
    full_ners = before_sent['ners'] + i[6] + after_sent['ners']
    
    # Cand condition
    if 'dam' in passage or 'Dam' in passage and 'DATE' in full_ners:
        doc_ids.append((before_sent['docid'], i[1], after_sent['docid']))
        sent_ids.append((before_sent['sentid'], i[2], after_sent['sentid']))
        passages.append(passage)

df2 = pd.DataFrame({'docid': doc_ids,
                    'sentid': sent_ids,
                    'passage': passages})
print(df2.info())

# save to disk
df2.to_csv('cand-df.csv')