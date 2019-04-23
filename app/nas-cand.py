"""
Geodeepdive application to find and extract candidate
sentences along with document ids and sentence ids.

Output: nas-cand-df.csv
    3-sentence extractions that contain dam
"""
import yaml
import psycopg2
import requests
import pandas as pd
import numpy as np

from utils import connect_db, n_sents, get_species_list, get_doc_list


# Placeholders for storage
doc_ids = []
sent_ids = []
passages = []
ner = []

#Get list of NAS taxa terms
species_list = get_species_list()
#species_list = ['Oncorhynchus','Acipenser'] #testing

# Database connection
#documents = connect_db()

# For each taxa term process sentences for each document:
for term in species_list:
    print ('working on {}'.format(term))
    term_doc_list = get_doc_list(term)
    #term_doc_list = ['558']  #for local test
    documents = connect_db(term_doc_list)
    for doc in documents:
        for i in doc.itertuples():
            # Get surrounding sentences for scope
            surround_sents = n_sents(i[2], doc['docid'])
            try:
                before_sent = doc.iloc[surround_sents[0]]
                middle_sent = doc.iloc[surround_sents[1]]
                after_sent = doc.iloc[surround_sents[2]]
            except IndexError: # couldn't get surrounding sentences
                continue



            # Sample passage
            passage = before_sent['words'] + middle_sent['words'] + after_sent['words']
            full_ners = before_sent['ners'] + middle_sent['ners'] + after_sent['ners']
            # because terms can be multi-word and passage is a list, convert it to string to check for term inclusion
            passage_str = " ".join(passage)

            # Cand condition
            if term in passage_str:
                doc_ids.append((before_sent['docid'], middle_sent['docid'], after_sent['docid']))
                sent_ids.append((before_sent['sentid'], middle_sent['sentid'], after_sent['sentid']))
                passages.append(passage)
                ner.append(full_ners)
    print("Current total number of sentences: %s" % len(sent_ids))

df2 = pd.DataFrame({'docid': doc_ids,
                    'sentid': sent_ids,
                    'passage': passages,
                    'ner': ner})

df3 = df2.drop_duplicates(subset=['docid', 'sentid'], keep='first')
print(df3.info())

# save to disk
df3.to_csv('nas-cand-df.csv')
