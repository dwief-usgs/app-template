"""
File: run_app.py
Description: Sample app utilizing the GeoDeepDive infrastructure and products.
    This process is replicating a previous manual effort in identifying relevant
    documents to damn removal.
Assumes: make setup-local has been run (so that the example database is populated)
"""

import yaml
import psycopg2
from psycopg2.extensions import AsIs

with open('../credentials.yml', 'r') as credential_yaml:
    credentials = yaml.load(credential_yaml)

with open('../config.yml', 'r') as config_yaml:
    config = yaml.load(config_yaml)

# Connect to Postgres
connection = psycopg2.connect(
    dbname=credentials['postgres']['database'],
    user=credentials['postgres']['user'],
    host=credentials['postgres']['host'],
    port=credentials['postgres']['port'])
cursor = connection.cursor()

# tmp store
dam_removal_docs = []
docids = []

# retrieve docids -- replace w/ correct db table
cursor.execute("SELECT DISTINCT DOCID FROM geodeepdive;")
for id in cursor:
    docids.append(id[0])


def term_in_corpus(term, cursor):
    for i in cursor:
        if term in i[0]:
            return True


for id in docids:
    cursor.execute("SELECT WORD, DOCID, SENTID FROM geodeepdive WHERE docid = (%s);",(id))
    try:
        for sentence in cursor:
            words = sentence[0]
            sentid = sentence[2]
            for i in eval(words):
                if 'dam' == i and term_in_corpus('removal', cursor):
                    if term_in_corpus('stream', cursor) or term_in_corpus('river', cursor):
                            print('Match exists in document: ' + str(id) + str(sentid))
                            dam_removal_docs.append((id, sentid))
    except:
        pass

with open("../output/damremoval_documents.txt", "w") as f:
    for i in dam_removal_docs:
        f.write(str(i) + '\n')
