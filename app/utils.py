import yaml
import psycopg2
import requests
import pandas as pd


def remap_sent(sent): return ' '.join(sent)


def n_sents(idx, df):
    ''' Returns the surrounding sentences in relation
    to the passed in dataframe
    
    Returns
    -------
    tuple
        (starting sentence index, ending sentence index)
    '''
    start = idx
    end = idx
    if idx > 0:
        start = idx-1
    if idx < len(df):
        end = idx+1
    return(start, end)


def connect_db():
    ''' Establish a connection to a database based on a
    yaml configuration file. This expects in the current
    directory.

    Returns
    -------
    pandas dataframe
    '''
    # Database connection
    with open('./config.yml', 'r') as f:
        conf = yaml.load(f)

    conn = psycopg2.connect(dbname=conf['postgres']['database'], 
                            user=conf['postgres']['user'],
                            host=conf['postgres']['host'],
                            port=conf['postgres']['port'],
                            password=conf['postgres']['password'])
    cursor = conn.cursor()

    df = pd.read_sql_query('select docid, sentid, wordidx, words, poses, ners from sentences_nlp352;', con=conn)
    print('Sentences: %s' %len(df))
    return df