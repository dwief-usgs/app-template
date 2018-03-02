import yaml
import psycopg2
import requests
import pandas as pd


def remap_sent(sent): return ' '.join(sent)


def n_sents(idx, df):
    ''' Returns the surrounding sentences in rel to dataframe'''
    start = idx
    end = idx
    if idx > 0:
        start = idx-1
    if idx < len(df):
        end = idx+1
    return(start, end)


def connect_db():
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


# Loading related data
def get_drip_resources(URL='https://beta-gc2.datadistillery.org/api/v1/sql/bcb?q=select * from drip.dripdams'):
    """Export of known dam removals from data distillery
    Parameters
    ----------
    URL : str
        Built query for GC2.
    """
    try:
        r = requests.get(URL)
        if r.status_code == 200:
            return r.json()
        else:
            raise Exception('Data Distillery URL returning: %s', r.status_code)
    except Exception as e:
        raise Exception(e)

def get_dams():
    # Load Data Distillery dam data
    dams = pd.DataFrame([i['properties'] for i in get_drip_resources()['features']])
    # Cleaned name list
    # Rm river/dam and related words
    dams['name'] = dams['dam_name'].str.replace('Unknown', '')
    dams['name'] = dams['name'].dropna()
    dams['name'] = dams['dam_name'].str.replace('Dam', '').replace('River', '').replace('River)', '').replace('dam', '')
    return dams