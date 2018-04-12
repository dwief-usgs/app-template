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
    idx  = idx-1 # convert from sentid->dataframe index
    start = idx
    end = idx
    if idx > 0:
        start = idx-1
    if idx < len(df) - 1:
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

    print("searching for docids...")
    cursor.execute("select distinct(docid) from sentences_nlp352;")
    docids = [i[0] for i in cursor.fetchall()] # maybe not necessary -- don't want the cursor to conflict itself though.
    print("Looping over %s docids" % len(docids))
    for i in docids:
        df = pd.read_sql_query('select docid, sentid, wordidx, words, poses, ners from sentences_nlp352 WHERE docid=%(docid)s ORDER BY sentid;', con=conn, params={"docid" : i})
        yield df


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
