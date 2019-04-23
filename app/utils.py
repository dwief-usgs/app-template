import yaml
import psycopg2
import requests
import pandas as pd


def remap_sent(sent): return ' '.join(sent)

def n_sents(idx, df):
    ''' Returns index corresponding to ids of start and end sentences
    to reflect three consecetive sentences in relation
    to the passed in start sentence and dataframe
    in most cases this returns before and after sentence index
    except when at beginning or end of document

    Returns
    -------
    tuple
        (starting sentence index, ending sentence index)
    '''
    idx  = idx-1 # convert from sentid->dataframe index
    start = idx
    middle = idx
    end = idx
    # make sure we do not join same sentence twice but also ensure we get 3 sentence context
    if idx == len(df)-1:
        start = idx-2
        middle = idx-1
    elif idx > 0:
        start = idx-1

    if idx == 0:
        end = idx+2
        middle = idx+1
    elif idx < len(df)-1:
        end = idx+1

    return(start, middle, end)

def connect_db(term_doc_list):
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
    if len(term_doc_list) != 0:
        sql = "select distinct(docid) from sentences_nlp352 where docid in ({});".format(", ".join(["'"+str(n)+"'" for n in term_doc_list]))
        cursor.execute(sql)
        docids = [i[0] for i in cursor.fetchall()]
        print("Looping over %s docids" % len(docids))
        for i in docids:
            df = pd.read_sql_query('select docid, sentid, wordidx, words, poses, ners from sentences_nlp352 WHERE docid=%(docid)s ORDER BY sentid;', con=conn, params={"docid" : i})
            yield df




# Loading related data
#*********************

def get_nas_taxa(url='https://nas.er.usgs.gov/api/v1/species'):
    """Return list of taxa information for NAS species of interest
    ----------
    url : API that returns JSON results of NAS specie taxonomy
    """
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
        else:
            raise Exception('NAS API URL returning: {}'.format(r.status_code))
    except Exception as e:
        raise Exception(e)


def get_taxa_list():
    """Export list of taxa of interest
    ----------
    taxa_r = JSON response of all NAS taxa
    """
    taxa_r = get_nas_taxa()
    species_list = []

    for taxa in taxa_r['results']:
        if ' x ' in taxa['species']:
            taxa_list.append(taxa['common_name'])
        elif 'sp. ' in taxa['species']:
            taxa_list.append(taxa['genus'])
            taxa_list.append(taxa['common_name'])
        else:
            sci_name = (taxa['genus']+' '+taxa['species'] + ' '+ taxa['subspecies'] + ' ' + taxa['variety']).strip()
            taxa_list.append(sci_name)
    return taxa_list

def get_doc_list(term, URL='https://geodeepdive.org/api/terms?show_docids&term='):
    """Create list of docs mentioning a term of interest
    Parameters
    ----------
    URL : str
        endpoint for gdd term search.
    """
    search = URL + str(term)
    try:
        r = requests.get(search)
        if r.status_code == 200 and 'success' in r.json():
            json_r = r.json()
            data = json_r['success']['data']
            docids = data[0]['docids']
            return docids
        elif r.status_code == 200:
            docids = []
            return docids
        else:
            raise Exception('GDD API returning: %s', r.status_code)
    except Exception as e:
        raise Exception(e)




