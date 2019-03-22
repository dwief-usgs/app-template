import yaml
import psycopg2
import requests
import pandas as pd


def remap_sent(sent): return ' '.join(sent)


# def n_sents(idx, df):
#     ''' Returns the surrounding sentences in relation
#     to the passed in dataframe

#     Returns
#     -------
#     tuple
#         (starting sentence index, ending sentence index)
#     '''
#     idx  = idx-1 # convert from sentid->dataframe index
#     start = idx
#     end = idx
#     if idx > 0:
#         start = idx-1
#     if idx < len(df) - 1:
#         end = idx+1
#     return(start, end)

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
    if len(term_doc_list) == 0:
        sql = "select distinct(docid) from sentences_nlp352 where docid in ({});".format(", ".join(["'"+str(n)+"'" for n in term_doc_list]))
        cursor.execute(sql)
        docids = [i[0] for i in cursor.fetchall()]
        #print("Looping over %s docids, for %s" % len(docids))
        for i in docids:
            df = pd.read_sql_query('select docid, sentid, wordidx, words, poses, ners from sentences_nlp352 WHERE docid=%(docid)s ORDER BY sentid;', con=conn, params={"docid" : i})
            yield df
        



# Loading related data
#*********************

#This is saved in case this becomes available, currently no way to query full list of species
#this url will be in place soon https://nas.er.usgs.gov/api/v1/species
#def get_species_list(url='https://nas.er.usgs.gov/api/v1/species'):
#    """Export list of species of interest
#    ----------
#    URL : API call built for  NAS species
#    """
#    try:
#        r = requests.get(URL)
#        if r.status_code == 200:
#            return r.json()
#        else:
#            raise Exception('NAS API URL returning: %s', r.status_code)
#    except Exception as e:
#        raise Exception(e)


def get_species_list(file= './resources/nas_species_itis.csv'):
    """Export list of species of interest
    ----------
    file : data from NAS team, being used until API in place
    """
    species = pd.read_csv(file, encoding='iso_8859_5')
    species_list = []
    
    for row in species.itertuples():
        if ' x ' in row.scientificName:
            name = row.common_name
            species_list.append(name)
        elif ' sp.' in row.scientificName:
            name = row.Genus
            species_list.append(name)
        else:
            name = row.scientificName
            species_list.append(name)
    
    return species_list


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


	

