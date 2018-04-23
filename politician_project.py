import requests
import json
import pandas as pd
from congress import Congress
from IPython.display import clear_output, display as clear, display
import datetime as dt
import os
import requests
from sqlalchemy import create_engine

password = 'dwdstudent2015'

conn_string = 'mysql://{user}:{password}@{host}:{port}/?charset=utf8'.format(
    user     = 'root', 
    password = password, 
    host     = '127.0.0.01', 
    port     = 3306, 
    encoding = 'utf-8')

engine = create_engine(conn_string)
con = engine.connect()

propublica_key = 'wAxQ7sF8gcXCBRnY3lzegT23aljM4saALOb6JPlR'
congress = Congress(propublica_key)

def sql(query,show=True):
    
    '''Outputs df with SQL query or executes SQL'''
    
    try:
        return pd.read_sql(query,con=engine)
    except:
        try:
            engine.execute(query)
            print('Executed:',query)
        except:
            print('###ERROR###')    

def to_sql(df,table_name,columns='All',primary_key=None,db=None):
    
    '''Upload df to SQL'''
    
    if columns.upper() == 'ALL':
        columns = df.columns
    if db == None:
        db = 'congress'
    engine.execute('USE {db}'.format(db=db))
    engine.execute('DROP TABLE IF EXISTS {table}'.format(table = table_name))
    
    df[columns].to_sql(name=table_name,
                        con=engine,
                        if_exists='replace',
                        index=False)
    
    if primary_key is not None:
        add_key = 'ALTER TABLE {table} ADD PRIMARY KEY({key})'.format(table=table_name,key=primary_key)
        engine.execute(add_key)
        
def open_json(file_name,folder='/data'):
    
    '''Read json'''
    
    if file_name[0] != '/':
        file_name = '/'+file_name
    if '.json' not in file_name:
        file_name = file_name+'.json'

    cwd = os.getcwd()

    if folder not in file_name:
        if cwd not in file_name:
            file_path = cwd+folder+file_name
        else:
            file_path = folder + file_name
    elif cwd not in file_name:
        file_path = cwd+file_name
    else:
        file_path = file_name
    file_path
    
    file = open(file_path,'r')
    res = json.load(file,)
    file.close()
    return res

def roll_call(vote):
    
    '''Get roll call from votes data'''
    
    PROPUBLICA_API_KEY = 'wAxQ7sF8gcXCBRnY3lzegT23aljM4saALOb6JPlR'
    header = {'X-API-Key' : PROPUBLICA_API_KEY}

    roll_call = requests.get(vote['vote_uri'],headers=header).json()['results']['votes']['vote']
    print(vote['question']+':',vote['description'])

    for member in roll_call['positions']:
        member['question'] = vote['question']
        member['description'] = vote['description']
        member['date'] = dt.datetime.strptime(vote['date'],'%Y-%m-%d').date()
        member['chamber'] = vote['chamber']
    bill = vote['bill']
    if len(bill) > 0:
        print('='*3)
        print('Bill:',vote['bill']['title'])
        print('Bill id:',vote['bill']['bill_id'])
        for member in roll_call['positions']:
            member['bill_id'],member['congress'] = bill['bill_id'].split('-')
            member['bill_sponsor'] = bill['sponsor_id']
    else:
        for member in roll_call['positions']:
            member['bill_id'],member['congress'] = None,vote['congress']
            member['sponsor_id'] = None
    return pd.DataFrame(roll_call['positions'])

def clean_politifact(pf):
    
    '''Returns cleaned politifact data from politifact dataframe or json'''
    
    if type(pf) != pd.core.frame.DataFrame:
        pf = pd.DataFrame(pf)
        
    pf.sort_values('statement_date',ascending=False,inplace=True)
    pf.reset_index(inplace=True)

    pf['statement_date'] = pd.to_datetime(pf['statement_date'])
    pf['ruling_date'] = pd.to_datetime(pf['ruling_date'])
    pf['statement'] = (pf['statement'].str.replace('<p>',"").str.replace('</p>','')
                       .str.replace('"','').str.replace('&quot;','')
                       .str.replace('\r','').str.replace('\n','').str.replace('&#39;','\''))
    pf['ruling_comments'] = (pf['ruling_comments'].str.replace('<p>',"").str.replace('</p>','')
                       .str.replace('"','').str.replace('&quot;','')
                       .str.replace('\r','').str.replace('\n','').str.replace('&#39;','\''))

    pf['ruling_slug'] = [ruling['ruling_slug'] for ruling in pf['ruling']]

    pf['name_slug'] = [speaker['name_slug'] for speaker in pf['speaker']]
    pf['home_state'] = [speaker['home_state'] for speaker in pf['speaker']]
    pf['party'] = [speaker['party']['party_slug'] for speaker in pf['speaker']]
    pf['first_name'] = [speaker['first_name'] for speaker in pf['speaker']]
    pf['last_name'] = [speaker['last_name'] for speaker in pf['speaker']]

    pf['statement_type'] = [statement['statement_type'] for statement in pf['statement_type']]

    pf['subject_slug'] = [[subject['subject_slug'] for subject in subjects] for subjects in pf['subject']]
    
    pf = pf[['ruling_slug','first_name','last_name','name_slug',
                 'home_state','party','statement',
                 'subject_slug','ruling_comments','ruling_date',
                 'statement_context','statement_type','statement_date',
                 'twitter_headline','sources']]
    return pf

######## Some House Keeping Stuff ##########

col_bio = ['first_name','last_name','middle_name','date_of_birth','id']
col_position = ['title','party','seniority', # Seniority = years served
                 'state','district','state_rank','leadership_role',
                'in_office','next_election']
col_policy = ['dw_nominate','votes_with_party_pct','missed_votes_pct','missed_votes']
col_other = ['chamber','gender','congress','twitter_account']

all_cols = col_bio+col_position+col_policy+col_other