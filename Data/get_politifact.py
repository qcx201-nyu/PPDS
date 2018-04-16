import time
import pandas as pd
import requests
import json

print('\nGet Politifact\n')

limit = 100

print('*'*20)

def get_politifact(limit,offset):

    """Crawl Politifact API"""

    pf_url = 'http://www.politifact.com/api/v/2/statement/'

    params = {
        'format' : 'json',
        'edition__edition_slug':'truth-o-meter',
        'limit' : limit,
        'offset' : offset,
        'order_by' : 'ruling_date',
    }

    resp = requests.get(pf_url,params=params).json()
    pf = pd.DataFrame(resp['objects'])
    return pf

file_path = '/Users/Xie/Documents/School/Senior Spring/PPDS/Data/Politifact.json'

run = True
while run:
    try:
        t1 = time.time()
        old_df = pd.read_json(file_path)

        offset = len(old_df)+1

        print('data_length:',len(old_df))
        print('Offset:',offset)
        print('Getting data...')

        new_df = get_politifact(limit,offset)
        cols = new_df.columns

        df = old_df.append(new_df).drop_duplicates('statement').reset_index()[cols]
        df.to_json(file_path)

        print('---')
        print('data_length:',len(df))

        exp = offset+limit
        print('Expected offset:',exp)
        print('---')

        offset = len(df)+1
        print('New offset:',offset)

        timer = time.time() - t1
        print('runtime: {rt} seconds'.format(rt=round(timer,2)))

        if exp < offset:
            print('#'*10,'OFFSET ERROR','#'*10)
        elif offset > exp:
            print('OFFSET Diff:',offset-exp)
            
        time.sleep(15)

    except:
        time.sleep(30)

    print('='*20)