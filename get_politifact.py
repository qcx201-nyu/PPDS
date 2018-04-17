import time
import pandas as pd
import requests
import json
import os

print('\nGet Politifact\n')

limit = 100
file_name = 'Politifact.json'
folder_name = 'Data'

file_name = '/' + file_name
folder_name = '/' + folder_name

json_path = os.getcwd()+folder_name+file_name

def get_politifact(limit,offset):
    """Crawl Politifact API"""
    pf_url = 'http://www.politifact.com/api/v/2/statement/'
    params = {
        'format' : 'json',
        'edition__edition_slug':'truth-o-meter',
        'limit' : limit,
        'offset' : offset,
        'order_by' : 'ruling_date'}
    resp = requests.get(pf_url,params=params).json()
    pf = pd.DataFrame(resp['objects'])
    return pf

if not os.path.isdir(os.getcwd()+folder_name):
    os.mkdir(os.getcwd()+folder_name)
    print('* New dir made: ',os.getcwd()+folder_name)

if not os.path.isfile(json_path):
    t1 = time.time()
    df = get_politifact(100,0)
    df.to_json(json_path)
    print('* New file made:',json_path,'\n')
    print('data_length:',len(df))
    timer = time.time() - t1
    print('runtime: {rt} seconds'.format(rt=round(timer,2)))
    time.sleep(5)
print('*'*20)

run = True
while run:
    try:
        t1 = time.time()
        old_df = pd.read_json(json_path)
        old_offset = len(old_df)+1

        print('data_length:',len(old_df))
        print('Offset:',old_offset)
        print('Getting data...')

        new_df = get_politifact(limit,old_offset)
        cols = new_df.columns

        df = old_df.append(new_df).drop_duplicates('statement').reset_index()[cols]
        df.to_json(json_path)

        print('---')
        print('data_length:',len(df))

        exp = old_offset+limit
        print('Expected offset:',exp)
        print('---')

        new_offset = len(df)+1
        print('New offset:',new_offset)

        timer = time.time() - t1
        print('runtime: {rt} seconds'.format(rt=round(timer,2)))

        if exp < new_offset:
            print('#'*10,'OFFSET ERROR','#'*10)
        elif new_offset > exp:
            print('OFFSET Diff:',new_offset-exp)

        if new_offset == old_offset:
            print('#'*10,'OFFSET STABLE','#'*10)
            break

        time.sleep(5)
    except:
        print('#'*10,'EXCEPT ACTIVATED','#'*10)
        run = False
    print('='*20)