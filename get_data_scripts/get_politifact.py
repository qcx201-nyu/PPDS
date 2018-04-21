import requests
import json
import os
import time
from IPython.display import clear_output as clear  # Remove in script

t0 = time.time()
t1 = t0
def get_politifact(limit,offset):
    """Scrape Politifact API"""
    pf_url = 'http://www.politifact.com/api/v/2/statement/'
    params = {
        'format' : 'json',
        'edition__edition_slug':'truth-o-meter',
        'limit' : limit,
        'offset' : offset,
        'order_by' : 'ruling_date'}
    resp = requests.get(pf_url,params=params).json()
    res = resp['objects']
    return res

folder = '/data'
file_name = '/politifact_test.json'
cwd = os.getcwd()
file_path = cwd+folder+file_name

if not os.path.isdir(cwd+folder):
    os.mkdir(cwd+folder)
    print('* New dir made: ',cwd+folder)

if not os.path.isfile(file_path):
    politifact_list = list()
    print('* New file made:',file_path)
    offset = 0
else:
    file = open(file_path,'r')
    politifact_list = json.load(file)
    file.close()
    offset = len(politifact_list) + 1

duplicate = 0
limit = 30
n = 0
nr = 0

print('#'*5, 'Scrape Politifact Script','#'*5)
print('Round:',nr)
print('List len:',len(politifact_list))
print('Duplicates:',duplicate)
print('Batch size:',limit)
print('*'*20)

while True:

    try:

        print('Iteration:',n)
        print('Start:',offset)

        batch = get_politifact(limit,offset) # api fetch

        if len(batch) == 0:
            print('#'*5,'Complete','#'*5)
            print(file_path)
            break

        for entry in batch:
            if entry in politifact_list:
                duplicate += 1
            elif entry not in politifact_list:
                politifact_list.append(entry)
        print('list len:',len(politifact_list))
        print('='*3)

        file = open(file_path, 'w')
        json.dump(politifact_list, file, ensure_ascii=False)
        file.close()



        n+=1
        offset += limit
        if n % 5 == 0:
            timer0 = time.time()-t0
            timer1 = time.time()-t1
            t1 = time.time()

            os.system('clear')
            clear() # Remove in script
            print('#'*5, 'Scrape Politifact Script','#'*5)
            print('Round:',nr)
            print('Duplicates:',duplicate)
            print('Batch size:',limit)
            print('Round run time:',int(timer1/60),'m',round(timer1%60,2),'s')
            print('Total run time:',int(timer0/60),'m',round(timer0%60,2),'s')
            print('*'*20)
            nr += 1

    except:
        print('#'*5,'STOPPED','#'*5)
        print(file_path)
        break

file = open(file_path, 'w')
json.dump(politifact_list, file, ensure_ascii=False)
file.close()