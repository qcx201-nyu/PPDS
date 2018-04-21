import datetime as dt
from datetime import date, timedelta
import requests
import json
import os
import time

t0 = time.time()

file_name = '/votes.json'
folder = '/data'

start = dt.date(1988, 1, 1) # start date
end = dt.datetime.today().date()  # end date

header = {'X-API-Key': 'wAxQ7sF8gcXCBRnY3lzegT23aljM4saALOb6JPlR'}

cwd = os.getcwd()
file_path = cwd+folder+file_name

if not os.path.isdir(cwd+folder):
    os.mkdir(cwd+folder)
    print('* New dir made: ',cwd+folder)    

if not os.path.isfile(file_path):
    print('* New file made:',file_path)
    x = list()
elif os.path.isfile(file_path):
    file = open(file_path,'r')
    x = json.load(file)
    file.close()
    if len(x) > 0:
        start = dt.datetime.strptime(x[-1]['date'],'%Y-%m-%d').date() # start date

delta = end - start # timedelta
skip = 0
blank = 0

print('### Get Votes ###')
print('File:',file_name)
print('Start date:',start)
print('Data length:',len(x))
print('*'*20)

for i in range(delta.days + 1):
    date = start + dt.timedelta(days=i)
    try:
        votes_date_url = ('https://api.propublica.org/congress/v1/both/votes/{date}/{date}.json'
                          .format(date=date))
        votes = requests.get(votes_date_url,headers=header).json()['results']['votes']
        if votes not in x and len(votes) > 0:
            x.extend(votes)
        else:
            blank+=1
        if i % 15 == 0:
            vote_file = open(file_path,'w',encoding='utf-8')
            json.dump(x,vote_file,ensure_ascii=False)
            vote_file.close()

            os.system('clear')
            timer = time.time() - t0
            print('### Get Votes ###')
            print('File:',file_name)
            print('Start date:',start)
            print('Data length:',len(x))
            print('Skipped:',skip)
            print('Blanks:',blank)
            print('Run Time:',int(timer/60),'m',round(timer%60,2),'s')
            print('*'*20)

    except KeyboardInterrupt:
        print('### Stopped ###')
        break
    except:
        print('### ERROR ###')
        skip+=1
        pass

    print(date)
    print('Number of votes:',len(votes))
    print('='*10)

vote_file = open(file_path,'w',encoding='utf-8')
json.dump(x,vote_file,ensure_ascii=False)
vote_file.close()
print('### Complete ###')