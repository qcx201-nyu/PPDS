import requests
from lxml import html
import pandas as pd
import re
from IPython.display import display, clear_output

url = "https://www.govtrack.us/misconduct"
gt_url = 'https://www.govtrack.us'
response = requests.get(url)
doc = html.fromstring(response.text)

# Using Regex to get entry class names
html = requests.get(url).text
regex = re.compile(r'class="misconduct-entry.*"')
matches = regex.finditer(html)
calls = [m.group() for m in matches]
entries = [doc.xpath('//div[@{ent}]'.format(ent=call))[0] for call in calls]

def parseEntryNode(entry):

    member = entry.xpath('.//h3/a')[0].text_content().strip()
    member_page = gt_url+entry.xpath('.//h3/a')[0].get('href')
    tags = [tag.text_content().strip() for tag in entry.xpath('.//div[@class="tag-list"]')[0]]
    paragraph = entry.xpath('.//p')[0].text_content()
    updates = [' '.join(update.text_content().split()) for update in entry.xpath('.//table')[0]]
    update_links = [update.xpath('.//a')[0].get('href') if len(update.xpath('.//a')) > 0 else None for update in entry.xpath('.//table')[0]]

    result = {
    'member' : member,
    'member_page' : member_page,
    'tags' : tags,
    'paragraph' : paragraph,
    'updates' : updates,
    'update_links' : update_links
    }
    return result

contents = doc.xpath('//div[@class="col-sm-8 col-sm-pull-4"]')[0]

# Data contants starts from index 1
data = [parseEntryNode(contents[i]) for i in range(1,len(contents))] 
df = pd.DataFrame(data)
def get_bioguide(member_url):
    '''returns bioguide given govtrack member page'''
    member_html = requests.get(member_url).text
    bioregex = re.compile(r'href="http://bioguide.congress.gov/.*" ')
    res = bioregex.findall(member_html)[0]
    bioguide = res.replace('href="http://bioguide.congress.gov/scripts/biodisplay.pl?index=','')[:7]
    return bioguide
print('Getting bioguide...')
bioguide = [get_bioguide(url) for url in df['member_page']] # This step takes some time
df['bioguide'] = bioguide

# Print duplicates
print('Duplicate rows:', len(df[df['paragraph'].duplicated()]))

# Reverse entry_index for primary key
df['entry_index'] = len(df)-df.index

# Get names and districts of congress members using Regex raw string for member column
members_strings = df['member'].to_string()

# The district regex is imperfect because of irregularities
district_regex = re.compile(r'\[+\w-[\w]*')
district_matches = district_regex.finditer(members_strings)
rg_district = [m.group()[1:] for m in district_matches]

# Get names using regex
names_regex = re.compile(r"[A-z].*\[")
names_matches = names_regex.finditer(members_strings)
names = [m.group()[:-2] for m in names_matches]
df['member_name'] = names

# Get districts from names
district = [df['member'][i].replace(
    df['member_name'][i]+' ','')
            for i in range(len(df))]
df['district'] = district

# Member parties are denoted by 1 letter abbreviations before the district
print(set(df['district'].str[1:3]))

# Add column with full party name
party_dict = {'A':'A (Miscellaneous)','D':'Democrat','F':'Federalist',
         'I':'Independent','J':'Jackson','R':'Republican',
         'U' : 'Unconditional Unionist','W' :'Whig'}
member_party = [party_dict[df['district'].iloc[i][1]] for i in range(len(df))]
df['party'] = member_party

### SQL ###
print('uploading to SQL')

from sqlalchemy import create_engine
db = 'congressional_misconduct'
pw = 'dwdstudent2015'
conn_string = 'mysql://{user}:{password}@{host}:{port}/?charset=utf8'.format(
    user     = 'root', 
    password = pw, 
    host     = '127.0.0.01', 
    port     = 3306, 
    encoding = 'utf-8'
)
engine = create_engine(conn_string)
engine.execute('CREATE DATABASE IF NOT EXISTS {db}'.format(db=db))
print('Database:',db)

engine.execute('USE {db}'.format(db=db))
pd.read_sql('SHOW Databases',con=engine)

# Drop all existing tables
db_tables = pd.read_sql('SHOW TABLES',con=engine).iloc[:,0]
for t in db_tables:
    if t != 'misconduct_entries':
        engine.execute('DROP TABLE IF EXISTS {table}'.format(table = t))

# Drop table with foreign key reference last        
engine.execute('DROP TABLE IF EXISTS {table}'.format(table = 'misconduct_entries'))

df[['entry_index','member_name','bioguide','party','district',
    'member','member_page','paragraph']].to_sql(name='misconduct_entries',
                                                 con=engine,
                                                 if_exists='replace',
                                                 index=False)
add_key_query = 'ALTER TABLE misconduct_entries ADD PRIMARY KEY(entry_index)'
engine.execute(add_key_query)

# Create dataframe from iterated list
index_list = list()
tags_list = list()

for i in range(len(df)):
    for tag in df.iloc[i]['tags']:
        index_list.append(df.iloc[i]['entry_index']) # Use entry_index as foreign key
        tags_list.append(tag)

tags_df = pd.DataFrame({
    'entry_index' : index_list,
    'tags' : tags_list})
# Create table
tags_df.to_sql(name='misconduct_tags',con=engine,if_exists='replace',index=False)

# Create dataframe iterated list
index_list = list()
update_list = list()
link_list = list()

for i in range(len(df)):
    for update in df.iloc[i]['updates']:
        index_list.append(df.iloc[i]['entry_index'])
        update_list.append(update)
    for lnk in df.iloc[i]['update_links']:
        link_list.append(lnk)

updates_df = pd.DataFrame({
    'entry_index' : index_list,
    'updates' : update_list,
    'update_links' : link_list})

# Create table
updates_df.to_sql(name='timeline_update',con=engine,if_exists='replace',index=False)

# Add foreign keys
db_tables = pd.read_sql('SHOW TABLES',con=engine).iloc[:,0]

for t in db_tables:
    if t != 'misconduct_entries':
        add_key_query = '''ALTER TABLE {table} ADD FOREIGN KEY(entry_index) REFERENCES misconduct_entries(entry_index)'''.format(table=t)

        engine.execute(add_key_query)

print('### Complete ###')