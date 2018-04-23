
from flask import Flask, render_template, request
from sqlalchemy import create_engine

#import politician_project as pp

app = Flask(__name__)

@app.route('/member_page')

def member_information():
    
    member_id = str(request.args.get('member_id'))

    conn_string = 'mysql://{user}:{password}@{host}:{port}/?charset=utf8'.format(
                    user     = 'root', 
                    password = 'almaibra', 
                    host     = '127.0.0.01', 
                    port     = 3306, 
                    encoding = 'utf-8')

    engine = create_engine(conn_string)
    con = engine.connect()
    
    db = 'congress'
    
    select = '*' #['first_name','last_name','party','state','title','seniority','id','congress']

    table_name = 'congress'
    condition = 'WHERE id = %s'
    con.execute('USE {db}'.format(db=db))
    
    query = ('SELECT {columns} FROM {table} {condition}'
             .format(columns = ','.join(select), table = table_name, condition = condition))

    attributes = list(con.execute(query, (member_id,)))
    image_url = "http://bioguide.congress.gov/bioguide/photo/{b}/{bioguide}.jpg".format(b=member_id[0],bioguide=member_id)

    con.close()

    return render_template('member_page.html', member_id = member_id,
    attributes = list(attributes), image_url = image_url)

app.run(host='0.0.0.0', port=5000, debug=True)
