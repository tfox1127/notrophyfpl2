import os
import pandas as pd
from datetime import datetime as dt
from flask import Flask, render_template, request, redirect, url_for
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

app = Flask(__name__)

DATABASE_URL = os.environ['HEROKU_POSTGRESQL_GAS_URL']

engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")
db = scoped_session(sessionmaker(bind=engine))
app.secret_key = 'pizza'

@app.route('/')
def index():
    posts = db.execute("""SELECT post_number, header, body FROM fpl_blog ORDER BY post_number DESC LIMIT 10""")
    db.commit()
    return render_template('index.html', posts=posts)

@app.route('/fpl_live')
def fpl_live():

    q="""
        SELECT 'rank here', test_do_subs_eow.entry, player_name, sum((multiplier * points) + GREATEST(cast(t_bonus as INT), 0) - bonus) as score_2, 'total here', 'minutes here', 'salary here', 
            cap.cap_player_id, cap.web_name as Captain, vc.vc_player_id, vc.web_name as Vice
        FROM test_do_subs_eow
        LEFT JOIN 
            (SELECT entry, event, element as cap_player_id, web_name FROM api_picks LEFT JOIN (SELECT id, web_name FROM api_elements) as elements ON element = id WHERE api_picks.is_captain) as cap 
            ON test_do_subs_eow.entry = cap.entry

        LEFT JOIN 
            (SELECT entry, event, element as vc_player_id, web_name FROM api_picks LEFT JOIN (SELECT id, web_name FROM api_elements) as elements ON element = id WHERE api_picks.is_vice_captain) as vc
            ON test_do_subs_eow.entry = vc.entry
            
        GROUP BY test_do_subs_eow.entry, player_name, entry_name, cap.web_name, vc.web_name, cap.cap_player_id, vc.vc_player_id
        """

    live_table = db.execute(q)
    db.commit()

    return render_template('fpl_live.html', live_table=live_table)


@app.route('/run_search', methods= ['POST'])
def run_search():
    if request.method == 'POST':
        search_for = request.form['search_for']
        search_for_like = "%" + search_for + "%"
        #elements = db.execute("SELECT * FROM api_elements WHERE UPPER(api_elements.\"web_name\") LIKE UPPER(:search_for_like) OR UPPER(\"api_elements\".\"second_name\") LIKE UPPER(:search_for_like)", {"search_for_like":search_for_like})
        #elements = db.execute(""" SELECT * FROM api_elements WHERE UPPER(api_elements.web_name) LIKE UPPER(:search_for_like) """ , {"search_for_like":search_for_like})
        elements = db.execute(""" 
        SELECT * FROM "api_elements" WHERE UPPER("web_name") LIKE UPPER('%var%')
        """) 

        db.commit()
        return render_template('fpl_search_results.html', elements=elements, search_for=search_for)

if __name__ == '__main__':
    app.run()
