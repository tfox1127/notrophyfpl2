import os
import pandas as pd
from datetime import datetime as dt
from flask import Flask, render_template, request, redirect, url_for
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

app = Flask(__name__)

#DATABASE_URL
DATABASE_URL = os.environ['DATABASE_URL']

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

    return render_template('fpl_live.html')


@app.route('/run_search', methods= ['POST'])
def run_search():
    if request.method == 'POST':
        search_for = request.form['search_for']
        search_for_like = "%" + search_for + "%"
        elements = db.execute("SELECT * FROM ftbl_elli2 WHERE UPPER(ftbl_elli2.\"web_name\") LIKE UPPER(:search_for_like) OR UPPER(\"ftbl_elli2\".\"team_name\") LIKE UPPER(:search_for_like)", {"search_for_like":search_for_like})
        db.commit()
        return render_template('search_results.html', elements=elements, search_for=search_for)

if __name__ == '__main__':
    app.run()
