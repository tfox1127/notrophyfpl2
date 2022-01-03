import os, sys
import pandas as pd
from datetime import datetime as dt
from flask import Flask, render_template, request, redirect, url_for, session
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import api_check 

CURRENT_WEEK, FIRST_UNFINISHED_WEEK = api_check.pull_current_week()

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

@app.route('/login', methods = ["POST", "GET"])
def login():

    q = "SELECT player_name FROM api_standings ORDER by player_name"
    fpl_players = db.execute(q)
    db.commit()

    df = pd.DataFrame(fpl_players.fetchall(), columns=fpl_players.keys())
    test = df['player_name'].to_list()
    

    if request.method == "POST":
        #return request.form['user_name']
        user_name = request.form["user_name"]

        q = """SELECT entry FROM api_standings WHERE UPPER(player_name) = UPPER((:user_name))"""
        user_entry = db.execute(q, {"user_name": user_name})
        db.commit()
        
        user_entry = user_entry.fetchall()
        user_entry = user_entry[0][0]   
                
        session['user_name'] = user_name
        session['user_entry'] = user_entry

        return redirect(url_for("fpl_live"))   
        
    else:
        return render_template("/login.html", fpl_players = fpl_players, test=test)

@app.route('/fpl_live')
def fpl_live():

    q = """ 
        SELECT rank_live, 
            calc_score_parts.entry, 
            CASE name
                WHEN 'NO_CHIP' THEN player_name
                ELSE CONCAT(player_name, ' (', name, ')')
                END AS player_name,
            CAST(score_3 AS int) as score_3, 
            CAST(calc_score_parts.total_points - calc_score_parts.event_transfers_cost + calc_score_parts.score_3 AS int) as Total,
            ROUND(CAST(played_games AS numeric), 1) as test, 
            price_pct_str as Salary, 
            cap.cap_player_id, 
            cap.web_name as Captain, 
            vc.vc_player_id,
            vc.web_name as Vice,
            change_str,
            CAST(expected_games AS int), 
            ROUND(CAST(salary_possible AS numeric), 1) as salary_possible
        FROM calc_score_parts
        LEFT JOIN 
            (SELECT entry, player_name FROM api_standings) as names
            ON calc_score_parts.entry = names.entry
        LEFT JOIN 
            (SELECT entry, event, element as cap_player_id, web_name FROM api_picks LEFT JOIN (SELECT id, web_name FROM api_elements) as elements ON element = id WHERE api_picks.is_captain) as cap 
            ON calc_score_parts.entry = cap.entry

        LEFT JOIN 
            (SELECT entry, event, element as vc_player_id, web_name FROM api_picks LEFT JOIN (SELECT id, web_name FROM api_elements) as elements ON element = id WHERE api_picks.is_vice_captain) as vc
            ON calc_score_parts.entry = vc.entry

        ORDER BY rank_live

    """

    live_table = db.execute(q)
    db.commit()

    df = pd.DataFrame(live_table.fetchall(), columns=live_table.keys())
    live_table = df.to_records(index=False)
    live_table = list(live_table)

    #ACTIVE GAMES 
    groups = db.execute(f"""SELECT 
                        "Team 1 ID", "Team 1 Name", "score_1", "price_pct_str_1",
                        "Team 2 ID", "Team 2 Name", "score_2", "price_pct_str_2",
                        "Group"
                        FROM 
        (SELECT "Group", "Team 1 ID", "Team 2 ID", "Team 1 Name", "Team 2 Name" FROM tbl_2122_groups WHERE "GW" = {CURRENT_WEEK}) as GROUPS
        LEFT JOIN 
            (SELECT "entry" as entry_1, "score_3" as score_1, "price_pct_str" as "price_pct_str_1" FROM "calc_score_parts") as SCOREBOARD_1
                ON GROUPS."Team 1 ID" = SCOREBOARD_1.entry_1
        LEFT JOIN 
            (SELECT "entry" as entry_2, "score_3" as score_2, "price_pct_str" as "price_pct_str_2" FROM "calc_score_parts") as SCOREBOARD_2
                ON GROUPS."Team 2 ID" = SCOREBOARD_2.entry_2
        ORDER BY "Group"
        """)

    db.commit()

    return render_template('fpl_live.html', live_table=live_table, groups=groups)

@app.route('/team/<int:fpl_team_id>')
def fpl_team(fpl_team_id):
    q = f""" SELECT * FROM 
        (SELECT 
            "element", "position", "multiplier", "is_captain", "is_vice_captain", "web_name", "team", "plural_name_short", 
            "fixture", "bps", "t_bonus", "minutes", "goals_scored", "assists", "clean_sheets", "goals_conceded", "own_goals", 
            "penalties_saved", "penalties_missed", "yellow_cards", "red_cards", "saves", "bonus", "team_a", "team_h", 
            "fix_minutes", "status_game", "status_player", "position_name", "score_3", "points"
        FROM scores_player_lvl
        WHERE entry = {fpl_team_id}) as scores_player_lvl
        LEFT JOIN (
            SELECT 
                "id", ROUND(CAST(now_cost AS numeric) / 10, 1) as cost, "points_per_game", "value_form", "value_season", "ict_index"
                "chance_of_playing_next_round", "chance_of_playing_this_round", "ep_this", "ep_next",
                "selected_by_percent", "transfers_in_event", "transfers_out_event"
            FROM api_elements
            )  as element_info
        ON scores_player_lvl.element = element_info.id
        
    """

    stats = db.execute(q)
    db.commit()

    df = pd.DataFrame(stats.fetchall(), columns=stats.keys())

    fix_these = ["bps", "t_bonus", "minutes", "goals_scored", "assists", "clean_sheets", "goals_conceded", "own_goals", 
            "penalties_saved", "penalties_missed", "yellow_cards", "red_cards", "saves", "bonus", "team_a", "team_h", 
            "fix_minutes", "score_3", "points"]
    for i in fix_these: 
        df[i] = df[i].fillna(0).astype(int)

    stats = df.to_records(index=False)
    stats = list(stats)

    return render_template('fpl_team.html', stats=stats)

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
