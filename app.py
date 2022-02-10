import os, sys
from webbrowser import get
import pandas as pd
import datetime as dt
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
    CURRENT_WEEK, FIRST_UNFINISHED_WEEK = api_check.pull_current_week()
    
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

    # GROUP STAGE
    groups = db.execute(f"""SELECT 
                        "Team 1 ID", "Team 1 Name", "score_1", "price_pct_str_1",
                        "Team 2 ID", "Team 2 Name", "score_2", "price_pct_str_2",
                        "Group", "Match ID"
                        FROM 
            (SELECT "Match ID", "Group", "Team 1 ID", "Team 2 ID", "Team 1 Name", "Team 2 Name" 
                FROM tbl_2122_groups WHERE "GW" = {CURRENT_WEEK}) as GROUPS
        LEFT JOIN 
            (SELECT "entry" as entry_1, "score_3" as score_1, "price_pct_str" as "price_pct_str_1" 
            FROM "calc_score_parts") as SCOREBOARD_1
                ON GROUPS."Team 1 ID" = SCOREBOARD_1.entry_1
        LEFT JOIN 
            (SELECT "entry" as entry_2, "score_3" as score_2, "price_pct_str" as "price_pct_str_2" 
            FROM "calc_score_parts") as SCOREBOARD_2
                ON GROUPS."Team 2 ID" = SCOREBOARD_2.entry_2
        ORDER BY "Group"
        """)

    db.commit()

    return render_template('fpl_live.html', live_table=live_table, groups=groups)

@app.route('/team/<int:fpl_team_id>')
def fpl_team(fpl_team_id):

    # heading
    q = f""" 
        SELECT 
            calc_score_parts.entry, 
            CASE name
                WHEN 'NO_CHIP' THEN player_name
                ELSE CONCAT(player_name, ' (', name, ')')
                END AS player_name,        
            CAST(score_3 AS int) as score_3, 
            CAST(calc_score_parts.total_points - calc_score_parts.event_transfers_cost + calc_score_parts.score_3 AS int) as Total,
            rank_live, 
            change_str
        FROM calc_score_parts
        LEFT JOIN 
            (SELECT entry, player_name FROM api_standings) as names
            ON calc_score_parts.entry = names.entry
        WHERE calc_score_parts.entry = {fpl_team_id}
        ORDER BY rank_live        
    """

    d = db.execute(q)
    db.commit()

    df = pd.DataFrame(d.fetchall(), columns=d.keys())
    df['rang_gw'] = df['score_3'].rank(method='min', ascending=False).fillna(0).round().astype(int)
    dfr = df.to_records(index=False)
    heading = list(dfr)

    #body
    q = f""" SELECT * FROM 
        (SELECT 
            "element", "position", "multiplier", "is_captain", "is_vice_captain", "web_name", "team", "plural_name_short", 
            "fixture", "bps", "t_bonus", "minutes", "goals_scored", "assists", "clean_sheets", "goals_conceded", "own_goals", 
            "penalties_saved", "penalties_missed", "yellow_cards", "red_cards", "saves", "bonus", "team_a", "team_h", 
            "fix_minutes", "status_game", "status_player", "position_name", "score_3", "points", "importance"
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
        LEFT JOIN (
            SELECT "id" as "team_id", "short_name" FROM api_teams
            ) as player_team
            ON "team" = player_team.team_id
        LEFT JOIN (
            SELECT "id" as "h_team_id", "short_name" as "home" FROM api_teams
            ) as home_team
            ON "team_h" = home_team.h_team_id
        LEFT JOIN (
            SELECT "id" as "a_team_id", "short_name" as "away" FROM api_teams
            ) as away_team
            ON "team_a" = away_team.a_team_id
        ORDER BY "position"
    """

    stats = db.execute(q)
    db.commit()

    df = pd.DataFrame(stats.fetchall(), columns=stats.keys())

    df['position_name'] = df['position_name'].fillna("None")

    print(df['position_name'].value_counts())

    fix_these = ["bps", "t_bonus", "minutes", "goals_scored", "assists", "clean_sheets", "goals_conceded", "own_goals", 
            "penalties_saved", "penalties_missed", "yellow_cards", "red_cards", "saves", "bonus", "team_a", "team_h", 
            "fix_minutes", "score_3", "points"]
    for i in fix_these: 
        df[i] = df[i].fillna(0).astype(int)

    stats = df.to_records(index=False)
    stats = list(stats)

    return render_template('fpl_team.html', heading=heading, stats=stats)

def make_roster(df, team): 
    df1_roster = df.loc[df['entry'] == team, 'element'].drop_duplicates().to_list()

    bench = []
    lineup = []
    for i in df1_roster: 
        multi = df.loc[(df['entry'] == team) & (df['element'] == i), 'multiplier'].drop_duplicates().item()
        
        if multi == 0: 
            bench.append(i)
        elif multi == 1: 
            lineup.append(i)
        elif multi > 1: 
            for j in range(multi): 
                lineup.append(i) 

    return lineup, bench

def compare_rosters(row, t1_lineup, t2_lineup, t1_bench, t2_bench):
    
    if (row['element'] in (t1_lineup)) & (row['element'] in (t2_lineup)):
        val = "both"
    elif (row['element'] in (t1_lineup)):
        if row['element'] in (t2_bench): 
            val = "team 1 only (other bench)"
        else: 
            val = "team 1 only"
    elif (row['element'] in (t2_lineup)):
        if row['element'] in (t1_bench): 
            val = "team 2 only (other bench)"
        else: 
            val = "team 2 only"
        
    else:
        val = "bench?"

    return val

def compare_captain(row):
    if (row['multiplier'] > 1):
        val = row['web_name'] + "(" + str(row['multiplier']) + "x)"
    else:
        val = row['web_name']

    return val

def compare_rollup_player(row):
    if (row['status_game'] == "Game Over"):
        val = "Match Over"
    else:
        val = row['web_name_adj']

    return val

def compare_rollup_match(row):
    if (row['status_game'] == "Game Over"):
        val = "-"
    else:
        val = row['match']

    return val

def get_score(team):
    q = f""" SELECT CAST(score_3 AS int) as score_3 FROM calc_score_parts WHERE entry = {team}"""

    d = db.execute(q)
    db.commit()

    df = pd.DataFrame(d.fetchall(), columns=d.keys())
    
    return df.score_3.item()



@app.route('/compare/<int:team1>/<int:team2>')
def compare(team1, team2): 
    
    q_base = f""" SELECT "element", "web_name", "plural_name_short", "player_name", "multiplier", 
                CONCAT("status_game", ' | ', "status_player") as status, CAST("score_3" as INT), 
                CONCAT("home", ' | ', "away") as match, CAST("points" as INT), "entry", "status_game" 
            FROM (
                SELECT "element", "web_name", "plural_name_short", "player_name", "multiplier", 
                    "status_game", "status_player", "score_3", "points", "team_h", "team_a", "entry" FROM scores_player_lvl) as main
            LEFT JOIN (
                SELECT "id" as "h_team_id", "short_name" as "home" FROM api_teams) as home_team
                ON "team_h" = home_team.h_team_id
            LEFT JOIN (
                SELECT "id" as "a_team_id", "short_name" as "away" FROM api_teams) as away_team
                ON "team_a" = away_team.a_team_id
    """

    ###############################################################
    ###############################################################
    # CAPTAIN
    #TEAM 1 
    q_var = f"""WHERE entry = {team1} AND multiplier > 1
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t1_cap = db.execute(q)
    db.commit()

    #TEAM 2 
    q_var = f"""WHERE entry = {team2} AND multiplier > 1
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t2_cap = db.execute(q)
    db.commit()

    ###############################################################
    ###############################################################
    # GKP 
    #TEAM 1 
    q_var = f"""WHERE entry = {team1} AND plural_name_short = 'GKP' AND multiplier < 2
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t1_gkp = db.execute(q)
    db.commit()

    #TEAM 2 
    q_var = f"""WHERE entry = {team2} AND plural_name_short = 'GKP' AND multiplier < 2
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t2_gkp = db.execute(q)
    db.commit()

    ###############################################################
    ###############################################################
    # DEF 
    #TEAM 1 
    q_var = f"""WHERE entry = {team1} AND plural_name_short = 'DEF' AND multiplier < 2
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t1_def = db.execute(q)
    db.commit()

    #TEAM 2 
    q_var = f"""WHERE entry = {team2} AND plural_name_short = 'DEF' AND multiplier < 2
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t2_def = db.execute(q)
    db.commit()

    ###############################################################
    ###############################################################
    # MID 
    #TEAM 1 
    q_var = f"""WHERE entry = {team1} AND plural_name_short = 'MID' AND multiplier < 2
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t1_mid = db.execute(q)
    db.commit()

    #TEAM 2 
    q_var = f"""WHERE entry = {team2} AND plural_name_short = 'MID' AND multiplier < 2
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t2_mid = db.execute(q)
    db.commit()

    ###############################################################
    ###############################################################
    # FWD 
    #TEAM 1 
    q_var = f"""WHERE entry = {team1} AND plural_name_short = 'FWD' AND multiplier < 2
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t1_fwd = db.execute(q)
    db.commit()

    #TEAM 2 
    q_var = f"""WHERE entry = {team2} AND plural_name_short = 'FWD' AND multiplier < 2
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t2_fwd = db.execute(q)
    db.commit()

    q = f"""SELECT entry, player_name FROM api_standings WHERE entry = {team1}"""
    t1_name = db.execute(q)
    db.commit()

    df = pd.DataFrame(t1_name.fetchall(), columns=t1_name.keys())
    t1_name = df.to_records(index=False)
    t1_name = list(t1_name)

    q = f"""SELECT entry, player_name FROM api_standings WHERE entry = {team2}"""
    t2_name = db.execute(q)
    db.commit()

    df = pd.DataFrame(t2_name.fetchall(), columns=t2_name.keys())
    t2_name = df.to_records(index=False)
    t2_name = list(t2_name)

    #TEAM 2 
    q_var = f"""WHERE entry = {team2} AND plural_name_short = 'FWD' AND multiplier < 2
                ORDER BY score_3 DESC"""

    q = q_base + q_var

    t2_fwd = db.execute(q)
    db.commit()

    #ROSTERS 
    q_var = f"""WHERE entry = {team1} or entry = {team2}"""

    q = q_base + q_var
    
    d= db.execute(q)
    db.commit()

    df = pd.DataFrame(d.fetchall(), columns=d.keys())
    df.loc[df['match'] == 'BUR | WAT', 'status_game'] = "Pending"    

    t1_lineup, t1_bench = make_roster(df, team1)
    t2_lineup, t2_bench = make_roster(df, team2)

    df['compare'] = df.apply(compare_rosters, args=(t1_lineup, t2_lineup, t1_bench, t2_bench), axis=1)
    df['web_name_adj'] = df.apply(compare_captain, axis=1)
    df['rollup_player'] = df.apply(compare_rollup_player, axis=1)
    df['rollup_match'] = df.apply(compare_rollup_match, axis=1)

    m = {team1 : "team1", team2: "team2"}
    df['player_name_2'] = df['entry'].map(m)

    dft = df.loc[(df['compare'] == "both"), :]
    both = pd.pivot_table(dft, index='rollup_player', columns='player_name_2', values='score_3', aggfunc='sum').reset_index()
    
    try: 
        both['team1'] = both['team1'].astype(int)
        both = both.sort_values("team1", ascending=False)
        both['team2'] = both['team2'].astype(int)
        both = both.sort_values("team2", ascending=False)
    except: 
        pass
    both = both.to_records(index=False)
    both = list(both)

    #dft = df.loc[(df['compare'].isin(['team 1 only (other bench)', 'team 1 only'])), :]
    dft = df.loc[(df['compare'].isin(['team 1 only (other bench)', 'team 1 only'])), :]
    diff_1 = pd.pivot_table(dft, index=['rollup_player', 'rollup_match'], columns='player_name_2', values='score_3', aggfunc='sum').reset_index()
    diff_1 = diff_1.sort_values('team1',ascending=False)
    diff_1['team1'] = diff_1['team1'].astype(int)
    diff_1 = diff_1.to_records(index=False)
    diff_1 = list(diff_1)

    dft = df.loc[(df['compare'].isin(['team 2 only (other bench)', 'team 2 only'])), :]
    #dft = df.loc[(df['compare'].isin(['team 2 only'])), :]
    diff_2 = pd.pivot_table(dft, index=['rollup_player', 'rollup_match'], columns='player_name_2', values='score_3', aggfunc='sum').reset_index()
    diff_2 = diff_2.sort_values('team2',ascending=False)
    diff_2['team2'] = diff_2['team2'].astype(int)
    try: 
        diff_2 = diff_2.drop(columns=['team1'])
    except: 
        pass
    diff_2 = diff_2.to_records(index=False)
    
    diff_2 = list(diff_2)

    score_1 = get_score(team1)
    score_2 = get_score(team2)

    return render_template('compare.html', t1_cap=t1_cap, t2_cap=t2_cap, t1_gkp=t1_gkp, t2_gkp=t2_gkp, 
                                            t1_def=t1_def, t2_def=t2_def, t1_mid=t1_mid, t2_mid=t2_mid, 
                                            t1_fwd=t1_fwd, t2_fwd=t2_fwd, t1_name=t1_name, t2_name=t2_name, 
                                            both=both, diff_1=diff_1, diff_2=diff_2, t2_bench=t2_bench, 
                                            score_1=score_1, score_2=score_2)

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

@app.route('/fpl_cup')
def fpl_cup():
    # cup table - GROUP A
    q = f""" SELECT "Group", "Team", "Name", CAST("Points" as INT), CAST("W" as INT), CAST("D" as INT), CAST("L" as INT) 
    FROM cup_static_table
    WHERE "Group" = 'A' """

    d = db.execute(q)
    db.commit()

    df = pd.DataFrame(d.fetchall(), columns=d.keys())
    dfr = df.to_records(index=False)
    cup_table_a = list(dfr)

    # cup table - GROUP B
    q = f""" SELECT "Group", "Team", "Name", CAST("Points" as INT), CAST("W" as INT), CAST("D" as INT), CAST("L" as INT) 
    FROM cup_static_table
    WHERE "Group" = 'B' """

    d = db.execute(q)
    db.commit()

    df = pd.DataFrame(d.fetchall(), columns=d.keys())
    dfr = df.to_records(index=False)
    cup_table_b = list(dfr)

    # cup table - GROUP C
    q = f""" SELECT "Group", "Team", "Name", CAST("Points" as INT), CAST("W" as INT), CAST("D" as INT), CAST("L" as INT) 
    FROM cup_static_table
    WHERE "Group" = 'C' """

    d = db.execute(q)
    db.commit()

    df = pd.DataFrame(d.fetchall(), columns=d.keys())
    dfr = df.to_records(index=False)
    cup_table_c = list(dfr)

    # cup table - GROUP D
    q = f""" SELECT "Group", "Team", "Name", CAST("Points" as INT), CAST("W" as INT), CAST("D" as INT), CAST("L" as INT) 
    FROM cup_static_table
    WHERE "Group" = 'D' """

    d = db.execute(q)
    db.commit()

    df = pd.DataFrame(d.fetchall(), columns=d.keys())
    dfr = df.to_records(index=False)
    cup_table_d = list(dfr)

    # cup table - GROUP E
    q = f""" SELECT "Group", "Team", "Name", CAST("Points" as INT), CAST("W" as INT), CAST("D" as INT), CAST("L" as INT) 
    FROM cup_static_table
    WHERE "Group" = 'E' """

    d = db.execute(q)
    db.commit()

    df = pd.DataFrame(d.fetchall(), columns=d.keys())
    dfr = df.to_records(index=False)
    cup_table_e = list(dfr)

    return render_template('fpl_cup.html', cup_table_a=cup_table_a, 
        cup_table_b=cup_table_b, 
        cup_table_c=cup_table_c, 
        cup_table_d=cup_table_d, 
        cup_table_e=cup_table_e)

if __name__ == '__main__':
    app.run()
