import pandas as pd 
import requests
import sys
sys.path.append("/Users/tfox/OneDrive/Scripts/fpl_library")

def pull(link):
    session = requests.session()
    r = session.get(link)
    j = r.json()

    return j

#FIND CURRENT WEEK 
def pull_current_week(): 
    link = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    j = pull(link)

    df = pd.DataFrame(j['events'])

    u = ['id', 'name', 'deadline_time', 'finished', 'data_checked', 'is_previous', 'is_current', 'is_next']
    df = df[u].copy(deep=True)

    THIS_EVENT = df.loc[df['is_current'] == True, 'id'].item()
    STILL_TO_PLAY = df.loc[df['finished'] == False, 'id'].to_list()
    NEXT_EVENT = STILL_TO_PLAY[0]

    return THIS_EVENT, NEXT_EVENT

