import streamlit as st
import streamlit.components.v1 as components
import altair as alt
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import pandas as pd

st.set_page_config(layout="wide")

st.title("Borussia MyAwesomeDataStuff")

bundesliga_table = '<iframe src="https://widgets.livesoccertv.com/tables/germany/bundesliga/" width="100%" height="902" frameborder="0" scrolling="no"></iframe><div style="font-size:12px;font-family:Roboto, Verdana, Geneva, sans-serif; padding-left:10px;">Table provided by <a target="_blank" href="https://www.livesoccertv.com/">Live Soccer TV</a></div>'
latest_news = '<a class="twitter-timeline" data-height="1000" data-theme="dark" href="https://twitter.com/BL_LatestNews?ref_src=twsrc%5Etfw">Tweets by BL_LatestNews</a> <script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>'

# Initialize connection.
# Uses st.cache to only run once.
# @st.cache(allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"], cursor_factory=RealDictCursor)

conn = init_connection()

# Perform query.
# Uses st.cache to only rerun when the query changes or after 10 min.
@st.cache(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

clubs = run_query("""
    SELECT
        club, COUNT(*)
    FROM season_data
    LEFT JOIN league_data USING (club)
    GROUP BY club

""")

st.sidebar.title("Filter data")
club_list_raw = st.sidebar.multiselect("Select Club", pd.DataFrame(clubs)['club'].unique())
# Hacky and gross, don't even look at it

if len(club_list_raw) > 0:
    club_list = "("
    for row in club_list_raw:
        club_list += "'" + row + "'"
    club_list += ")"
else:

    club_list = "("
    for row in pd.DataFrame(clubs)['club'].unique():
        club_list += "'" + row + "'"
    club_list += ")"

club_list = club_list.replace("''", "','")

games = run_query("SELECT date, hometeam, score, awayteam from game_data WHERE date < '{}' AND (awayteam in {} OR hometeam in {});".format(
        datetime.today().strftime('%Y-%m-%d'),
        club_list,
        club_list
    )
)

remaining = run_query("SELECT date, hometeam, 'TBD' score, awayteam from game_data WHERE date < '{}' AND (awayteam in {} OR hometeam in {});".format(
        datetime.today().strftime('%Y-%m-%d'),
        club_list,
        club_list
    )
)

table = run_query("""
    SELECT
        club,
        COUNT(*) played,
        SUM(points) points,
        SUM(conceded) scored,
        SUM(scored) conceded,
        SUM(conceded) - SUM(scored) dif,
        '€' || ROUND((MAX(total_market_value)/1000000)::integer, 2)::varchar(255) || 'M' market_value
    FROM season_data
    LEFT JOIN league_data USING (club)
    WHERE club IN {}
    GROUP BY club
    ORDER BY points DESC;
""".format(club_list))

finance = run_query("""
    SELECT
        club,
        COUNT(*) played,
        SUM(points) points,
        SUM(conceded) scored,
        SUM(scored) conceded,
        SUM(conceded) - SUM(scored) dif,
        '€' || ROUND(((MAX(total_market_value)/SUM(scored))::integer)/1000000, 2)::varchar(255) || 'M' player_cost_per_goal,
        COUNT(*) FILTER (WHERE points = 3) wins,
        '€' || ROUND(((MAX(total_market_value)/COUNT(*) FILTER (WHERE points = 3))/1000000)::integer, 2)::varchar(255) || 'M' cost_per_win,
        '€' || ROUND((MAX(total_market_value)/1000000)::integer, 2)::varchar(255) || 'M' market_value
    FROM season_data
    LEFT JOIN league_data USING (club)
    WHERE club IN {}
    GROUP BY club
    ORDER BY points DESC;
""".format(club_list))


club_values = run_query("""
    SELECT
        club,
        ROUND((MAX(total_market_value))::integer, 2)::integer market_value
    FROM season_data
    LEFT JOIN league_data USING (club)
    WHERE club in {}
    GROUP BY club;
""".format(club_list))

player_values = run_query("""
    SELECT
        club,
        ROUND((MAX(average_player_value))::integer, 2)::integer average_player_value
    FROM season_data
    LEFT JOIN league_data USING (club)
    WHERE club in {}
    GROUP BY club;
""".format(club_list))

cost_per_win = run_query("""
    SELECT
        club,
        ROUND(((MAX(total_market_value)/COUNT(*) FILTER (WHERE points = 3))/1000000)::integer, 2)::integer cost_per_win
    FROM season_data
    LEFT JOIN league_data USING (club)
    WHERE club in {}
    GROUP BY club;
""".format(club_list))




st.title("Table")

html = ""

for row in finance:

    html += """
    
    <tr>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
    </tr>
    
    """.format(
        row['club'],
        # row['played'],
        row['points'],
        row['scored'],
        row['conceded'],
        row['dif'],
        row['player_cost_per_goal'],
        row['wins'],
        row['cost_per_win'],
        row['market_value']
    )

components.html("""
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
<script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
<table class="table table-dark">
  <tr>
    <th>Club</th>
    <th>Points</th>
    <th>Scored</th>
    <th>Conceded</th>
    <th>Dif</th>
    <th>Cost Per Goal</th>
    <th>Wins</th>
    <th>Cost Per Win</th>
    <th>Market Value</th>
  </tr>
  {}
  </table>
""".format(html), height=940)
# st.table(table)

# st.bar_chart(data=club_values, width=0, height=0, use_container_width=True)
st.title("Club Market Values")
st.write("The value of all the clubs players based on Transfermarkt data")
club_value_chart = alt.Chart(pd.DataFrame(club_values)).mark_bar().encode(
  x=alt.X('club'),
  y=alt.Y('market_value'),
  color=alt.Color("club"),
  tooltip=[alt.Tooltip('market_value', title='Value €')]
)
st.altair_chart(club_value_chart, use_container_width=True)

st.title("Club Average Player Cost")
st.write("The value of all the clubs players averaged based on Transfermarkt data")
player_value_chart = alt.Chart(pd.DataFrame(player_values)).mark_bar().encode(
  x=alt.X('club'),
  y=alt.Y('average_player_value'),
  color=alt.Color("club"),
  tooltip=[alt.Tooltip('average_player_value', title='Value €')]
)
st.altair_chart(player_value_chart, use_container_width=True)

st.title("Club Cost Per Win")
st.write("Based on current club value, how much does it cost to win a Bundesliga game")
cost_per_win_chart = alt.Chart(pd.DataFrame(cost_per_win)).mark_bar().encode(
  x=alt.X('club'),
  y=alt.Y('cost_per_win'),
  color=alt.Color("club"),
  tooltip=[alt.Tooltip('cost_per_win', title='Cost €')]
)
st.altair_chart(cost_per_win_chart, use_container_width=True)


st.title("Latest Updates")
components.html(latest_news, height=500)

st.title("Bundesliga Played Fixtures")
st.table(data=games)

st.title("Bundesliga Remaining Fixtures")
st.table(data=remaining)
