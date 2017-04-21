#!/usr/bin/env python

import sqlite3
import datetime
from argparse import ArgumentParser

DB_FILE = '/Users/jhume/work/fantasy_football/test_out.db'
DB_OUT_FILE = '/Users/jhume/work/fantasy_football/tables_out.db'



# Total number of Premier League matches each team plays
MAX_MATCHES = 20 *2 - 2

# First Thursday after the season starts. It and subsequent Thursday is chosen as the date that that the program will
# calculate league positions on because:
# a) All matches for a gameweek are completed.
# b) not normally any matches on a Thursday.
SEASON_FIRST_THU= '2016-08-18'

# Weeks in season
SEASON_WEEKS = 40



SQL_CREATE_TABLE = '''
CREATE TABLE league_tables (
    id INTEGER PRIMARY KEY,
    win_size INTEGER NOT NULL,
    thursday TEXT NOT NULL,
    team TEXT NOT NULL,
    played INTEGER NOT NULL,
    won INTEGER NOT NULL,
    drawn INTEGER NOT NULL,
    lost INTEGER NOT NULL,
    home_diff INTEGER NOT NULL,
    away_diff INTEGER NOT NULL,
    goal_diff INTEGER NOT NULL,
    points INTEGER NOT NULL,
    next_match INTEGER NOT NULL,
    next_at_home INTEGER NOT NULL,
    next_opponent INTEGER NOT NULL,
    next_result TEXT NOT NULL
); '''

SQL_INSERT = '''INSERT INTO league_tables (win_size, thursday, team, played, won, drawn, lost, home_diff, away_diff, 
goal_diff, points, next_match, next_at_home, next_opponent, next_result) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

# parser: ArgumentParser = ArgumentParser()
# parser.description = "Downloads the Premier League results for the current season from the BBC"
#
# parser.add_argument('-i', '--in-db',
#                     help='Specifies the url to download from %s' % DB_FILE,
#                     default=DB_FILE,
#                     required=False,
#                     type=str
#                     )
#
# parser.add_argument('-o', '--out-db',
#                     help='Specifies the name of sqlite DB to OVERWRITE %s' % DB_FILE,
#                     default=DB_FILE,
#                     required=False,
#                     type=str
#                     )


in_db_file = DB_FILE

with sqlite3.connect(in_db_file) as db_in_connection:
    db_in_cursor = db_in_connection.cursor()

    # Get the team names
    sql_teams = 'SELECT DISTINCT results.home_team FROM results'
    query_out = db_in_cursor.execute(sql_teams).fetchall()
    teams = list(map(lambda x: x[0], query_out))  # fetchall returns an array, just need first element

    first_thursday = datetime.datetime.strptime(SEASON_FIRST_THU, '%Y-%m-%d')
    week = datetime.timedelta(days=7)
    thursdays_for_the_season = [first_thursday + x * week for x in range(SEASON_WEEKS)]

    with sqlite3.connect(DB_OUT_FILE) as db_out_connection:
        db_out_cursor = db_out_connection.cursor()
        db_out_cursor.execute(SQL_CREATE_TABLE)

        for window_size in range(1, SEASON_WEEKS+1):
        # for window_size in range(1, 2):
            for window_end_thursday in thursdays_for_the_season:
                # Determine the first Thursday in the window being used for this table
                window_start_thursday = window_end_thursday - (window_size * week)
                print('Win start date = %s, win end date = %s' % (window_start_thursday, window_end_thursday))
                # table.
                if window_end_thursday >= datetime.datetime.today() + week:  # Calculate up to the current week in play but no more
                    continue

                print("==== League at %s ===" % window_end_thursday)
                for team in teams:
                    # Find the wins
                    SQL_WINS_DATES = 'SELECT date FROM results WHERE ' \
                                     '((home_team=? AND (home_score > away_score)) OR ' \
                                     '(away_team=? AND (away_score > home_score))) AND ' \
                                     'date BETWEEN ? AND ?'
                    query_out = db_in_cursor.execute(SQL_WINS_DATES, (team,
                                                                      team,
                                                                      window_start_thursday.isoformat(),
                                                                      window_end_thursday.isoformat()
                                                                      )
                                                     ).fetchall()
                    wins = len(query_out)

                    # Find the draws
                    SQL_DRAW_DATES = 'SELECT date FROM results WHERE ' \
                                     '((home_team=? AND (home_score == away_score)) OR ' \
                                     '(away_team=? AND (away_score == home_score))) AND ' \
                                     'date BETWEEN ? AND ?'
                    query_out = db_in_cursor.execute(SQL_DRAW_DATES, (team,
                                                                      team,
                                                                      window_start_thursday.isoformat(),
                                                                      window_end_thursday.isoformat()
                                                                      )
                                                     ).fetchall()
                    draws = len(query_out)

                    # Find the losses
                    SQL_LOSE_DATES = 'SELECT date FROM results WHERE ' \
                                     '((home_team=? AND (home_score < away_score)) OR ' \
                                     '(away_team=? AND (away_score < home_score))) AND ' \
                                     'date BETWEEN ? AND ?'
                    query_out = db_in_cursor.execute(SQL_LOSE_DATES, (team,
                                                                      team,
                                                                      window_start_thursday.isoformat(),
                                                                      window_end_thursday.isoformat()
                                                                      )
                                                     ).fetchall()
                    losses = len(query_out)

                    # Calculate goal differences
                    SQL_GOAL_DIFF_HOME = 'SELECT sum(home_score-away_score) FROM results WHERE home_team=? AND ' \
                                         'date BETWEEN ? AND ?'
                    query_out = db_in_cursor.execute(SQL_GOAL_DIFF_HOME, (team,
                                                                          window_start_thursday.isoformat(),
                                                                          window_end_thursday.isoformat()
                                                                          )
                                                     ).fetchone()
                    home_diff = query_out[0]

                    if home_diff is None:  # Cope with possibly no home games at start of season
                        home_diff = 0

                    SQL_GOAL_DIFF_AWAY = 'SELECT sum(away_score-home_score) FROM results WHERE away_team=? AND ' \
                                         'date BETWEEN ? AND ? '
                    query_out = db_in_cursor.execute(SQL_GOAL_DIFF_AWAY, (team,
                                                                          window_start_thursday.isoformat(),
                                                                          window_end_thursday.isoformat()
                                                                          )
                                                    ).fetchone()
                    away_diff = query_out[0]
                    if away_diff is None:  # Cope with possible no away games at start of season
                        away_diff = 0

                    # Calculate meta-stats
                    points = 3* wins + draws
                    played = wins + draws + losses
                    goal_diff = home_diff + away_diff


                    # Find the next match and associated information

                    SQL_NEXT_MATCH = 'SELECT date, home_team, away_team, home_score, away_score FROM results WHERE ' \
                                     'date > ? AND (home_team=? OR away_team=?) '\
                                     'ORDER BY date LIMIT 1'
                    query_out = db_in_cursor.execute(SQL_NEXT_MATCH,
                                                     (window_end_thursday.isoformat(), team, team)).fetchone()
                    # print(query_out)
                    next_match_day = next_home_team = next_away_team = 'no data'
                    if query_out is not None:
                        (next_match_day, next_home_team, next_away_team, next_home_score, next_away_score) = query_out
                    next_at_home = False

                    next_opponents = next_home_team
                    if team == next_home_team:
                        next_at_home = True
                        next_opponents = next_away_team

                    next_result = 'draw'
                    if next_at_home:
                        if next_home_score > next_away_score :
                            next_result = 'win'
                        elif next_home_score < next_away_score:
                            next_result = 'loss'
                    else:
                        if next_home_score < next_away_score:
                            next_result = 'win'
                        elif next_home_score > next_away_score:
                            next_result = 'loss'



                    print('%s played %s, won %s, drawn %s, lost %s, home_diff %s, away_diff %s, goal_diff %s, points %s' %
                          (team, played, wins, draws, losses, home_diff, away_diff, goal_diff, points))
                    print('         win size %s, next match %s, at home %s against %s result %s' %
                          (window_size, next_match_day, next_at_home, next_opponents, next_result))

                    SQL_INSERT = '''INSERT INTO league_tables (win_size, thursday, team, played, won, drawn, lost, home_diff, away_diff, 
                    goal_diff, points, next_match, next_at_home, next_opponent, next_result) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''


                    db_out_cursor.execute(SQL_INSERT, (window_size, window_end_thursday, team, played, wins, draws, losses, home_diff, away_diff, goal_diff, points, next_match_day, next_at_home, next_opponents, next_result))
                    # Find the team's next home match and away match

        db_out_connection.commit()