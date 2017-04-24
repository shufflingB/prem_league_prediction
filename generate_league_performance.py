#!/usr/bin/env python

# Algorithm being tested - Classic Premier League.
# Primary rank determinant based on summation of  3 points for a win, 1 for a draw
# and 0 for a loss. Secondary determinant is goal difference sum of goals for minus sum of goals against.

import sqlite3
import datetime
import statistics
import logging
import os

DB_FILE = '/Users/jhume/work/fantasy_football/windowed_stats.db'
DB_OUT_FILE = '/Users/jhume/work/fantasy_football/stats_out.db'
WEEK_OFFSET = datetime.timedelta(days=7)

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

with sqlite3.connect(DB_FILE) as db_in_connection:
    log = logging.getLogger('log')

    db_in_cursor = db_in_connection.cursor()

    def get_stats_window_sizes() -> [str]:
        # Get the window sizes that we have previously calculated windowed base statistics and information for
        sql = 'select DISTINCT win_size FROM windowed_stats ORDER BY win_size ASC'
        sql_out = db_in_cursor.execute(sql).fetchall()
        return list(map(lambda x: x[0], sql_out))  # fetchall returns an array of tuples, just need first element


    def get_end_of_gameweek_thursdays() -> [datetime]:
        # Get the end of gameweek thursdays that we have calculated windowed stats for
        sql = 'select DISTINCT win_end_thursday FROM windowed_stats ORDER BY win_end_thursday ASC'
        sql_out = db_in_cursor.execute(sql).fetchall()
        # fetchall returns an array of tuples, just need first element
        return list(map(lambda x: datetime.datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S'), sql_out))


    def get_next_match_details(on_thursday: str) -> [(str, str, bool, str, str, str)]:
        # An array of tuples containing the details of the next match results
        # (next_date, home_team, away_team, result_for_home_team)
        sql = 'SELECT DISTINCT next_match, team, next_opponent, next_result FROM windowed_stats WHERE ' \
              'win_end_thursday="%s" AND next_at_home ORDER BY team ASC' % on_thursday

        return db_in_cursor.execute(sql).fetchall()


    def calculate_league(on_date: str, window_size: str) -> {(int, int, int)}:
        # Returns dict with team name key and values are [(position, 1st_determinant, 2nd_determinant, ...)]
        # where primary_determinant might be say points and secondary would be goal difference

        sql_create_table = 'select team, played, won, drawn, lost,won*3+drawn AS points,' \
                           'home_diff+away_diff AS goal_diff FROM ' \
                           'windowed_stats WHERE ' \
                           'win_size="%s" AND win_end_thursday="%s" ORDER BY ' \
                           'points DESC, goal_diff DESC' % (window_size, on_date)

        league_table = db_in_cursor.execute(sql_create_table).fetchall()
        log.debug('For window size = %s league table is' % window_size)
        table_dict = {}
        position = 0
        last_gd = last_pts = None
        for row in range(0, len(league_table)):
            (team, played, won, drawn, lost, points, goal_diff) = league_table[row]

            # Cope with drawn league positions
            if row + 1 > 1:
                if points != last_pts or goal_diff != last_gd:
                    position = row + 1
            else:
                position = row + 1

            log.debug('Pos = %s, Team = %s, P = %s, GD = %s, Pts = %s' % (position, team, played, goal_diff, points))
            table_dict[team] = (position, points, goal_diff)
            last_pts = points
            last_gd = goal_diff

        return table_dict


    win_sizes = get_stats_window_sizes()
    # win_sizes = [6]

    end_of_gameweek_thursdays = get_end_of_gameweek_thursdays()

    print('Predictive performance results')
    print('Winsize (wks), Accuracy (%), Stdev, Weekly Performances (%s)')
    for window_size in win_sizes:
        pcent_rights_for_win_size = []

        for thursday in end_of_gameweek_thursdays:

            # Skip gameweeks that we don't have the results to compare against with yet
            if thursday > datetime.datetime.today():
                continue

            log.debug('------------------- Window size %s week ending %s ----------------------------------------------'
                  % (window_size, thursday))
            next_match_details = get_next_match_details(on_thursday=str(thursday))
            log.debug('Next match details %s' % next_match_details)

            league_hash = calculate_league(on_date=str(thursday), window_size=str(window_size))

            count_predicted = 0  # Hold's count of predicted results
            for match in next_match_details:
                (next_match_day, home_team,  away_team, result_for_home_team) = match
                (home_position, home_det1, home_det2) = league_hash[home_team]
                (away_position, away_det1, away_det2) = league_hash[away_team]
                log.debug('%s vs %s, %i vs %i, Result for home team %s' % (home_team, away_team, home_position, away_position,
                                                                       result_for_home_team))
                if home_position < away_position and result_for_home_team == 'win':
                    log.debug("Predicted home win yay")
                    count_predicted += 1
                elif home_position > away_position and result_for_home_team == 'loss':
                    log.debug("Predicted home loss yay")
                    count_predicted += 1
                elif home_position == away_position and result_for_home_team == 'draw':
                    log.debug("Predicted draw yay")
                    count_predicted += 1
                else:
                    log.debug("Dang it, got wrong result")

            num_matches = len(next_match_details)
            if num_matches:
                pcent_predicted = 100.0*count_predicted/num_matches
                pcent_rights_for_win_size.append(pcent_predicted)
            else:
                pcent_predicted = 0

            logging.debug('winsize = %i, # matches = %i,  # predicted = %i, %% predicted %2.1f' %
                  (window_size, num_matches, count_predicted, pcent_predicted ))

        log.debug('pcent_rights_for_win_size %s ' % pcent_rights_for_win_size)
        mean = statistics.mean(pcent_rights_for_win_size)
        stddev = statistics.stdev(pcent_rights_for_win_size)

        log.debug('=== League performance with window size = %i, average accuracy = %2.2f, std dev = %2.2f ===' %
                  (window_size, mean, stddev ))
        print('%i, %2.2f, %2.2f, %s' % (window_size, mean, stddev, ', '.join(map(lambda x : str(x), pcent_rights_for_win_size))))