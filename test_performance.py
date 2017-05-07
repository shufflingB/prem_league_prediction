#!/usr/bin/env python

# Algorithm being tested - Classic Premier League.
# Primary rank determinant based on summation of  3 points for a win, 1 for a draw
# and 0 for a loss. Secondary determinant is goal difference sum of goals for minus sum of goals against.

import logging
import os
import sqlite3
from datetime import timedelta
from datetime import date

from league_maker import League
from league_maker import ResultsFile

DB_FILE = 'tests/fixture/results_2017_04_28.db'

# How often to recalculate the performance of the metric.
FIRST_CALC_ON = date(2016, 8, 18)  # I'm using the first Thursday after the season starts
RECALC_DELTA = timedelta(days=7)  # and weekly thereafter.

# Create separate tables for home and way performance
HOME_ONLY = None

# Normalise metrics by the number of matches that have been played so that those who have played more, are not over
#  weighted in comparisons.
NORMALISE_BY_MATCHES = True
MIN_WINDOW_SIZE = 2
MAX_WINDOW_SIZE = 40
NUM_TEAMS_IN_LEAGUE = 20

WINDOW_SIZES = [x for x in range(MIN_WINDOW_SIZE, MAX_WINDOW_SIZE + 1)]
DESCRIPTION_TXT = 'Results prediction performance for different window sizes as for standard Premier League position. '\
                  'Except that points and goal difference figures are normalised by the number of matches that a team' \
                  ' has played prior to building the table'

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

with sqlite3.connect(DB_FILE) as db_in_connection:
    db_in_connection.row_factory = sqlite3.Row  # Allows accessing rows by their column names as well as by index
    log = logging.getLogger('log')
    log.info('Analysing feature performance using results in %s' % DB_FILE)
    log.info('Min/max window sizes of %i and %i' % (MIN_WINDOW_SIZE, MAX_WINDOW_SIZE))

    db_in_cursor = db_in_connection.cursor()

    results_for_all_windows = dict()
    header_str = '{:^15}'.format('WinSize (days)')
    mean_str = '{:^15}'.format('Avg accuracy (%)')
    stddev_str = '{:^15}'.format('Std dev')
    for window_size in WINDOW_SIZES:
        #  Work out the performance of the feature at the Window size for each results bin interval
        log.info('Determining performance for window size %i' % window_size)


        def load_next_data(feat_win_end, results_win_end):
            actuals = ResultsFile(cursor=db_in_cursor, win_size=RECALC_DELTA - timedelta(days=1),
                                  win_end=results_win_end)
            feats = League.init_from_db(cursor=db_in_connection, teams=actuals.teams, win_size=window_size,
                                        win_end_date=feat_win_end, normalize_by_matches=NORMALISE_BY_MATCHES)
            return actuals, feats


        # The feature window ends before the results for the interval that it it predicting. i.e
        #  feat win end 1 - length  < feat win data 1 <= feat win end 1 < results data 1
        features_window_ends = FIRST_CALC_ON
        results_window_ends = features_window_ends + RECALC_DELTA
        (actual_results, features) = load_next_data(features_window_ends, results_window_ends)

        results_for_window_size = {}
        predictions_for_window_size = {}
        while features_window_ends <= date.today() and actual_results and features:
            #  Work out the performance of the feature against actual matches this results bin interval.
            log.debug('For win size $s on date %s', features)

            correctly_predicted_count = 0
            number_of_results = 0
            home_wins_predicted = 0
            away_wins_predicted = 0
            draws_predicted = 0

            for match_result in actual_results.results_data:

                home_team_name = match_result['home_team']
                away_team_name = match_result['away_team']

                home_score = match_result['home_score']
                away_score = match_result['away_score']

                match_day = match_result['date']

                # 'Goodness' metric's for use in comparison, lower is better for league rank, so let's convert it
                # so we can use absolute size
                home_metric = NUM_TEAMS_IN_LEAGUE - features.table_position[home_team_name]
                away_metric = NUM_TEAMS_IN_LEAGUE - features.table_position[away_team_name]

                log.debug('Match details %s, %s %i - %i, %s, Prediction features %1.2f - %1.2f' %
                          (match_day, home_team_name, home_score, away_score, away_team_name, home_metric, away_metric))

                home_win_predicted = True if home_metric > away_metric else  False
                home_loss_predicted = True if home_metric < away_metric else  False
                draw_predicted = True if home_metric == away_metric else False

                number_of_results += 1
                if  home_win_predicted and home_score > away_score:
                    log.debug("Home win predicted correctly")
                    correctly_predicted_count += 1
                elif home_loss_predicted and home_score < away_score:
                    log.debug("Home loss predicted correctly")
                    correctly_predicted_count += 1
                elif draw_predicted and home_score == away_score:
                    log.debug("Draw predicted correctly")
                    correctly_predicted_count += 1
                else:
                    log.debug("Prediction wrong")
            #  End for each match_result seeing if the feature predicted it correctly.

            if number_of_results == 0:  # Then we don't have any results, e.g. possible international break etc.
                # We'll deal with this by duplicating the previous result so that we have the same number
                # of stats for each window size.
                log.debug(
                    'For win size %i, ending %s, no results in the next interval, copying last interval\'s' %
                    (window_size, features_window_ends.isoformat()))
                last_date = features_window_ends - RECALC_DELTA
                results_for_window_size[features_window_ends.isoformat()] = results_for_window_size[
                    last_date.isoformat()]
            else:
                log.debug('For window size %i, bin date %s, we predicted correctly = %i, out of %i' % (
                    window_size, features_window_ends.isoformat(), correctly_predicted_count, number_of_results))
                results_for_window_size[
                    features_window_ends.isoformat()] = 100.0 * correctly_predicted_count / number_of_results

            #  Move our window/bin interval along.
            features_window_ends += RECALC_DELTA
            results_window_ends = features_window_ends + RECALC_DELTA
            (actual_results, features) = load_next_data(features_window_ends, results_window_ends)
            #  End while working out the performance of the feature for all bin intervals.

        results_for_all_windows[window_size] = results_for_window_size
        # End working out the performance for each window_size

    # Now lets try and display our results in a way that is both okay to look at visually and works for simple import
    # into a spreadsheet program.
    description_str = ', '.join(['{:<20}'.format('Details'), DESCRIPTION_TXT])
    print(description_str)

    header_str = ', '.join(['{:<20}'.format('Window Sizes'), *['{:^7}'.format(x) for x in WINDOW_SIZES]])
    print(header_str)

    for key in results_for_all_windows[MIN_WINDOW_SIZE].keys():
        bin_end = key
        bin_results = [results_for_all_windows[win_size][bin_end] for win_size in results_for_all_windows.keys()]

        bin_str = ', '.join(['{:<20}'.format(bin_end), *['{:>6}'.format('%.4f' % x) for x in bin_results]])
        print(bin_str)
