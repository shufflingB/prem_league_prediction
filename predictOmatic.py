#!/usr/bin/env python

import logging
import os
import sqlite3
from argparse import ArgumentParser
import datetime
import re
import sys


lib_path = os.path.join(os.path.dirname(__file__), 'lib')
sys.path.append(lib_path)

# Use separate Home and Away models to predict match results
from StatsLib import Stats
from FeatureLib import FeatureModel, FootballMatchPredictor

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

# Maximum number of samples to use when calculating models, lets go for two years worth
# 19 *
MAX_SAMPLES = 19 * 2 * 2



if __name__ == "__main__":

    parser: ArgumentParser = ArgumentParser()
    parser.description = "Predicts match results based on past encounters using separate Home and Away Goal Diff models"
    parser.add_argument('-r', '--results-sqlite',
                        help='Sqlite file containing past results. Expected schema is (id, date, home_team, home_score, '
                             'away_team, away_score)',
                        required=True,
                        type=str
                        )

    parser.add_argument('-m', '--matches',
                        help='Specify the matches to predict, format is \'home team 1-away team 1, '
                             'home team 2-away team 2, ...\'',
                        required=True,
                        type=str
                        )


    args = parser.parse_args()

    # De-couple front-end cli from program internals
    results_db_file = args.results_sqlite
    matches_str = args.matches




    # Remove preceeding white space around match and team separators and create a list of match tuples
    # to iterate over later.
    matches_cleaned_up_str = re.sub(r'\s*-\s*', '-', re.sub(r'\s*,\s*', ',', matches_str))
    matches_to_predict = [match_pair.split('-') for match_pair in matches_cleaned_up_str.split(',')]

    # From the list of matches we're going to predict obtain a de-duplicated list of teams
    # involved in those matches so that we can build models for them.
    teams = {team:True for match in matches_to_predict for team in match}.keys()

    # Create the models for the teams

    use_data_upto_date = datetime.datetime.today().date()

    with sqlite3.connect(results_db_file) as db_conn:
        db_conn.row_factory = sqlite3.Row
        db_cursor = db_conn.cursor()


        def create_model_fn(fn_team: str):
            team_stat_home = Stats.n_sample_stats_for_team(cursor=db_cursor,
                                                           team=fn_team,
                                                           last_sample_date=use_data_upto_date,
                                                           n_samples=MAX_SAMPLES,
                                                           home_only=True,
                                                           normalize_by_matches=True)

            team_stat_away = Stats.n_sample_stats_for_team(cursor=db_cursor,
                                                           team=fn_team,
                                                           last_sample_date=use_data_upto_date,
                                                           n_samples=MAX_SAMPLES,
                                                           home_only=False,
                                                           normalize_by_matches=True)

            return FeatureModel(
                input_data=[team_stat_home.goal_diff, team_stat_away.goal_diff],
                id=team_stat_home.team_name,
            )


        team_models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
            model_making_fn=create_model_fn, entities=teams)



    for (home_team_name, away_team_name) in matches_to_predict:

        # Use model to make a prediction
        (predicted_result, predicted_distance, _) = FootballMatchPredictor(
            models=team_models
        ).predict(
            home_team=home_team_name, away_team=away_team_name
        )

        print('Predicted results for %s vs %s is a %s with distance  %f' % (home_team_name, away_team_name,
                                                                            predicted_result, predicted_distance))



