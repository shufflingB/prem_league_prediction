#!/usr/bin/env python

import logging
import os
import sqlite3

from datetime import timedelta
from datetime import date
from ActualResultsLib import ActualResults
from FeatureLib import FeatureModel, FootballMatchPredictor
from StatsLib import Stats
import unittest



RAW_MATCH_RESULTS_IN_DB_FILE = os.path.join(os.path.dirname(__file__), 'fixture','results_2016_2017_season.db')
print(RAW_MATCH_RESULTS_IN_DB_FILE)
TEST_OUTPUT_STEM_DIR = os.path.join(os.path.dirname(__file__), 'algorithm_test_results')
print(TEST_OUTPUT_STEM_DIR)

NUM_OF_TEAMS = 20
ALL_TEAMS_PLAYED_HOME_OR_AWAY = date(2016, 8, 16)  #  Prior to this date not all teams had played at least once
ALL_TEAMS_PLAYED_HOME_AND_AWAY = date(2016, 9, 11)  #  Prior to this date not all teams have played home and away once
HALF_MATCHES_PLAYED = date(2017, 1, 2)
LAST_DAY_OF_SEASON = date(2017, 5, 20)


# Ground sizes, form Wikipedia, for the season being analysed.
GROUND_SIZES = {'Chelsea': 	41663, 'Tottenham Hotspur':	36284, 'Manchester City': 55097, 'Liverpool': 54074,
                'Arsenal': 60432, 'Manchester United': 75643, 'Everton': 39572, 'Southampton': 32505,
                'Bournemouth': 11464, 'West Bromwich Albion': 26852, 'West Ham United': 60000, 'Leicester City': 32315,
                'Stoke City': 27902, 'Crystal Palace': 25456, 'Swansea City': 21088, 'Burnley': 21800, 'Watford': 21438,
                'Hull City': 25450, 'Middlesbrough': 33746, 'Sunderland': 49000}

# Order for the league under analysis
SANITY_TEST_LEAGUE_ORDER = ['Chelsea', 'Tottenham Hotspur', 'Manchester City', 'Liverpool', 'Arsenal',
                            'Manchester United', 'Everton', 'Southampton', 'Bournemouth', 'West Bromwich Albion',
                            'West Ham United', 'Leicester City', 'Stoke City', 'Crystal Palace', 'Swansea City',
                            'Burnley', 'Watford', 'Hull City', 'Middlesbrough', 'Sunderland']


# Override by default by setting Environmental variable 'LOGLEVEL' to 'DEBUG' to the program emit more debug information
logging.basicConfig(level=logging.DEBUG)

###
#  Definitions related to the sqlite database - here for info, unlikely to want to tinker with.
###

#  Default SQL bindings that should be with any query specific ones prior to a query.

SQL_CREATE_TEST_LOGGING = \
    """
    CREATE TABLE 
      test_logging_table (
          id INTEGER PRIMARY KEY,
          samples_requested INTEGER NOT NULL,
          match_date TEXT NOT NULL,
          prediction_correct BOOLEAN NOT NULL,
          home_team  TEXT NOT NULL,
          away_team TEXT NOT NULL,
          predicted_result TEXT NOT NULL,
          predicted_distance REAL,
          variants TEXT,
          actual_result TEXT NOT NULL,
          actual_home_score INTEGER NOT NULL,
          actual_away_score INTEGER NOT NULL
      )
    """

SQL_DROP_TEST_LOGGING = \
    """
    DROP TABLE IF EXISTS test_logging_table
    """

SQL_INSERT_TEST_LOG_ENTRY = \
    """
    INSERT INTO 
      test_logging_table (
          samples_requested,
          match_date,
          prediction_correct,
          home_team,
          away_team,
          predicted_result,
          predicted_distance,
          variants,
          actual_result,
          actual_home_score,
          actual_away_score
      )  
      VALUES (
    
          :samples_requested, 
          :match_date, 
          :prediction_correct, 
          :home_team, 
          :away_team, 
          :predicted_result, 
          :predicted_distance, 
          :variants, 
          :actual_result, 
          :actual_home_score, 
          :actual_away_score
      )
    """

SQL_CREATE_TEST_MODELS_LOG = \
    """
    CREATE TABLE 
      test_models_table (
          id INTEGER PRIMARY KEY,
          date TEXT NOT NULL,
          team TEXT NOT NULL,
          model_name TEXT NOT NULL,
          feature TEXT NOT NULL
      )
    """
SQL_DROP_TEST_MODELS_LOG = \
    """
    DROP TABLE IF EXISTS test_models_table
    """

SQL_INSERT_TEST_MODELS_LOG = \
    """
    INSERT INTO 
      test_models_table (
          date,
          team,
          model_name,
          feature
      )
      VALUES (
        :date,
        :team,
        :model_name,
        :feature
      )
"""


class StatsPredictionPremierLeague(unittest.TestCase):

    def setUp(self):
        """ As a convenience, defines teams and dates to make predictions for and sets up connections to the two sqlite
        databases that contains, or will contain:
        1) Existing raw match results data that will be used for creating the models and testing against.
        2) A per test case, logging database that holds:
            a) The models used.
            b) The parametric results of the test, such that they can be analysed subsequently in an sqlite console,
            excel etc.
        """
        global db_in_connection, \
            db_in_cursor, \
            num_matches_in_season, \
            teams, \
            played_home_OR_away_before_dates, \
            played_home_AND_away_before_dates, \
            db_log_connection, \
            db_log_cursor

        # Set up our connection to the raw input match data
        db_in_connection = sqlite3.connect(RAW_MATCH_RESULTS_IN_DB_FILE)
        db_in_connection.row_factory = sqlite3.Row
        db_in_cursor = db_in_connection.cursor()
        
        # Setup our output logging connection for the test results and their associated models
        db_log_file_path = '%s/%s.db' % (TEST_OUTPUT_STEM_DIR, self.id().split('.')[-1])
        db_log_connection = sqlite3.connect(db_log_file_path)
        db_log_cursor = db_log_connection.cursor()

        db_log_cursor.execute(SQL_DROP_TEST_LOGGING)
        db_log_cursor.execute(SQL_CREATE_TEST_LOGGING)
        db_log_cursor.execute(SQL_DROP_TEST_MODELS_LOG)
        db_log_cursor.execute(SQL_CREATE_TEST_MODELS_LOG)

        # Create a couple of lists of match dates we will be making predictions for, we do this by removing and
        # candidate dates where we do not have enough data to derive the models. The:
        # - First list is prediction dates where all teams have played home OR away least once i.e. when it's sensible
        # to make predictions for models that combine home and away information to make predictions.
        # - Second list is for dates when teams have played at at least once, home AND away and it makes sense to start
        #  doing predictions using distinct home and away models
        # TODO: P4 these dates could be extracted from the SQL data, rather than being hard coded.
        all_match_dates = ActualResults.get_dates(db_cursor=db_in_cursor)
        played_home_OR_away_before_dates = [i for i in all_match_dates if i >= ALL_TEAMS_PLAYED_HOME_OR_AWAY]
        played_home_AND_away_before_dates = [i for i in all_match_dates if i >= ALL_TEAMS_PLAYED_HOME_AND_AWAY]
        
        teams = ActualResults.get_teams(db_cursor=db_in_cursor)
        num_matches_in_season = 2 * (NUM_OF_TEAMS - 1)

    def tearDown(self):
        db_in_connection.close()
        db_log_connection.commit()
        db_log_connection.close()

    def persist_models(self, model_gen_date, model_description, models):
        for team in models:
            sql_bindings = {
                'date': model_gen_date.isoformat(),
                'team': team,
                'model_name': model_description,
                'feature': str(models[team])
            }
            db_log_cursor.execute(SQL_INSERT_TEST_MODELS_LOG, sql_bindings)

    @staticmethod
    def crange(first, test, update):
        # It's kind of annoying that you can't use the default for range combo to do the equivalent
        # to C's for (i = 0; i += anything i < whatever). However, this from Stackoverflow allows something similar.
        # https://stackoverflow.com/questions/2740901/simulating-c-style-for-loops-in-python
        # Usage is     for i in  crange(0,lambda i:i<anything,lambda i:i+whatever):
        # print i
        while test(first):
            yield first
            first = update(first)


    def make_and_store_predictions_for_date(self, match_date: date, models: {str: FeatureModel},
                                            variants=None, draw_range:(float,float) = None):
        """

        :param match_date: Match date object for which predictions are to be made
        :param models: dictionary or tuple, str is the name of the team/entity for which the FeatureModel pertains,
        FeatureModel is the model for that team/entity. In comparisons the largest model is assumed to be the victor.
        :param variants: Arbitrary free text string describing how the model varies, e.g. 'manager="joe blogs".
         Recommend that whatever is used, it easy to search for in sqlite.
        :param draw_range: tuple defining the lowest and highest of the range of distances that should be classified
         as drawn outcomes.

        Description
        Takes a supplied match date and set of models and uses them to make predictions against the actual match
         results. Those match results, predictions and models are stored in the logging database for future analysis
         and posterity.
        """

        for unseen_match_data in ActualResults.get_results_data(db_cursor=db_in_cursor,
                                                                win_size=timedelta(days=0),  # 0 == Just that day
                                                                win_end=match_date):
            (match_day, home_team_name, home_score, away_team_name, away_score, actual_result) = \
                ActualResults.unpack_match_result_data(unseen_match_data)

            # Use model to make a prediction
            (predicted_result, predicted_distance, _) = FootballMatchPredictor(
                models=models,
            ).predict(
                home_team=home_team_name, away_team=away_team_name)
            logging.debug(predicted_result, predicted_distance)

            # Override the vanilla prediction if distance is not large enough
            if draw_range is not None:
                if draw_range[0] <= predicted_distance <= draw_range[1]:
                    predicted_result = 'draw'



            # Compare the prediction to the actual result
            prediction_correct = True if predicted_result == actual_result else False

            # Persist the setup and results
            sql_binding = {
                'samples_requested': self.num_samples,
                'match_date': match_date.isoformat(),
                'prediction_correct': prediction_correct,
                'home_team': home_team_name,
                'away_team': away_team_name,
                'predicted_result': predicted_result,
                'predicted_distance': float(predicted_distance),
                'variants': variants,
                'actual_result': actual_result,
                'actual_home_score': home_score,
                'actual_away_score': away_score
            }
            db_log_cursor.execute(SQL_INSERT_TEST_LOG_ENTRY, sql_binding)

    # End of create_model_fn

    def test_010_premier_league_and_sanity_test(self):
        """Premier League position for predicting match results and sanity test the system.
        It's not actually true Premier League position, which is 3 points for a win, 1 for a draw and then teams
        are further split up or down depending on goal difference. But instead we approximate by using points and then
        adding a tiny amount (small enough that over the course of a season points will always be greater) for goal
        difference to nudge teams that would be tied on points apart.

        We use this  as our starting point and also as a place to inject a few sanity tests to make sure that
        the prediction recording and associated code paths look okay as we know what the end league result should
        look like.
        """

        def create_premier_league_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=False)

            #  Approximate premier league rank, by using points and a tiny nudge (compared to points) from goal
            # difference to separate those on the same points.
            return FeatureModel(input_data=team_stat,
                                id=team_stat.team_name,
                                feature_model_making_fn=lambda stat: stat.points + stat.goal_diff / 1000000
                                )

        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_premier_league_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(),
                                models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)

        ###
        #  Sanity tests on the above.
        ###

        # Check how many predictions we've made, should be equal to the number of matches in the season minus
        # the number skipped at the beginning to ensure all teams have played at least once so that the models
        # have something to work on.
        num_expected_match_predictions = (NUM_OF_TEAMS - 1) * NUM_OF_TEAMS - NUM_OF_TEAMS / 2
        sql_query = 'SELECT COUNT(*) FROM test_logging_table'
        num_match_predictions = db_log_cursor.execute(sql_query).fetchone()[0]
        self.assertEqual(num_expected_match_predictions, num_match_predictions)

        #  Also do a quick/slightly hacky sanity test, because we know what the final premier league table looked like
        # at the end of the season, by building models for day after the league has ended, and then seeing how they rank
        expected_prem_league_order = SANITY_TEST_LEAGUE_ORDER

        #  Here comes the icky
        self.model_date = LAST_DAY_OF_SEASON + timedelta(days=1)
        models: [FeatureModel] = FeatureModel.create_models_for_all_teams(
            model_making_fn=create_premier_league_model_fn, entities=teams)

        self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)
        sql_query = 'SELECT team FROM test_models_table WHERE date=:date ORDER BY feature DESC'
        generated_model_order = [tupe[0] for tupe in
                                 db_log_cursor.execute(sql_query, {'date': self.model_date.isoformat()}).fetchall()]
        self.assertEqual(expected_prem_league_order, generated_model_order)

    def test_020_premier_league_normalised(self):
        """  Use normalised (approximated) Premier League Rank to predict match results, same test as 10, except
        that the approximation is normalised by the amount of matches played.
        """

        def create_premier_league_normalised_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)

            #  Approximate premier league rank, by using points and a tiny nudge (compared to points) from goal
            # difference to separate those on the same points.
            return FeatureModel(input_data=team_stat,
                                id=team_stat.team_name,
                                feature_model_making_fn=lambda stat: stat.points + stat.goal_diff / 1000000
                                )

        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_premier_league_normalised_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(),
                                models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)

    def test_030_premier_league_normalised_points(self):
        """ Premier League points normalised by number of matches played i.e. no Goal Difference information in models
        as was the case in 10 and 20.
        """

        def create_premier_league_normalised_points_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)

            #  Approximate premier league rank, by using points and a tiny nudge (compared to points) from goal
            # difference to separate those on the same points.
            return FeatureModel(input_data=team_stat,
                                id=team_stat.team_name,
                                feature_model_making_fn=lambda stat: stat.points
                                )

        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_premier_league_normalised_points_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(),
                                models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)

    def test_040_normalised_goal_difference(self):
        """ Premier League Goal Difference normalised by the number of matches played.
        """

        def create_premier_league_normalised_goal_diff_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)

            return FeatureModel(input_data=team_stat,
                                id=team_stat.team_name,
                                feature_model_making_fn=lambda stat: stat.goal_diff
                                )

        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_premier_league_normalised_goal_diff_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(),
                                models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)

    def test_041_normalised_goal_difference_home_only(self):
        """ 1 of 2 exploring the relative import of home and away Goal Differerence data for making match predictions,
        other is 42
        """

        def create_premier_league_normalised_goal_diff_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True, home_only=True)

            return FeatureModel(input_data=team_stat,
                                id=team_stat.team_name,
                                feature_model_making_fn=lambda stat: stat.goal_diff
                                )

        for match_date in played_home_AND_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_premier_league_normalised_goal_diff_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(),
                                models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)

    def test_042_normalised_goal_difference_away_only(self):
        """ 2 of 2 exploring the relative import of home and away Goal Differerence data for making match predictions,
        other is 41
        """

        def create_premier_league_normalised_goal_diff_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True, home_only=False)

            return FeatureModel(input_data=team_stat,
                                id=team_stat.team_name,
                                feature_model_making_fn=lambda stat: stat.goal_diff
                                )

        for match_date in played_home_AND_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_premier_league_normalised_goal_diff_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(),
                                models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)



    def test_065_normalised_wins_only(self):
        """Wins only - 1 of 3 exploring the relative importance of win, draws and losses in predicting results, others
        are 66 and 67.
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=False)

            return FeatureModel(input_data=team_stat,
                                id=team_stat.team_name,
                                feature_model_making_fn=lambda stat: stat.won / stat.played
                                )

        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(),
                                models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)

    def test_066_normalised_draws_only(self):
        """Draws only - 2 of 3 exploring the relative importance of win, draws and losses in predicting results, others
        are 65 and 67.
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=False)

            return FeatureModel(input_data=team_stat,
                                id=team_stat.team_name,
                                feature_model_making_fn=lambda stat: stat.drawn / stat.played
                                )

        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)

    def test_067_normalised_loser_only(self):
        """Losses only - 3 of 3 exploring the relative importance of win, draws and losses in predicting results, others
        are 65 and 66.
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=False)
            return FeatureModel(input_data=team_stat,
                                id=team_stat.team_name,
                                feature_model_making_fn=lambda stat: (-1 * stat.lost) / stat.played
                                )

        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)

    def test_070_boosted_goal_difference_for_home_models(self):
        """ Giving the home team a 0.72 head start, 0.72 was determined from 071, the 0.72 was determined from 71.
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)


            return FeatureModel(
                input_data=[self.home_boost + team_stat.goal_diff, team_stat.goal_diff],
                            id=team_stat.team_name
                )


        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.home_boost = 0.72
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)



    def test_071_various_boosted_goal_difference_for_home_models(self):
        """ Looking at the effect of varying the amount of Home boost between 0 and and 500% on prediction performance.
        Aim of this test is to see what he optimum boost would have been.
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)


            return FeatureModel(
                input_data=[self.home_boost + team_stat.goal_diff, team_stat.goal_diff],
                            id=team_stat.team_name
                )

        # TODO: convert this to use crange
        for i in range(0, 201):
            boost = i/100

            for match_date in played_home_OR_away_before_dates:
                ####
                #  Build model up to the day before the match
                ####
                self.home_boost = boost
                self.model_date = match_date - timedelta(days=1)
                self.num_samples = num_matches_in_season

                models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                    model_making_fn=create_model_fn, entities=teams)

                model_desc = 'gdn_boost_%s' % boost
                self.persist_models(model_gen_date=self.model_date, model_description=model_desc, models=models)

                self.make_and_store_predictions_for_date(match_date=match_date, models=models, variants=model_desc)




    def test_074_home_ground(self):
        """ Predict match results based on the home ground size of teams. This actually works quite well, I would
        guess partially because large home grounds will be intimidating to the opposition, but also because
        it probably correlates with the economic clout of the club and likely team value/quality.
        """

        def create_model_fn(fn_team: str):
            new_feat = GROUND_SIZES[fn_team]

            return FeatureModel(
                input_data=[(self.home_boost * new_feat + new_feat),
                            new_feat],
                id=fn_team,
                )

        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.home_boost = 0.0
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)



    def test_080_goal_difference_separate_home_away_models(self):
        """ Goal Difference, normalised and using distinct home and away models that have been derived from only
        the home or away data respectively.
        """

        def create_model_fn(fn_team: str):
            team_stat_home = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                           team=fn_team,
                                                           last_sample_date=self.model_date,
                                                           n_samples=self.num_samples,
                                                           home_only=True,
                                                           normalize_by_matches=True)

            team_stat_away = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                           team=fn_team,
                                                           last_sample_date=self.model_date,
                                                           n_samples=self.num_samples,
                                                           home_only=False,
                                                           normalize_by_matches=True)

            return FeatureModel(
                input_data=[team_stat_home.goal_diff, team_stat_away.goal_diff],
                id=team_stat_home.team_name,
                )

        for match_date in played_home_AND_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models)



    def test_100_moving_windows_at_various_sizes(self):
        """ Comparison of moving analysis window sizes using normalised Goal Difference to see if using a data
        windowing approach that routinely discards old results leads to improved performances.
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)

            return FeatureModel(
                input_data=[(abs(self.home_boost * team_stat.goal_diff) + team_stat.goal_diff) / team_stat.played,
                            team_stat.goal_diff / team_stat.played],
                id=team_stat.team_name,
                )

        for num_samples in range(1, num_matches_in_season+1):
            for match_date in played_home_OR_away_before_dates:
                ####
                #  Build model up to the day before the match
                ####
                self.home_boost = 0.0
                self.model_date = match_date - timedelta(days=1)
                self.num_samples = num_samples

                models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                    model_making_fn=create_model_fn, entities=teams)

                model_description = '%s - num_samples = %s' % (self.shortDescription(), num_samples)
                print(model_description)
                self.persist_models(model_gen_date=self.model_date, model_description=model_description, models=models)

                self.make_and_store_predictions_for_date(match_date=match_date, models=models, variants=model_description)


    def test_200_boosted_goal_difference_for_home_models_with_thresholds(self):
        """ Giving the home team a 0.72 head start, 0.72 determined from 071, with thresholds of 0.3 and 0.9 determined
        from frequency diagram for the same. Aim is to see what happens if what the best is that might happen if we
        had select a static data range for the classification of drawn matches.
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)


            return FeatureModel(
                input_data=[self.home_boost + team_stat.goal_diff, team_stat.goal_diff],
                            id=team_stat.team_name
                )


        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.home_boost = 0.72
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models, draw_range=(0.3, 0.9))

    def test_205_boosted_goal_difference_for_home_models_with_thresholds(self):
        """ Giving the home team a 0.72 head start, 0.72 determined from 071. Thresholds determined by by examing  the
        data from 210 and 220. Lower of  -0.792 (higher than than  0.3 in 200) and 1.945 (higher than 0.9 in 200). Aim
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)


            return FeatureModel(
                input_data=[self.home_boost + team_stat.goal_diff, team_stat.goal_diff],
                            id=team_stat.team_name
                )


        for match_date in played_home_OR_away_before_dates:
            ####
            #  Build model up to the day before the match
            ####
            self.home_boost = 0.72
            self.model_date = match_date - timedelta(days=1)
            self.num_samples = num_matches_in_season

            models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                model_making_fn=create_model_fn, entities=teams)

            self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)

            self.make_and_store_predictions_for_date(match_date=match_date, models=models, draw_range=(-0.792, 1.945))



    def test_210_boosted_goal_difference_for_home_models_with_various_lower_away_win_threshold(self):
        """ Giving the home team a 0.72 head start, 0.72 determined from 071, with various upper thresholds. This is
        intended to see if can find a threshold value that gives us a significant level of confidence in the
        prediction of away_wins.
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)


            return FeatureModel(
                input_data=[self.home_boost + team_stat.goal_diff, team_stat.goal_diff],
                            id=team_stat.team_name
                )

        default_threshold_lower = 0.3
        default_threshold_upper = 0.9

        explore_range = (-2.0, default_threshold_upper)
        num_steps_wanted = 60
        step_size = (explore_range[1] - explore_range[0])/num_steps_wanted

        threshold_upper = default_threshold_upper
        for threshold_lower in StatsPredictionPremierLeague.crange(first=explore_range[0], test=lambda x: x <= explore_range[1],
                                                             update=lambda x: x + step_size):
            for match_date in played_home_OR_away_before_dates:
                ####
                #  Build model up to the day before the match
                ####
                self.home_boost = 0.72
                self.model_date = match_date - timedelta(days=1)
                self.num_samples = num_matches_in_season

                models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                    model_making_fn=create_model_fn, entities=teams)

                self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)

                # variant_string = 'threshold_lower=%f, threshold_upper=%f' % (threshold_lower, threshold_upper)
                self.make_and_store_predictions_for_date(match_date=match_date, models=models, draw_range=(threshold_lower, threshold_upper),
                                                         variants=threshold_lower)


    def test_220_boosted_goal_difference_for_home_models_with_various_upper_home_win_threshold(self):
        """ Giving the home team a 0.72 head start, 0.72 determined from 071, with various upper thresholds. This is
        intended to see if can find a threshold value that gives us a significant level of confidence in the
        prediction of home_wins.
        """

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_in_cursor,
                                                      team=fn_team,
                                                      last_sample_date=self.model_date,
                                                      n_samples=self.num_samples,
                                                      normalize_by_matches=True)


            return FeatureModel(
                input_data=[self.home_boost + team_stat.goal_diff, team_stat.goal_diff],
                            id=team_stat.team_name
                )

        default_threshold_lower = 0.3
        default_threshold_upper = 0.9

        explore_range = (default_threshold_lower, 5.0)
        num_steps_wanted = 60
        step_size = (explore_range[1] - explore_range[0])/num_steps_wanted

        threshold_lower = default_threshold_lower
        for threshold_upper in StatsPredictionPremierLeague.crange(first=explore_range[0], test=lambda x: x <= explore_range[1],
                                                             update=lambda x: x + step_size):
            for match_date in played_home_OR_away_before_dates:
                ####
                #  Build model up to the day before the match
                ####
                self.home_boost = 0.72
                self.model_date = match_date - timedelta(days=1)
                self.num_samples = num_matches_in_season

                models: {str: FeatureModel} = FeatureModel.create_models_for_all_teams(
                    model_making_fn=create_model_fn, entities=teams)

                self.persist_models(model_gen_date=self.model_date, model_description=self.shortDescription(), models=models)

                # variant_string = 'threshold_lower=%f, threshold_upper=%f' % (threshold_lower, threshold_upper)
                self.make_and_store_predictions_for_date(match_date=match_date, models=models, draw_range=(threshold_lower, threshold_upper),
                                                         variants=threshold_upper)

