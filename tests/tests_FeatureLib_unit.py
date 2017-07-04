import sqlite3
import unittest
from datetime import date
from math import sqrt

import numpy as np

from FeatureLib import Feature, FeatureModel, FootballMatchPredictor
from StatsLib import Stats

RESULTS_FIXTURE_DATA = './fixture/results_2017_04_28.db'


class FeatureTests(unittest.TestCase):

    def test_fn_call_default(self):
        def mk_np_array(x):
            return np.array(x)

        a = Feature([3], feature_making_fn=mk_np_array)
        b = Feature([3])
        self.assertEqual(a,b)

    def test_magnitude(self):
        a = Feature([2, 2])
        e_val = sqrt(2**2+2**2)
        self.assertEqual(e_val, a.magnitude())

    def test_equalities(self):
        a = Feature([2])
        b = Feature([1])
        c = Feature([2])

        self.assertGreater(a, b)
        self.assertLess(b, a)
        self.assertEqual(a, c)

    def test_list_sorting(self):

        a = Feature([2])
        b = Feature([1])
        c = Feature([2])

        unsorted_list = [a, b, c]
        e_val = str([b, a, c])

        sorted_list = sorted(unsorted_list)
        sorted_str = str(sorted_list)

        self.assertEqual(e_val, sorted_str)

    def test_storing_original_data(self):

        a = Feature([2])

        e_val = [2]

        self.assertEqual(e_val, a.input_data)


    def test_storing_original_data_with_id(self):
        a = Feature([2])

        e_val = [2]

        self.assertEqual(e_val, a.input_data)

    def test_using_stats(self):
        with sqlite3.connect(RESULTS_FIXTURE_DATA) as db_conn:
            db_conn.row_factory = sqlite3.Row
            db_cursor = db_conn.cursor()
            team='Arsenal'
            stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=40,
                                                  win_end_date=date(2016, 8, 14))
        self.assertEqual([team, 1, 0, 0, 1, 3, 4, -1, 0], list(stats))

        a = Feature(stats, feature_making_fn=lambda x: np.array([x.goal_diff]))
        self.assertEqual(stats.goal_diff, a)
        self.assertEqual(stats, a.input_data)


class FeatureModelTests(unittest.TestCase):
    """The FeatureModel has all the attributes of a Feature but also gains an identity """
    def test_setting_id(self):
        a = FeatureModel(input_data=[1], id='Andy')
        b = FeatureModel(input_data=[2], id='Bob')

        self.assertEqual(a.id, 'Andy')
        self.assertEqual(b.id, 'Bob')
        self.assertGreater(b, a)


class FootballMatchPredictorTests(unittest.TestCase):

    def test_basic_prediction(self):
        team_a = 'Mudchester United'
        team_b = 'Bogglington Stanley Rovers'
        a = FeatureModel(input_data=[1], id=team_a)
        b = FeatureModel(input_data=[2], id=team_b)

        models = {m.id:m for m in [a,b]}

        expect = ('away_win', -1, None)
        actual = FootballMatchPredictor(models=models).predict(home_team=team_a, away_team=team_b)

        self.assertEqual(expect, actual)


    def test_basic_prediction_with_good_data_flag(self):
        team_a = 'Mudchester United'
        team_b = 'Bogglington Stanley Rovers'
        a = FeatureModel(input_data=[1], id=team_a, good_data=True)
        b = FeatureModel(input_data=[2], id=team_b, good_data=True)

        models = {m.id:m for m in [a,b]}

        expect = ('away_win', -1, None)
        actual = FootballMatchPredictor(models=models).predict(home_team=team_a, away_team=team_b)
        self.assertEqual(expect, actual)

    def test_basic_prediction_with_a_bad_good_data_flag(self):
        team_a = 'Mudchester United'
        team_b = 'Bogglington Stanley Rovers'
        why_bad = 'Testing'
        a = FeatureModel(input_data=[1], id=team_a, good_data=False)
        b = FeatureModel(input_data=[2], id=team_b, good_data=False, bad_data_reason=why_bad)

        models = {m.id:m for m in [a,b]}

        expect = ('away_win', -1, why_bad)
        (a_res, a_dist, a_explanation) = FootballMatchPredictor(models=models).predict(home_team=team_a, away_team=team_b)
        self.assertEqual(expect[0], a_res)
        self.assertEqual(expect[1], a_dist)
        self.assertRegex(a_explanation, why_bad )

    def test_home_and_away_prediction(self):
        team_a = 'Mudchester United'
        team_b = 'Bogglington Stanley Rovers'
        a = FeatureModel(input_data=[1, 1], id=team_a)
        b = FeatureModel(input_data=[2, 0], id=team_b)

        models = {m.id: m for m in [a, b]}

        expect = ('home_win', 1, None)
        actual = FootballMatchPredictor(models=models).predict(home_team=team_a, away_team=team_b)
        self.assertEqual(expect, actual)

    def test_create_models(self):
        """
        """
        with sqlite3.connect(RESULTS_FIXTURE_DATA) as db_conn:
            db_conn.row_factory = sqlite3.Row
            db_cursor = db_conn.cursor()

        last_date = date(2016, 8, 26)
        num_samples = 2
        team1 = 'Arsenal'
        teams = [team1]

        def create_model_fn(fn_team: str):
            team_stat = Stats.n_sample_stats_for_team(cursor=db_cursor,
                                                      team=fn_team,
                                                      last_sample_date=last_date,
                                                      n_samples=num_samples,
                                                      normalize_by_matches=True)
            explanation = ''
            good_data = True
            if team_stat.played < num_samples:
                good_data = False
                explanation = 'Not enough samples. Got %i, wanted %i' % (team_stat.played, num_samples)
            return FeatureModel(input_data=team_stat,
                                     id=team_stat.team_name,
                                     feature_model_making_fn=lambda stat: stat.goal_diff,
                                     good_data=good_data, bad_data_reason=explanation
                                     )

        models:[FeatureModel] = FeatureModel.create_models_for_all_teams(
            model_making_fn=create_model_fn, entities=teams)

        created_model = models[team1]
        self.assertIsInstance(created_model, FeatureModel)
        self.assertEqual(True, created_model.good_data)




if __name__ == '__main__':
    unittest.main()