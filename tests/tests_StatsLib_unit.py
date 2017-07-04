import sqlite3
import unittest
import logging
from datetime import date, datetime

from StatsLib import Stats


RESULTS_FIXTURE_DATA = './fixture/results_2017_04_28.db'

FIRST_MATCHDAY = date.fromtimestamp(datetime.strptime('2016-08-13', '%Y-%m-%d').timestamp())

class StatsTests(unittest.TestCase):

    def setUp(self):
        global db_connection
        db_connection = sqlite3.connect(RESULTS_FIXTURE_DATA)
        db_connection.row_factory = sqlite3.Row
        global db_cursor
        db_cursor = db_connection.cursor()

    def tearDown(self):
        db_connection.close()

    def test_stats_iterable(self):
        team = 'Southampton'
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=2,
                                              win_end_date=date(2017, 4, 28))
        self.assertIsInstance(list(stats), list)

    def test_stats_windowed_no_results(self):
        # Check okay with no data for the team
        team = 'Arsenal'  # Didn't play until date(2016, 8, 14)
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=40,
                                              win_end_date=FIRST_MATCHDAY)
        self.assertEqual([team, 0, 0, 0, 0, 0, 0, 0, 0],list(stats))

    def test_stats_n_samples_no_results(self):
        # Check okay with no data for the team
        team = 'Arsenal'  # Didn't play until date(2016, 8, 14)
        stats = Stats.n_sample_stats_for_team(cursor=db_cursor, team=team, last_sample_date=FIRST_MATCHDAY, n_samples=10)
        self.assertEqual([team, 0, 0, 0, 0, 0, 0, 0, 0],list(stats))



    def test_stats_windowed_single_results_no_others(self):
        # Check okay with single match for the team
        team = 'Arsenal'  # Didn't play until date(2016, 8, 14)
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=40,
                                              win_end_date=date(2016, 8, 14))
        self.assertEqual([team, 1, 0, 0, 1, 3, 4, -1, 0], list(stats))

    def test_stats_n_samples_single_result_when_others(self):
        # Check okay with no data for the team
        team = 'Arsenal'  # Arsenals next match after their first of the season was on 20th of August which was a
        # nil-nil
        stats = Stats.n_sample_stats_for_team(cursor=db_cursor, team=team, last_sample_date=date(2016, 8, 20), n_samples=1)
        self.assertEqual([team, 1, 0, 1, 0, 0, 0, 0, 1], list(stats))

        self.assertEqual(date(2016, 8, 20), stats.cover_from)
        self.assertEqual(date(2016, 8, 20), stats.cover_to)



    def test_stats_n_samples_home_last_two(self):
        team = 'Arsenal'  # Didn't play until date(2016, 8, 14)
        stats = Stats.n_sample_stats_for_team(cursor=db_cursor, team=team, n_samples=2,
                                              last_sample_date=date(2016, 9, 23), home_only=True)
        self.assertEqual([team, 2, 1, 0, 1, 5, 5, 0, 3], list(stats))

    def test_stats_n_samples_away_last_two(self):
        team = 'Arsenal'  # Didn't play until date(2016, 8, 14)
        stats = Stats.n_sample_stats_for_team(cursor=db_cursor, team=team, n_samples=2,
                                              last_sample_date=date(2016, 9, 23), home_only=False)
        self.assertEqual([team, 2, 2, 0, 0, 7, 2, 5, 6], list(stats))
        self.assertEqual(date(2016, 8, 27), stats.cover_from)
        self.assertEqual(date(2016, 9, 23), stats.cover_to)


    def test_stats_not_enough_data(self):
        team = 'Arsenal'
        # On the 23rd of September Arsenal had played away three times,  but only at home twice
        stats = Stats.n_sample_stats_for_team(cursor=db_cursor, team=team, n_samples=2,
                                              last_sample_date=date(2016, 9, 23), home_only=False)
        self.assertEqual([team, 2, 2, 0, 0, 7, 2, 5, 6], list(stats))
        self.assertEqual(2, stats.n_samples)

        stats = Stats.n_sample_stats_for_team(cursor=db_cursor, team=team, n_samples=3,
                                              last_sample_date=date(2016, 9, 23), home_only=True)
        # Expect it to give us some stat.
        self.assertEqual([team, 2, 1, 0, 1, 5, 5, 0, 3], list(stats))




    def test_stats_watford_regression_n_samples(self):
        team = 'Watford'
        stats = Stats.n_sample_stats_for_team(cursor=db_cursor,
                                      team=team,
                                      last_sample_date=date(2016, 8, 26),
                                      n_samples=2,
                                      normalize_by_matches=False)
        self.assertEqual([team, 2, 0, 1, 1, 2, 3, -1, 1], list(stats))




    def test_stats_windowed_instance_variables(self):
        # Check can access the same data via the named instance variables
        team = 'Arsenal'  # Didn't play until date(2016, 8, 14)
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=40,
                                              win_end_date=date(2016, 8, 14))
        self.assertEqual([team, 1, 0, 0, 1, 3, 4, -1, 0], list(stats))
        self.assertEqual(team, stats.team_name)
        self.assertEqual(1, stats.played)
        self.assertEqual(0, stats.won)
        self.assertEqual(0, stats.drawn)
        self.assertEqual(1, stats.lost)
        self.assertEqual(3, stats.score_for)
        self.assertEqual(4, stats.score_against)
        self.assertEqual(-1, stats.goal_diff)
        self.assertEqual(0, stats.points)


    def test_stats_windowed_two_results(self):
        team = 'Tottenham Hotspur'
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=2,
                                              win_end_date=date(2017, 4, 28))
        self.assertEqual([team, 2, 2, 0, 0, 5, 0, 5, 6], list(stats))

    def test_stats_windowed_home_only(self):
        team = 'Tottenham Hotspur'
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=2,
                                              win_end_date=date(2017, 4, 28), home_only=True)
        self.assertEqual([team, 1, 1, 0, 0, 4, 0, 4, 3], list(stats))

    def test_stats_windowed_away_only(self):
        team = 'Tottenham Hotspur'
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=2,
                                              win_end_date=date(2017, 4, 28), home_only=False)
        self.assertEqual([team, 1, 1, 0, 0, 1, 0, 1, 3], list(stats))

    def test_stats_windowed_wks40_all(self):
        # Tests all for the season
        team = 'Arsenal'
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=40,
                                              win_end_date=date(2017, 4, 28))
        self.assertEqual(['Arsenal', 32, 18, 6, 8, 64, 40, 24, 60], list(stats))

    def test_stats_windowed_normalised_by_num_matches(self):
        # Tests all for the season
        team = 'Arsenal'
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=40,
                                              win_end_date=date(2017, 4, 28), normalize_by_matches=True)
        self.assertEqual(['Arsenal', 32, 18, 6, 8, 64, 40, 24 / 32, 60 / 32], list(stats))

    def test_stats_windowed(self):
        team = 'Arsenal'  # Played on the 20th and the 27th, so should be only one result
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=1,
                                              win_end_date=date(2016, 8, 27))
        self.subTest(1)
        self.assertEqual(['Arsenal', 1, 1, 0, 0, 3, 1, 2, 3], list(stats))

        team = 'Sunderland'  # Sunderland played on the 21st and 27th so two results
        stats = Stats.windowed_stats_for_team(cursor=db_cursor, team=team, win_weeks=1,
                                              win_end_date=date(2016, 8, 27))
        self.subTest(2)
        self.assertEqual(['Sunderland', 2, 0, 1, 1, 2, 3, -1, 1], list(stats))

if __name__ == '__main__':
    unittest.main()