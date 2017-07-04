import os
import unittest
import sqlite3
from datetime import date

from StatsLib import Stats, create_league_using_windowed_stats

RESULTS_FIXTURE_DATA = os.path.join(os.path.dirname(__file__), 'fixture','results_2017_04_28.db')

TEAMS = ['Arsenal', 'Bournemouth', 'Burnley', 'Chelsea', 'Crystal Palace', 'Everton', 'Hull City',
         'Leicester City', 'Liverpool', 'Manchester City', 'Manchester United', 'Middlesbrough',
         'Southampton', 'Stoke City', 'Sunderland', 'Swansea City', 'Tottenham Hotspur', 'Watford',
         'West Bromwich Albion', 'West Ham United']

class LeagueGenerationUsingPremierLeagueRules(unittest.TestCase):
    def setUp(self):
        global db_connection
        db_connection = sqlite3.connect(RESULTS_FIXTURE_DATA)
        db_connection.row_factory = sqlite3.Row
        global db_cursor
        db_cursor = db_connection.cursor()

    def tearDown(self):
        db_connection.close()

    def test_league_wks40_all(self):
        league = create_league_using_windowed_stats(cursor=db_cursor, teams=TEAMS, win_size=40,
                                                    win_end_date=date(2017, 4, 28),
                                                    stats_ranking_function=Stats.premier_league_ranking_fn)

        expected_stats = [[1, 'Chelsea', 33, 25, 3, 5, 69, 29, 40, 78],
                          [2, 'Tottenham Hotspur', 33, 22, 8, 3, 69, 22, 47, 74],
                          [3, 'Liverpool', 34, 19, 9, 6, 70, 42, 28, 66],
                          [4, 'Manchester City', 33, 19, 8, 6, 63, 35, 28, 65],
                          [5, 'Manchester United', 33, 17, 13, 3, 50, 24, 26, 64],
                          [6, 'Arsenal', 32, 18, 6, 8, 64, 40, 24, 60],
                          [7, 'Everton', 34, 16, 10, 8, 60, 37, 23, 58],
                          [8, 'West Bromwich Albion', 33, 12, 8, 13, 39, 42, -3, 44],
                          [9, 'Southampton', 32, 11, 7, 14, 39, 44, -5, 40],
                          [10, 'Watford', 33, 11, 7, 15, 37, 54, -17, 40],
                          [11, 'Stoke City', 34, 10, 9, 15, 37, 50, -13, 39],
                          [12, 'Crystal Palace', 34, 11, 5, 18, 46, 54, -8, 38],
                          [13, 'Bournemouth', 34, 10, 8, 16, 49, 63, -14, 38],
                          [14, 'West Ham United', 34, 10, 8, 16, 44, 59, -15, 38],
                          [15, 'Leicester City', 33, 10, 7, 16, 41, 54, -13, 37],
                          [16, 'Burnley', 34, 10, 6, 18, 33, 49, -16, 36],
                          [17, 'Hull City', 34, 9, 6, 19, 36, 67, -31, 33],
                          [18, 'Swansea City', 34, 9, 4, 21, 39, 68, -29, 31],
                          [19, 'Middlesbrough', 34, 5, 12, 17, 24, 43, -19, 27],
                          [20, 'Sunderland', 33, 5, 6, 22, 26, 59, -33, 21]]

        stats_list = [[row[0], *list(row[1])] for row in list(league)]

        self.assertEqual(expected_stats, stats_list)

    def test_league_wks40_home_only(self):
        league = create_league_using_windowed_stats(cursor=db_cursor, teams=TEAMS, win_size=40,
                                                    win_end_date=date(2017, 4, 28), home_only=True,
                                                    stats_ranking_function=Stats.premier_league_ranking_fn)

        expected_stats = [[1, 'Tottenham Hotspur', 17, 15, 2, 0, 43, 8, 35, 47],
                          [2, 'Chelsea', 16, 14, 0, 2, 43, 13, 30, 42],
                          [3, 'Everton', 17, 12, 4, 1, 41, 13, 28, 40],
                          [4, 'Liverpool', 17, 11, 4, 2, 42, 18, 24, 37],
                          [5, 'Arsenal', 16, 11, 3, 2, 32, 15, 17, 36],
                          [6, 'Burnley', 17, 10, 2, 5, 23, 16, 7, 32],
                          [7, 'Manchester City', 16, 8, 7, 1, 27, 15, 12, 31],
                          [8, 'Manchester United', 17, 7, 9, 1, 23, 11, 12, 30],
                          [9, 'Leicester City', 16, 9, 3, 4, 26, 18, 8, 30],
                          [10, 'West Bromwich Albion', 17, 9, 2, 6, 27, 20, 7, 29],
                          [11, 'Watford', 17, 8, 4, 5, 25, 23, 2, 28],
                          [12, 'Hull City', 17, 8, 4, 5, 27, 26, 1, 28],
                          [13, 'Bournemouth', 17, 8, 3, 6, 31, 26, 5, 27],
                          [14, 'Stoke City', 17, 7, 5, 5, 23, 20, 3, 26],
                          [15, 'Southampton', 15, 6, 4, 5, 17, 18, -1, 22],
                          [16, 'West Ham United', 17, 6, 4, 7, 18, 27, -9, 22],
                          [17, 'Swansea City', 17, 6, 3, 8, 24, 33, -9, 21],
                          [18, 'Crystal Palace', 17, 5, 2, 10, 20, 23, -3, 17],
                          [19, 'Middlesbrough', 17, 4, 5, 8, 14, 19, -5, 17],
                          [20, 'Sunderland', 17, 3, 5, 9, 16, 31, -15, 14]]

        stats_list = [[row[0], *list(row[1])] for row in list(league)]
        self.assertEqual(expected_stats, stats_list)

    def test_league_wks40_away_only(self):
        league = create_league_using_windowed_stats(cursor=db_cursor, teams=TEAMS, win_size=40,
                                                    win_end_date=date(2017, 4, 28), home_only=False,
                                                    stats_ranking_function=Stats.premier_league_ranking_fn)

        expected_stats = [[1, 'Chelsea', 17, 11, 3, 3, 26, 16, 10, 36],
                          [2, 'Manchester City', 17, 11, 1, 5, 36, 20, 16, 34],
                          [3, 'Manchester United', 16, 10, 4, 2, 27, 13, 14, 34],
                          [4, 'Liverpool', 17, 8, 5, 4, 28, 24, 4, 29],
                          [5, 'Tottenham Hotspur', 16, 7, 6, 3, 26, 14, 12, 27],
                          [6, 'Arsenal', 16, 7, 3, 6, 32, 25, 7, 24],
                          [7, 'Crystal Palace', 17, 6, 3, 8, 26, 31, -5, 21],
                          [8, 'Southampton', 17, 5, 3, 9, 22, 26, -4, 18],
                          [9, 'Everton', 17, 4, 6, 7, 19, 24, -5, 18],
                          [10, 'West Ham United', 17, 4, 4, 9, 26, 32, -6, 16],
                          [11, 'West Bromwich Albion', 16, 3, 6, 7, 12, 22, -10, 15],
                          [12, 'Stoke City', 17, 3, 4, 10, 14, 30, -16, 13],
                          [13, 'Watford', 16, 3, 3, 10, 12, 31, -19, 12],
                          [14, 'Bournemouth', 17, 2, 5, 10, 18, 37, -19, 11],
                          [15, 'Middlesbrough', 17, 1, 7, 9, 10, 24, -14, 10],
                          [16, 'Swansea City', 17, 3, 1, 13, 15, 35, -20, 10],
                          [17, 'Sunderland', 16, 2, 1, 13, 10, 28, -18, 7],
                          [18, 'Leicester City', 17, 1, 4, 12, 15, 36, -21, 7],
                          [19, 'Hull City', 17, 1, 2, 14, 9, 41, -32, 5],
                          [20, 'Burnley', 17, 0, 4, 13, 10, 33, -23, 4]]

        stats_list = [[row[0], *list(row[1])] for row in list(league)]
        self.assertEqual(expected_stats, stats_list)

    def test_league_last_2wks(self):
        league = create_league_using_windowed_stats(cursor=db_cursor, teams=TEAMS, win_size=2,
                                                    win_end_date=date(2017, 4, 28),
                                                    stats_ranking_function=Stats.premier_league_ranking_fn)

        expected_stats = [[1, 'Manchester United', 3, 2, 1, 0, 4, 0, 4, 7],
                          [2, 'Tottenham Hotspur', 2, 2, 0, 0, 5, 0, 5, 6],
                          [3, 'Arsenal', 2, 2, 0, 0, 3, 1, 2, 6],
                          [4, 'Manchester City', 2, 1, 1, 0, 3, 0, 3, 4],
                          [5, 'Everton', 2, 1, 1, 0, 3, 1, 2, 4],
                          [6, 'Crystal Palace', 3, 1, 1, 1, 4, 4, 0, 4],
                          [7, 'Swansea City', 2, 1, 0, 1, 2, 1, 1, 3],
                          [8, 'Bournemouth', 2, 1, 0, 1, 4, 4, 0, 3],
                          [8, 'Chelsea', 2, 1, 0, 1, 4, 4, 0, 3],
                          [8, 'Hull City', 2, 1, 0, 1, 3, 3, 0, 3],
                          [8, 'Liverpool', 2, 1, 0, 1, 2, 2, 0, 3],
                          [8, 'Stoke City', 2, 1, 0, 1, 3, 3, 0, 3],
                          [13, 'Watford', 2, 1, 0, 1, 1, 2, -1, 3],
                          [14, 'Middlesbrough', 3, 1, 0, 2, 2, 6, -4, 3],
                          [15, 'West Ham United', 2, 0, 2, 0, 2, 2, 0, 2],
                          [16, 'Leicester City', 2, 0, 1, 1, 2, 3, -1, 1],
                          [16, 'Sunderland', 2, 0, 1, 1, 2, 3, -1, 1],
                          [18, 'West Bromwich Albion', 1, 0, 0, 1, 0, 1, -1, 0],
                          [19, 'Burnley', 2, 0, 0, 2, 1, 5, -4, 0],
                          [20, 'Southampton', 2, 0, 0, 2, 2, 7, -5, 0]]

        stats_list = [[row[0], *list(row[1])] for row in list(league)]
        self.assertEqual(expected_stats, stats_list)

    def test_league_last_2wks_normalised(self):

        league = create_league_using_windowed_stats(cursor=db_cursor, teams=TEAMS, win_size=2,
                                                    win_end_date=date(2017, 4, 28), normalize_by_matches=True,
                                                    stats_ranking_function=Stats.premier_league_ranking_fn)
        expected_stats = [[1, 'Tottenham Hotspur', 2, 2, 0, 0, 5, 0, 2.5, 3.0],
                          [2, 'Arsenal', 2, 2, 0, 0, 3, 1, 1.0, 3.0],
                          [3, 'Manchester United', 3, 2, 1, 0, 4, 0, 1.3333333333333333, 2.3333333333333335],
                          [4, 'Manchester City', 2, 1, 1, 0, 3, 0, 1.5, 2.0],
                          [5, 'Everton', 2, 1, 1, 0, 3, 1, 1.0, 2.0],
                          [6, 'Swansea City', 2, 1, 0, 1, 2, 1, 0.5, 1.5],
                          [7, 'Bournemouth', 2, 1, 0, 1, 4, 4, 0.0, 1.5],
                          [7, 'Chelsea', 2, 1, 0, 1, 4, 4, 0.0, 1.5], [7, 'Hull City', 2, 1, 0, 1, 3, 3, 0.0, 1.5],
                          [7, 'Liverpool', 2, 1, 0, 1, 2, 2, 0.0, 1.5],
                          [7, 'Stoke City', 2, 1, 0, 1, 3, 3, 0.0, 1.5],
                          [12, 'Watford', 2, 1, 0, 1, 1, 2, -0.5, 1.5],
                          [13, 'Crystal Palace', 3, 1, 1, 1, 4, 4, 0.0, 1.3333333333333333],
                          [14, 'West Ham United', 2, 0, 2, 0, 2, 2, 0.0, 1.0],
                          [15, 'Middlesbrough', 3, 1, 0, 2, 2, 6, -1.3333333333333333, 1.0],
                          [16, 'Leicester City', 2, 0, 1, 1, 2, 3, -0.5, 0.5],
                          [16, 'Sunderland', 2, 0, 1, 1, 2, 3, -0.5, 0.5],
                          [18, 'West Bromwich Albion', 1, 0, 0, 1, 0, 1, -1.0, 0.0],
                          [19, 'Burnley', 2, 0, 0, 2, 1, 5, -2.0, 0.0],
                          [20, 'Southampton', 2, 0, 0, 2, 2, 7, -2.5, 0.0]]

        stats_list = [[row[0], *list(row[1])] for row in list(league)]
        self.assertEqual(expected_stats, stats_list)

    def test_league_position_last_2wks(self):
        league = create_league_using_windowed_stats(cursor=db_cursor, teams=TEAMS, win_size=2,
                                                    win_end_date=date(2017, 4, 28),
                                                    stats_ranking_function=Stats.premier_league_ranking_fn)

        expected_stats = [[1, 'Manchester United', 3, 2, 1, 0, 4, 0, 4, 7],
                          [2, 'Tottenham Hotspur', 2, 2, 0, 0, 5, 0, 5, 6],
                          [3, 'Arsenal', 2, 2, 0, 0, 3, 1, 2, 6],
                          [4, 'Manchester City', 2, 1, 1, 0, 3, 0, 3, 4],
                          [5, 'Everton', 2, 1, 1, 0, 3, 1, 2, 4],
                          [6, 'Crystal Palace', 3, 1, 1, 1, 4, 4, 0, 4],
                          [7, 'Swansea City', 2, 1, 0, 1, 2, 1, 1, 3],
                          [8, 'Bournemouth', 2, 1, 0, 1, 4, 4, 0, 3],
                          [8, 'Chelsea', 2, 1, 0, 1, 4, 4, 0, 3],
                          [8, 'Hull City', 2, 1, 0, 1, 3, 3, 0, 3],
                          [8, 'Liverpool', 2, 1, 0, 1, 2, 2, 0, 3],
                          [8, 'Stoke City', 2, 1, 0, 1, 3, 3, 0, 3],
                          [13, 'Watford', 2, 1, 0, 1, 1, 2, -1, 3],
                          [14, 'Middlesbrough', 3, 1, 0, 2, 2, 6, -4, 3],
                          [15, 'West Ham United', 2, 0, 2, 0, 2, 2, 0, 2],
                          [16, 'Leicester City', 2, 0, 1, 1, 2, 3, -1, 1],
                          [16, 'Sunderland', 2, 0, 1, 1, 2, 3, -1, 1],
                          [18, 'West Bromwich Albion', 1, 0, 0, 1, 0, 1, -1, 0],
                          [19, 'Burnley', 2, 0, 0, 2, 1, 5, -4, 0],
                          [20, 'Southampton', 2, 0, 0, 2, 2, 7, -5, 0]]

        stats_list = [[row[0], *list(row[1])] for row in list(league)]
        self.assertEqual(expected_stats, stats_list)

        team = 'Everton'
        pos_expected = 5
        pos_found = league.id2ranking[team]
        self.assertEqual(pos_expected, pos_found)

        team = 'Sunderland'
        pos_expected = 16
        pos_found = league.id2ranking[team]
        self.assertEqual(pos_expected, pos_found)


if __name__ == '__main__':
    unittest.main()