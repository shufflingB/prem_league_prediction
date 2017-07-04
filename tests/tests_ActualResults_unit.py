import unittest

import sqlite3
from datetime import date, datetime, timedelta

from ActualResultsLib import ActualResults

RESULTS_FIXTURE_DATA = './fixture/results_2017_04_28.db'

TEAMS = ['Arsenal', 'Bournemouth', 'Burnley', 'Chelsea', 'Crystal Palace', 'Everton', 'Hull City',
         'Leicester City', 'Liverpool', 'Manchester City', 'Manchester United', 'Middlesbrough',
         'Southampton', 'Stoke City', 'Sunderland', 'Swansea City', 'Tottenham Hotspur', 'Watford',
         'West Bromwich Albion', 'West Ham United']

MATCH_DATES = ['2016-08-13', '2016-08-14', '2016-08-15', '2016-08-19', '2016-08-20', '2016-08-21', '2016-08-27',
               '2016-08-28', '2016-09-10', '2016-09-11', '2016-09-12', '2016-09-16', '2016-09-17', '2016-09-18',
               '2016-09-24', '2016-09-25', '2016-09-26', '2016-09-30', '2016-10-01', '2016-10-02', '2016-10-15',
               '2016-10-16', '2016-10-17', '2016-10-22', '2016-10-23', '2016-10-29', '2016-10-30', '2016-10-31',
               '2016-11-05', '2016-11-06', '2016-11-19', '2016-11-20', '2016-11-21', '2016-11-26', '2016-11-27',
               '2016-12-03', '2016-12-04', '2016-12-05', '2016-12-10', '2016-12-11', '2016-12-13', '2016-12-14',
               '2016-12-17', '2016-12-18', '2016-12-19', '2016-12-26', '2016-12-27', '2016-12-28', '2016-12-30',
               '2016-12-31', '2017-01-01', '2017-01-02', '2017-01-03', '2017-01-04', '2017-01-14', '2017-01-15',
               '2017-01-21', '2017-01-22', '2017-01-31', '2017-02-01', '2017-02-04', '2017-02-05', '2017-02-11',
               '2017-02-12', '2017-02-13', '2017-02-25', '2017-02-26', '2017-02-27', '2017-03-04', '2017-03-05',
               '2017-03-06', '2017-03-08', '2017-03-11', '2017-03-12', '2017-03-18', '2017-03-19', '2017-04-01',
               '2017-04-02', '2017-04-04', '2017-04-05', '2017-04-08', '2017-04-09', '2017-04-10', '2017-04-15',
               '2017-04-16', '2017-04-17', '2017-04-22', '2017-04-23', '2017-04-25', '2017-04-26', '2017-04-27']

FIRST_MATCHDAY = date.fromtimestamp(datetime.strptime(MATCH_DATES[0], '%Y-%m-%d').timestamp())

class ResultsFileTests(unittest.TestCase):
    def setUp(self):
        global db_connection
        db_connection = sqlite3.connect(RESULTS_FIXTURE_DATA)
        db_connection.row_factory = sqlite3.Row
        global db_cursor
        db_cursor = db_connection.cursor()

    def tearDown(self):
        db_connection.close()


    e_stats_arse_wks40_all = ['Arsenal', 32, 18, 6, 8, 64, 40, 24, 60]

    def test_get_teams(self):
        teams = ActualResults.get_teams(db_cursor)
        e_val = TEAMS

        self.assertEqual(e_val, teams)

    def test_get_matchdates(self):
        # Test we can extract match date objects from the results file
        dates: [date] = ActualResults.get_dates(db_cursor)

        assert isinstance(dates[0], date), dates

        dates_list = list(map(lambda x: x.isoformat(), dates))

        e_val = MATCH_DATES
        self.assertEqual(e_val, dates_list)

    def test_get_match_data(self):
        # Test we can extract a list of hashes containing the match data
        match_data = ActualResults.get_results_data(db_cursor, win_end=date(2016, 8, 13))

        # print(match_data)
        data_list = list(map(lambda x: list(x), match_data))
        e_data = [['2016-08-13', 'Burnley', 0, 'Swansea City', 1],
                  ['2016-08-13', 'Crystal Palace', 0, 'West Bromwich Albion', 1],
                  ['2016-08-13', 'Everton', 1, 'Tottenham Hotspur', 1],
                  ['2016-08-13', 'Hull City', 2, 'Leicester City', 1],
                  ['2016-08-13', 'Manchester City', 2, 'Sunderland', 1],
                  ['2016-08-13', 'Middlesbrough', 1, 'Stoke City', 1],
                  ['2016-08-13', 'Southampton', 1, 'Watford', 1]]

        self.assertEqual(e_data, data_list)

    def test_get_match_data_single_day_windowed(self):
        # Test we can extract a list of hashes containing the match data
        match_data = ActualResults.get_results_data(db_cursor, win_end=date(2016, 8, 14),
                                                    win_size=timedelta(days=0))

        # print(match_data)
        data_list = list(map(lambda x: list(x), match_data))
        e_data = [['2016-08-14', 'Arsenal', 3, 'Liverpool', 4],
                  ['2016-08-14', 'Bournemouth', 1, 'Manchester United', 3]]

        self.assertEqual(e_data, data_list)


    def test_get_match_data_week_windowed(self):
        # Test we can extract a list of hashes containing the match data
        match_data = ActualResults.get_results_data(db_cursor, win_end=date(2016, 8, 20),
                                                    win_size=timedelta(days=6))

        # print(match_data)
        data_list = list(map(lambda x: list(x), match_data))
        e_data = [['2016-08-14', 'Arsenal', 3, 'Liverpool', 4],
                  ['2016-08-14', 'Bournemouth', 1, 'Manchester United', 3],
                  ['2016-08-15', 'Chelsea', 2, 'West Ham United', 1],
                  ['2016-08-19', 'Manchester United', 2, 'Southampton', 0],
                  ['2016-08-20', 'Burnley', 2, 'Liverpool', 0],
                  ['2016-08-20', 'Leicester City', 0, 'Arsenal', 0],
                  ['2016-08-20', 'Stoke City', 1, 'Manchester City', 4],
                  ['2016-08-20', 'Swansea City', 0, 'Hull City', 2],
                  ['2016-08-20', 'Tottenham Hotspur', 1, 'Crystal Palace', 0],
                  ['2016-08-20', 'Watford', 1, 'Chelsea', 2],
                  ['2016-08-20', 'West Bromwich Albion', 1, 'Everton', 2]]

        self.assertEqual(e_data, data_list)

    def test_instantiation(self):
        no_data = ActualResults()
        assert no_data.dates == None

        foo = ActualResults(db_cursor)
        dates_list = list(map(lambda x: x.isoformat(), foo.dates))

        e_data = MATCH_DATES
        self.assertEqual(e_data, dates_list)

    def test_instantiation_windowed(self):
        foo = ActualResults(db_cursor, win_end=date(2016, 8, 20), win_size=timedelta(days=6))
        # print(foo.results_data)

        data_list = list(map(lambda x: list(x), foo.results_data))
        # print(data_list)

        e_data = [['2016-08-14', 'Arsenal', 3, 'Liverpool', 4],
                  ['2016-08-14', 'Bournemouth', 1, 'Manchester United', 3],
                  ['2016-08-15', 'Chelsea', 2, 'West Ham United', 1],
                  ['2016-08-19', 'Manchester United', 2, 'Southampton', 0],
                  ['2016-08-20', 'Burnley', 2, 'Liverpool', 0],
                  ['2016-08-20', 'Leicester City', 0, 'Arsenal', 0],
                  ['2016-08-20', 'Stoke City', 1, 'Manchester City', 4],
                  ['2016-08-20', 'Swansea City', 0, 'Hull City', 2],
                  ['2016-08-20', 'Tottenham Hotspur', 1, 'Crystal Palace', 0],
                  ['2016-08-20', 'Watford', 1, 'Chelsea', 2],
                  ['2016-08-20', 'West Bromwich Albion', 1, 'Everton', 2]]
        self.assertEqual(e_data, data_list)




