import unittest

from league_maker import Stats, League, ResultsFile
import sqlite3
from datetime import date, datetime, timedelta

RESULTS_FIXTURE_DATE = './fixture/results_2017_04_28.db'

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



with sqlite3.connect(RESULTS_FIXTURE_DATE) as db_in_connection:
    db_in_connection.row_factory = sqlite3.Row


    class ResultsFileTests(unittest.TestCase):

        e_stats_arse_wks40_all = ['Arsenal', 32, 18, 6, 8, 64, 40, 24, 60]

        # def test_foo(self):
        #     sql = 'SELECT * FROM results'
        #     db_in_connection.execute(sql).fetchall()
        #     print(db_in_connection)

        def test_get_teams(self):
            teams = ResultsFile.get_teams(db_in_connection)

            assert teams == TEAMS, 'Actually got %s' % teams

        def test_get_matchdates(self):
            # Test we can extract match date objects from the results file
            dates:[date] = ResultsFile.get_dates(db_in_connection)

            assert isinstance(dates[0], date), dates

            dates_list = list(map(lambda x: x.isoformat(), dates))
            assert dates_list == MATCH_DATES, 'Actually got %s' % dates_list

        def test_get_match_data(self):
            # Test we can extract a list of hashes containing the match data
            match_data = ResultsFile.get_results_data(db_in_connection, win_end=date(2016, 8, 13))

            # print(match_data)
            data_list = list(map(lambda x: list(x), match_data))
            e_data = [['2016-08-13', 'Burnley', 0, 'Swansea City', 1],
                      ['2016-08-13', 'Crystal Palace', 0, 'West Bromwich Albion', 1],
                      ['2016-08-13', 'Everton', 1, 'Tottenham Hotspur', 1],
                      ['2016-08-13', 'Hull City', 2, 'Leicester City', 1],
                      ['2016-08-13', 'Manchester City', 2, 'Sunderland', 1],
                      ['2016-08-13', 'Middlesbrough', 1, 'Stoke City', 1],
                      ['2016-08-13', 'Southampton', 1, 'Watford', 1]]

            assert data_list == e_data, 'Actually got %s' % data_list

        def test_get_match_data_single_day_windowed(self):
            # Test we can extract a list of hashes containing the match data
            match_data = ResultsFile.get_results_data(db_in_connection, win_end=date(2016, 8, 14),
                                                      win_size=timedelta(days=0))

            # print(match_data)
            data_list = list(map(lambda x: list(x), match_data))
            e_data = [['2016-08-14', 'Arsenal', 3, 'Liverpool', 4],
                      ['2016-08-14', 'Bournemouth', 1, 'Manchester United', 3]]

            assert data_list == e_data, 'Actually got %s' % data_list

        def test_get_match_data_week_windowed(self):
            # Test we can extract a list of hashes containing the match data
            match_data = ResultsFile.get_results_data(db_in_connection, win_end=date(2016, 8, 20),
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

            assert data_list == e_data, 'Actually got %s' % data_list




        def test_instantiation(self):

            no_data = ResultsFile()
            assert no_data.dates == None

            foo = ResultsFile(db_in_connection)
            dates_list = list(map(lambda x: x.isoformat(), foo.dates))
            assert dates_list == MATCH_DATES, 'Actually got %s' % dates_list



        def test_instantiation_windowed(self ):
            foo = ResultsFile(db_in_connection, win_end=date(2016, 8, 20), win_size=timedelta(days=6))
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
            assert data_list == e_data, 'Actually got %s' % data_list


    class StatsTests(unittest.TestCase):

        def test_stats_iterable(self):
            team = 'Southampton'
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=2,
                                                  win_end_date=date(2017, 4, 28))
            list(stats)

        def test_stats_no_results(self):
            # Check okay with no data for the team
            team = 'Arsenal'  # Didn't play until date(2016, 8, 14)
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=40,
                                                  win_end_date=FIRST_MATCHDAY)
            assert [team, 0, 0, 0, 0, 0, 0, 0, 0] == list(stats), 'Actually got %s' % list(stats)

        def test_stats_single_results(self):
            # Check okay with no data for the team
            team = 'Arsenal'  # Didn't play until date(2016, 8, 14)
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=40,
                                                  win_end_date=date(2016, 8, 14))
            assert [team, 1, 0, 0, 1, 3, 4, -1, 0] == list(stats), 'Actually got %s' % list(stats)

        def test_stats_two_results(self):
            team = 'Tottenham Hotspur'
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=2,
                                                  win_end_date=date(2017, 4, 28))
            assert [team, 2, 2, 0, 0, 5, 0, 5, 6] == list(stats), 'Actually got %s' % list(stats)

        def test_stats_home_only(self):
            team = 'Tottenham Hotspur'
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=2,
                                                  win_end_date=date(2017, 4, 28), home_only=True)
            assert [team, 1, 1, 0, 0, 4, 0, 4, 3] == list(stats), 'Actually got %s' % list(stats)

        def test_stats_away_only(self):
            team = 'Tottenham Hotspur'
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=2,
                                                  win_end_date=date(2017, 4, 28), home_only=False)
            assert [team, 1, 1, 0, 0, 1, 0, 1, 3] == list(stats), 'Actually got %s' % list(stats)

        def test_stats_wks40_all(self):
            # Tests all for the season
            team = 'Arsenal'
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=40,
                                                  win_end_date=date(2017, 4, 28))
            assert list(stats) == ['Arsenal', 32, 18, 6, 8, 64, 40, 24, 60], 'Actually got %s' % list(stats)

        def test_stats_normalised_by_num_matches(self):
            # Tests all for the season
            team = 'Arsenal'
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=40,
                                                  win_end_date=date(2017, 4, 28), normalize_by_matches = True)
            assert list(stats) == ['Arsenal', 32, 18, 6, 8, 64, 40, 24/32, 60/32], 'Actually got %s' % list(stats)




        def test_stats_window(self):
            team = 'Arsenal'  # Played on the 20th and the 27th, so should be only one result
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=1,
                                                  win_end_date=date(2016, 8, 27))

            assert list(stats) == ['Arsenal', 1, 1, 0, 0, 3, 1, 2, 3], 'Actually got %s' % list(stats)

            team = 'Sunderland'  # Sunderland played ont the 21st and 27th so two results
            stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=1,
                                                  win_end_date=date(2016, 8, 27))

            assert list(stats) == ['Sunderland', 2, 0, 1, 1, 2, 3, -1, 1], 'Actually got %s' % list(stats)

    class LeagueTests(unittest.TestCase):

        def test_league_wks40_all(self):
            league = League(stats=[])
            for team in TEAMS:
                stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=40,
                                                      win_end_date=date(2017, 4, 28))
                league.add_stat(new_stats=stats)

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
            assert list(stats_list) == expected_stats, 'Actually got %s' % list(league)

        def test_league_wks40_home_only(self):
            league = League(stats=[])
            for team in TEAMS:
                stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=40,
                                                      win_end_date=date(2017, 4, 28), home_only=True)
                league.add_stat(new_stats=stats)

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
            assert list(stats_list) == expected_stats, 'Actually got %s' % list(league)


        def test_league_wks40_away_only(self):
            league = League(stats=[])
            for team in TEAMS:
                stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=40,
                                                      win_end_date=date(2017, 4, 28), home_only=False)
                league.add_stat(new_stats=stats)

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
            assert list(stats_list) == expected_stats, 'Actually got %s' % list(league)

        def test_league_last_2wks(self):
            league = League(stats=[])
            for team in TEAMS:
                stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=2,
                                                      win_end_date=date(2017, 4, 28))
                league.add_stat(new_stats=stats)

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

            # stats_list = list(map(lambda x: , league.stats))
            # print(list(league))

            stats_list = [[row[0], *list(row[1])]  for row in list(league)]
            # print(stats_list)
            assert list(stats_list) == expected_stats, 'Actually got %s' % list(league)

        def test_init_from_db(self):
            league = League.init_from_db(cursor=db_in_connection, teams=TEAMS, win_size=2, win_end_date=date(2017, 4, 28))

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

            # stats_list = list(map(lambda x: , league.stats))
            # print(list(league))

            stats_list = [[row[0], *list(row[1])]  for row in list(league)]
            # print(stats_list)
            assert list(stats_list) == expected_stats, 'Actually got %s' % list(league)




        def test_find_stats_idx(self):
            league = League(stats=[])
            for team in TEAMS:
                stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=2,
                                                      win_end_date=date(2017, 4, 28))
                league.add_stat(new_stats=stats)

            team = 'Everton'
            idx_expected = 4
            idx_found = league.table_idx[team]
            assert idx_found == idx_expected, 'For %s, expected %s, got %s' % (team, idx_expected, idx_found)

            team = 'Sunderland'
            idx_expected = 16
            idx_found = league.table_idx[team]
            assert idx_found == idx_expected, 'For %s, expected %s, got %s' % (team, idx_expected, idx_found)



        def test_league_find_position(self):
            league = League(stats=[])
            for team in TEAMS:
                stats = Stats.windowed_stats_for_date(cursor=db_in_connection, team=team, win_weeks=2,
                                                      win_end_date=date(2017, 4, 28))
                league.add_stat(new_stats=stats)

            team = 'Everton'
            pos_expected = 5
            pos_found = league.table_position[team]
            assert pos_found == pos_expected, 'For %s, expected %s, got %s' % (team, pos_expected, pos_found)

            team = 'Sunderland'
            pos_expected = 16
            pos_found = league.table_position[team]
            assert pos_found == pos_expected, 'For %s, expected %s, got %s' % (team, pos_expected, pos_found)



