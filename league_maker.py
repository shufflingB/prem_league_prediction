#!/usr/bin/env python

import sqlite3
from datetime import date
from datetime import timedelta
from datetime import datetime
from argparse import ArgumentParser
import logging
import os

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class Stats(object):
    @staticmethod
    def windowed_stats_for_date(cursor: sqlite3.Cursor, team: str, win_weeks: int, win_end_date: date,
                                home_only: bool = None, normalize_by_matches: bool = False):

        # If window is 1 week long, then don't expect results to include two Saturdays worth of data. i.e.
        # expect results to be start < window <= end.
        win_start_date = win_end_date - timedelta(weeks=win_weeks, days=-1)

        start_str = win_start_date.isoformat()
        stop_str = win_end_date.isoformat()

        # These SQL statements could be written as one, but they're easier to understand if they're kept on their own.
        sql_home = {}
        sql_home[True] = 'SELECT ' \
                         'COUNT(date) AS points, ' \
                         'SUM(home_score > away_score) AS wins, ' \
                         'SUM(home_score == away_score) as draws, ' \
                         'SUM(home_score < away_score) AS losses, ' \
                         'SUM(home_score) AS for, ' \
                         'SUM(away_score) AS against ' \
                         'FROM results WHERE home_team="%s" AND date BETWEEN "%s" AND "%s"' % (
                             team, start_str, stop_str)
        # logging.debug(sql_home)

        sql_home[False] = 'SELECT ' \
                          'COUNT(date) AS points, ' \
                          'SUM(away_score > home_score) AS wins, ' \
                          'SUM(away_score == home_score) as draws, ' \
                          'SUM(away_score < home_score) AS losses, ' \
                          'SUM(away_score) AS for, ' \
                          'SUM(home_score) AS against ' \
                          'FROM results WHERE away_team="%s" AND date BETWEEN "%s" AND "%s"' % (
                              team, start_str, stop_str)

        # logging.debug(sql_away)

        if home_only is None:
            home_stats = Stats(team, *cursor.execute(sql_home[True]).fetchone(),
                               primary_fn=Stats.calc_points, secondary_fn=Stats.calc_score_diff,
                               normalize_by_matches=normalize_by_matches)
            away_stats = Stats(team, *cursor.execute(sql_home[False]).fetchone(),
                               primary_fn=Stats.calc_points, secondary_fn=Stats.calc_score_diff,
                               normalize_by_matches=normalize_by_matches)

            stats = home_stats + away_stats
        else:
            stats = Stats(team, *cursor.execute(sql_home[home_only]).fetchone(),
                          primary_fn=Stats.calc_points, secondary_fn=Stats.calc_score_diff,
                          normalize_by_matches=normalize_by_matches)

        return stats

    @staticmethod
    def calc_points(wins: int, draws: int) -> int:

        return 3 * wins + 1 * draws

    @staticmethod
    def calc_score_diff(score_for: int, score_against: int) -> int:
        return score_for - score_against

    @staticmethod
    def default(value, look_for=None, replace_with=0):
        if value == look_for:
            return replace_with
        return value

    def __init__(self, team_name: str, played: int, won: int, drawn: int, lost: int, score_for: int, score_against: int,
                 primary_fn=calc_points, secondary_fn=calc_score_diff, normalize_by_matches: bool = False):
        self.normalize_points_by_num_matches = normalize_by_matches
        self.team_name = team_name
        self.played = played
        self.won = Stats.default(won)
        self.drawn = Stats.default(drawn)
        self.lost = Stats.default(lost)
        self.score_for = Stats.default(score_for)
        self.score_against = Stats.default(score_against)
        self.secondary_fn = secondary_fn
        self.primary_fn = primary_fn
        self.calc_features()

    def __eq__(self, other) -> int:

        if self.primary_feature == other.primary_feature and self.secondary_feature == other.secondary_feature:
            return True
        else:
            return False

    def __lt__(self, other):
        if self.primary_feature < other.primary_feature or \
                (self.primary_feature == other.primary_feature and self.secondary_feature < other.secondary_feature):
            return True
        else:
            return False

    def __gt__(self, other):
        if self.primary_feature > other.primary_feature or \
                (self.primary_feature == other.primary_feature and self.secondary_feature > other.secondary_feature):
            return True
        else:
            return False

    def __add__(self, other):
        import copy
        assert hasattr(other, 'team_name'), 'No team_name attribute'
        assert self.team_name == other.team_name, 'Team names do not match,  %s and %s' % \
                                                  (self.team_name, other.team_name)
        copy = copy.deepcopy(self)
        copy.played += other.played
        copy.won += Stats.default(other.won)
        copy.drawn += Stats.default(other.drawn)
        copy.lost += Stats.default(other.lost)
        copy.score_for += Stats.default(other.score_for)
        copy.score_against += Stats.default(other.score_against)
        copy.calc_features()
        return copy

    def calc_features(self, normalize_by_matches: bool = False):
        # print('primary_fn =', self.primary_fn)
        if self.played > 0 and self.normalize_points_by_num_matches:

            denominator = self.played
        else:
            denominator = 1

        self.primary_feature = self.primary_fn(self.won, self.drawn) / denominator
        self.secondary_feature = self.secondary_fn(self.score_for, self.score_against) / denominator

    def __str__(self) -> str:
        return '%s' % list(self)

    def __iter__(self):
        for i in (self.team_name, self.played, self.won, self.drawn, self.lost, self.score_for, self.score_against,
                  self.secondary_feature, self.primary_feature):
            yield i


class League(object):
    def __init__(self, stats: [Stats]):
        self.stats = []  # stats is [[Stats, Stats, ...], [Stats, ...]
        self.table = []  # table is [[pos, stats] i.e. flattened stats, 1 team per row
        self.add_stats(new_stats=stats)
        self.table_idx = {}  # Dictionary lookup of team name: str to stats object index
        self.table_position = {}  #  Dictionary lookup of team name:str to league position:int

    def __str__(self, quiet: bool = True) -> str:
        out_str = ''
        if not quiet:
            out_str = 'Position, Team Name, Played, Won, Drawn, Lost, Goals For, Goals Against, Goal Diff, Point\n'

        for row in [[row[0], *list(row[1])] for row in list(self)]:
            out_str += '%s\n' % row
        return out_str

    def __iter__(self):
        place = 1
        increment = 0
        for position in self.stats:
            place += increment
            increment = 0
            for team_stat in position:
                # yield [place, *list(team_stat)]
                yield [place, team_stat]
                increment += 1

    def add_stats(self, new_stats: [Stats]):
        for stats_row in new_stats:
            self.add_stat(stats_row)

    def add_stat(self, new_stats: Stats):

        if len(self.stats) == 0:
            # print('Initialising')
            self.stats.append([new_stats])
        else:
            inserted = False
            for position in range(0, len(self.stats)):
                row_stats: Stats = self.stats[position][0]
                # print('position ', position, ' row_stats = ', row_stats, ' new_stats = ', new_stats)
                if new_stats == row_stats:
                    # print('Adding to current position')
                    self.stats[position].append(new_stats)
                    inserted = True
                    break
                elif new_stats > row_stats:
                    # print('Inserting at current position, shunting everything else down')
                    self.stats.insert(position, [new_stats])
                    inserted = True
                    break

            if inserted is False:
                # print('Adding to the end')
                self.stats.append([new_stats])

        self.update()

    def update(self):
        self.table = list(self)
        self.table_idx = {self.table[idx][1].team_name: idx for idx in range(0, len(self.table))}
        self.table_position = {self.table[idx][1].team_name: self.table[idx][0] for idx in range(0, len(self.table))}

    @staticmethod
    def init_from_db(cursor: sqlite3.Cursor, teams: [str], win_size: int, win_end_date: date, home_only: bool = None,
                     normalize_by_matches: bool = False):
        new_league = League([])

        for team in teams:
            stats = Stats.windowed_stats_for_date(cursor=cursor, team=team, win_weeks=win_size,
                                                  win_end_date=win_end_date, home_only=home_only,
                                                  normalize_by_matches=normalize_by_matches)
            new_league.add_stat(new_stats=stats)

        return new_league


class ResultsFile:
    def __init__(self, cursor: sqlite3.Cursor = None, win_size: timedelta = None, win_end: date = '*'):
        self.cursor = cursor
        self.win_size = win_size
        self.win_end = win_end
        self.teams = None
        self.dates = None
        self.results_data = None
        if self.cursor is not None:
            self.read_data(self.cursor)

    def read_data(self, cursor: sqlite3.Cursor):
        self.cursor = cursor
        self.teams = ResultsFile.get_teams(self.cursor)
        self.dates = ResultsFile.get_dates(self.cursor)
        self.results_data = ResultsFile.get_results_data(self.cursor, win_size=self.win_size, win_end=self.win_end)

    @staticmethod
    def get_teams(cursor: sqlite3.Cursor) -> [str]:
        sql = 'SELECT DISTINCT home_team FROM results ORDER BY home_team ASC'
        return list(map(lambda x: x[0], cursor.execute(sql).fetchall()))
        # fetchall gives an array, just need 1st element

    @staticmethod
    def get_dates(cursor: sqlite3.Cursor) -> [date]:
        sql = 'SELECT DISTINCT date FROM results ORDER BY date ASC'
        return list(map(lambda x: date.fromtimestamp(datetime.strptime(x[0], '%Y-%m-%d').timestamp()),
                        cursor.execute(sql).fetchall()))

    @staticmethod
    def get_results_data(cursor: sqlite3.Cursor, win_size: timedelta = None, win_end: date = '*') -> [()]:
        if isinstance(win_end, date):
            if isinstance(win_size, timedelta):
                win_start = win_end - win_size
                sql = 'SELECT date, home_team, home_score, away_team, away_score FROM results WHERE' \
                      ' date BETWEEN "%s" AND "%s" ' \
                      'ORDER BY date ASC, home_team ASC' % (win_start.isoformat(), win_end.isoformat())
            else:
                sql = 'SELECT date, home_team, home_score, away_team, away_score FROM results WHERE' \
                      ' date="%s" ' \
                      'ORDER BY date ASC, home_team ASC' % (win_end.isoformat())

                # print(sql)
        else:
            sql = 'SELECT date, home_team, home_score, away_team, away_score FROM results ' \
                  'ORDER BY date ASC, home_team ASC'

        return cursor.execute(sql).fetchall()


if __name__ == "__main__":

    parser: ArgumentParser = ArgumentParser()
    parser.description = "Calculates 'league' positions for a team"
    parser.add_argument('-f', '--sqlite-file',
                        help='Sqlite file containing raw results. Expected schema is (id, date, home_team, home_score, '
                             'away_team, away_score)',
                        required=True,
                        type=str
                        )
    parser.add_argument('-e', '--win-end-date',
                        default=date.today().isoformat(),
                        help='Specify end-date for the results window used to calculate the league positions. Expected '
                             'format is YYYY-MM-DD and the default is today.',
                        required=False,
                        type=str
                        )
    parser.add_argument('-w', '--win-size',
                        default=40,
                        help='The number of weeks to calculate the \'league\' back from the specified end-date. Default'
                             ' is 40',
                        required=False,
                        type=int
                        )

    parser.add_argument('-r', '--repeat-to',
                        default=False,
                        help='Re-calculate the \'league\' at weekly increments, using the same window size, until this '
                             'date. Set it to 0 to do until today, else YYYY-MM-DD. Default is not to repeat',
                        type=str,
                        required=False
                        )

    parser.add_argument('-m', '--only-home',
                        default=False,
                        help='Include only results for home matches in the calculations, default is not',
                        required=False,
                        action='store_true'
                        )

    parser.add_argument('-a', '--only-away',
                        default=False,
                        help='Include only results for away matches in the calculations, default is not',
                        required=False,
                        action='store_true'
                        )

    args = parser.parse_args()

    sqlite_file = args.sqlite_file

    win_size = args.win_size

    # TODO: Figure out how to do this conversion better (there must be something nicer)
    win_end_date = date.fromtimestamp(datetime.strptime(args.win_end_date, '%Y-%m-%d').timestamp())

    repeat_until_date = None
    if args.repeat_to is False:
        print('Detected should not repeat')
        repeat_until_date = win_end_date
        print('repeat_until = ', repeat_until_date)
    elif args.repeat_to == 0:
        repeat_until_date = date.today()
    elif args.repeat_to:
        repeat_until_date = date.fromtimestamp(datetime.strptime(args.win_end_date, '%Y-%m-%d').timestamp())

    home_only = None
    if args.only_home:
        home_only = True
    elif args.only_away:
        home_only = False

    with sqlite3.connect(sqlite_file) as db_in_connection:
        db_in_cursor = db_in_connection.cursor()
        teams = ResultsFile.get_teams(cursor=db_in_cursor)
        while win_end_date <= repeat_until_date:
            league = League.init_from_db(cursor=db_in_connection, teams=teams, win_size=win_size,
                                         win_end_date=win_end_date, home_only=home_only)
            print('======= League on date %s =====' % win_end_date)
            print(league.__str__(quiet=False))

            win_end_date += timedelta(weeks=1)
