import sqlite3
from datetime import date, timedelta, datetime
import typing

import logging

from FeatureLib import FeatureModelRanking

# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.INFO)


def create_league_using_windowed_stats(cursor: sqlite3.Cursor, teams: [str], win_size: int, win_end_date: date,
                                       stats_ranking_function: typing.Callable, home_only: bool = None,
                                       normalize_by_matches: bool = False) -> FeatureModelRanking:
    stats_list: [Stats] = Stats.get_windowed_stats_for_teams(cursor=cursor, teams=teams, win_size=win_size,
                                                             win_end_date=win_end_date, home_only=home_only,
                                                             normalize_by_matches=normalize_by_matches)

    return FeatureModelRanking(input_data=stats_list, id_fn=lambda x: x.team_name, feature_making_fn=stats_ranking_function)




class Stats(object):
    def __init__(self, team_name: str, played: int, won: int, drawn: int, lost: int, score_for: int, score_against: int,
                 cover_from: date, cover_to: date, normalize_by_matches: bool = False ):
        self.normalize_points_by_num_matches = normalize_by_matches
        self.team_name = team_name
        self.played = played
        self.cover_from = cover_from
        self.cover_to = cover_to
        self.won = Stats.default(won)
        self.drawn = Stats.default(drawn)
        self.lost = Stats.default(lost)
        self.score_for = Stats.default(score_for)
        self.score_against = Stats.default(score_against)
        # This is premier league points, 3 for a win, 1 for a draw
        self.points = 0
        self.goal_diff = 0
        self.n_samples = 0
        self.calc_derived()

    def calc_derived(self, normalize_by_matches: bool = False):
        denominator = 1
        if self.played > 0 and self.normalize_points_by_num_matches:
            denominator = self.played

        self.points = (3 * self.won + self.drawn) / denominator
        self.goal_diff = (self.score_for - self.score_against) / denominator
        self.n_samples = self.played

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
        copy.calc_derived()
        return copy

    def __str__(self) -> str:
        return '%s' % list(self)

    def __iter__(self):
        for i in (self.team_name, self.played, self.won, self.drawn, self.lost, self.score_for, self.score_against,
                  self.goal_diff, self.points):
            yield i

    def premier_league_ranking_fn(self) -> float:
        # Make sure goal diff is very very small compared to points, then use it to split according to normal prem
        # league procedure.
        return self.points + self.goal_diff / 1000.0

    def goal_diff_ranking_fn(self) -> float:
        # Make sure goal diff is very very small compared to points, then use it to split according to normal prem
        # league procedure.
        return self.goal_diff

    def points_ranking_fn(self) -> float:
        # Make sure goal diff is very very small compared to points, then use it to split according to normal prem
        # league procedure.
        return self.points

    def id_fn(self) -> str:
        return self.team_name


    @staticmethod
    def get_windowed_stats_for_teams(cursor: sqlite3.Cursor, teams: [str], win_size: int, win_end_date: date,
                                     home_only: bool = None, normalize_by_matches: bool = False) -> []:
        stats_list = [Stats.windowed_stats_for_team(cursor=cursor,
                                                    team=team,
                                                    win_weeks=win_size,
                                                    win_end_date=win_end_date,
                                                    home_only=home_only,
                                                    normalize_by_matches=normalize_by_matches
                                                    ) for team in teams]
        return stats_list

    # noinspection PyDictCreation
    @staticmethod
    def windowed_stats_for_team(cursor: sqlite3.Cursor, team: str, win_weeks: int, win_end_date: date,
                                home_only: bool = None, normalize_by_matches: bool = False):

        # If window is 1 week long, then don't expect results to include two Saturdays worth of data. i.e.
        # expect results to be start < window <= end.
        win_start_date = win_end_date - timedelta(weeks=win_weeks, days=-1)

        start_str = win_start_date.isoformat()
        stop_str = win_end_date.isoformat()

        sql_bindings = {'team_name': team, 'start_str': start_str, 'stop_str': stop_str}
        sql_bindings['home_team'] = sql_bindings['away_team'] = sql_bindings['team_name']
        if home_only is True:
            sql_bindings['away_team'] = 'NULL'
        elif home_only is False:
            sql_bindings['home_team'] = 'NULL'
        sql_home = \
            """
            SELECT
              COUNT(date)         AS played,
              COUNT(win)          AS wins,
              COUNT(drawn)        AS draws,
              COUNT(lost)         AS losses,
              SUM(scored_for)     AS for,
              SUM(scored_against) AS against
            
            FROM (SELECT
                    -- Need to do this as LIMIT does not operate as you might expect and you get weird count errors.
                    *,
                    CASE
                    WHEN home_team = :team_name AND home_score > away_score
                      THEN 1
                    WHEN home_team != :team_name AND home_score < away_score
                      THEN 1
                    END win,
            
                    CASE
                    WHEN home_team = :team_name AND home_score = away_score
                      THEN 1
                    WHEN home_team != :team_name AND home_score = away_score
                      THEN 1
                    END drawn,
            
                    CASE
                    WHEN home_team = :team_name AND home_score < away_score
                      THEN 1
                    WHEN home_team != :team_name AND home_score > away_score
                      THEN 1
                    END lost,
            
                    CASE
                    WHEN home_team = :team_name
                      THEN home_score
                    WHEN home_team != :team_name
                      THEN away_score
                    END scored_for,
            
                    CASE
                    WHEN home_team = :team_name
                      THEN away_score
                    WHEN home_team != :team_name
                      THEN home_score
                    END scored_against
            
            
                  FROM results
                  WHERE date BETWEEN :start_str AND :stop_str AND (home_team = :home_team OR away_team = :away_team)
                  ORDER BY date
                    DESC)            
        """

        logging.debug(sql_home)

        sql_out = iter(cursor.execute(sql_home, sql_bindings).fetchone())
        stats = Stats(team, *sql_out, cover_from=win_start_date,
                      cover_to=win_end_date, normalize_by_matches=normalize_by_matches)
        return stats

    @staticmethod
    def n_sample_stats_for_team(cursor: sqlite3.Cursor, team: str, n_samples: int, last_sample_date: date,
                                home_only: bool = None, normalize_by_matches: bool = False):
        stop_str = last_sample_date.isoformat()

        # Unfortunately we can't get away with two separate querries like we could with the windowed version
        # of this function, as we end up with too many samples in the join. Insteatwe have to have three separate
        # querries for home one, away only and a beast for both.
        sql_home = {}
        sql_bindings = {'team_name': team, 'n_samples': n_samples, 'last_sample_date': last_sample_date.isoformat()}
        sql_bindings['home_team'] = sql_bindings['away_team'] = sql_bindings['team_name']
        if home_only is True:
            sql_bindings['away_team'] = 'NULL'
        elif home_only is False:
            sql_bindings['home_team'] = 'NULL'

        #  We don't know how far back in time we've got to go to get our samples. So we end up needing to make to
        # queries. First to determine the start date that we've covering from. Then, the actual stats information. Both
        # are build on the same sql partial query.
        from_sql_partial = \
            """
                FROM results
                  WHERE (date <= :last_sample_date AND (home_team = :home_team OR away_team = :away_team))
                  ORDER BY date
                    DESC
                  LIMIT :n_samples
            """

        sql_first_date =  \
            """
            SELECT min(date) AS first_date FROM (SELECT * %s)  
            """ % from_sql_partial
        logging.debug(sql_bindings)
        logging.debug(sql_first_date)

        sql_out = cursor.execute(sql_first_date, sql_bindings).fetchone()
        (sql_out['first_date'])

        # Create a our date object, but also cope with the case that we have do not have _any_ data, so
        # most sensible answer is to return None
        first_date = date.fromtimestamp(datetime.strptime(sql_out['first_date'], '%Y-%m-%d').timestamp()) if sql_out['first_date'] is not None else None
        sql_home = \
            """
            SELECT
            COUNT(date)         AS played,
            COUNT(win)          AS wins,
            COUNT(drawn)        AS draws,
            COUNT(lost)         AS losses,
            SUM(scored_for)     AS for,
            SUM(scored_against) AS against
            
            
            FROM (SELECT
                -- Need to do this as LIMIT does not operate as you might expect and you get weird count errors.
                *,
                CASE
                WHEN home_team = :team_name AND home_score > away_score
                  THEN 1
                WHEN home_team != :team_name AND home_score < away_score
                  THEN 1
                END win,
            
                CASE
                WHEN home_team = :team_name AND home_score = away_score
                  THEN 1
                WHEN home_team != :team_name AND home_score = away_score
                  THEN 1
                END drawn,
            
                CASE
                WHEN home_team = :team_name AND home_score < away_score
                  THEN 1
                WHEN home_team != :team_name AND home_score > away_score
                  THEN 1
                END lost,
            
                CASE
                WHEN home_team = :team_name
                  THEN home_score
                WHEN home_team != :team_name
                  THEN away_score
                END scored_for,
            
                CASE
                WHEN home_team = :team_name
                  THEN away_score
                WHEN home_team != :team_name
                  THEN home_score
                END scored_against
            
            %s)
            """ % from_sql_partial
        logging.debug(sql_home)

        sql_out = iter(cursor.execute(sql_home, sql_bindings).fetchone())
        stats = Stats(team, *sql_out,  cover_from=first_date,
                      cover_to=last_sample_date, normalize_by_matches=normalize_by_matches)
        # stats.good_stat = True if stats.n_samples == n_samples else False

        return stats

    @staticmethod
    def calc_premier_league_points(wins: int, draws: int) -> int:
        return 3 * wins + 1 * draws

    @staticmethod
    def calc_score_diff(score_for: int, score_against: int) -> int:
        return score_for - score_against

    @staticmethod
    def default(value, look_for=None, replace_with=0):
        if value == look_for:
            return replace_with
        return value
