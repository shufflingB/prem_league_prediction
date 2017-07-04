import sqlite3
from datetime import date
from datetime import datetime
from datetime import timedelta


class ActualResults:
    def __init__(self, db_cursor: sqlite3.Cursor = None, win_size: timedelta = None, win_end: date = '*'):
        self.cursor = db_cursor
        self.win_size = win_size
        self.win_end = win_end
        self.teams = None
        self.dates = None
        self.results_data = None
        if self.cursor is not None:
            self.read_data(self.cursor)

    def read_data(self, db_cursor: sqlite3.Cursor):
        self.cursor = db_cursor
        self.teams = ActualResults.get_teams(self.cursor)
        self.dates = ActualResults.get_dates(self.cursor)
        self.results_data = ActualResults.get_results_data(self.cursor, win_size=self.win_size, win_end=self.win_end)


    @staticmethod
    def get_teams(db_cursor: sqlite3.Cursor) -> [str]:

        sql = """SELECT DISTINCT home_team FROM results ORDER BY home_team ASC"""
        return [x[0] for x in db_cursor.execute(sql).fetchall()]

    @staticmethod
    def get_dates(db_cursor: sqlite3.Cursor) -> [date]:
        sql = """SELECT DISTINCT date FROM results ORDER BY date ASC"""
        return list(map(lambda x: date.fromtimestamp(datetime.strptime(x[0], '%Y-%m-%d').timestamp()),
                        db_cursor.execute(sql).fetchall()))
    @staticmethod
    def get_results_data(db_cursor: sqlite3.Cursor, win_size: timedelta = None, win_end: date = '*') -> [()]:

        sql_bind = {}
        if isinstance(win_end, date):
            if isinstance(win_size, timedelta):
                win_start = win_end - win_size
                sql_bind = {'win_end':win_end.isoformat(), 'win_start':win_start.isoformat()}
                sql = \
                    """
                    SELECT 
                        date, 
                        home_team, 
                        home_score, 
                        away_team, 
                        away_score 
                    FROM results 
                    WHERE date BETWEEN :win_start AND :win_end  
                    ORDER BY date 
                      ASC, home_team 
                        ASC
                    """
            else:
                sql_bind = {'win_end':win_end.isoformat()}
                sql = \
                    """
                    SELECT 
                      date, 
                      home_team, 
                      home_score, 
                      away_team, 
                      away_score 
                    FROM results 
                    WHERE date=:win_end
                    ORDER BY date 
                      ASC, home_team 
                        ASC
                    """


                # print(sql)
        else:
            # sql = 'SELECT date, home_team, home_score, away_team, away_score FROM results ' \
            #       'ORDER BY date ASC, home_team ASC'
            sql = \
                """
                SELECT 
                  date, 
                  home_team, 
                  home_score, 
                  away_team, 
                  away_score 
                FROM results 
                ORDER BY date 
                  ASC, home_team 
                    ASC
                """

        return db_cursor.execute(sql, sql_bind).fetchall()

    @staticmethod
    def unpack_match_result_data(match_result) ->(str, str, int, str, int, str):
        home_team_name = match_result['home_team']
        away_team_name = match_result['away_team']

        home_score = match_result['home_score']
        away_score = match_result['away_score']

        match_day = match_result['date']

        actual_result = 'draw'
        if home_score > away_score:
            actual_result = 'home_win'
        elif home_score < away_score:
            actual_result = 'away_win'

        return match_day, home_team_name, home_score, away_team_name, away_score, actual_result
