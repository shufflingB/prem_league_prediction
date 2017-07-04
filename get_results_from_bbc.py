#!/usr/bin/env python

from bs4 import BeautifulSoup, NavigableString
from argparse import ArgumentParser
import sqlite3
import requests
import re
import datetime
import os
import logging

from datetime import date


DEBUG = True
LIVE_URL = 'http://www.bbc.co.uk/sport/football/premier-league/results'
TEST_FILE = '/Users/jhume/work/fantasy_football/test.html'
DB_FILE = '/Users/jhume/work/fantasy_football/raw_results.db'
MANAGER_DB_FILE = '/Users/jhume/work/fantasy_football/tests/fixture/managers_2017_05_17.db'
TABLE_NAME = 'results'
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


SQL_CREATE_TABLE = """
CREATE TABLE %s (
 id INTEGER PRIMARY KEY,
 date TEXT NOT NULL,
 home_team  TEXT NOT NULL,
 home_score INTEGER NOT NULL,
 away_team TEXT NOT NULL,
 away_score INTEGER NOT NULL
); """ % TABLE_NAME

SQL_INSERT = '''INSERT INTO %s(date, home_team, home_score, away_team, away_score) VALUES (?,?,?,?,?)''' % TABLE_NAME

SQL_DROP_TABLE = '''DROP TABLE IF EXISTS %s;''' % TABLE_NAME


def get_match_date(match_day_string: NavigableString)->datetime.date:
    match_day_string = str(match_day_string)
    # Expected format is  a = "\n\           Monday 17th October 2016    "
    regex_pattern = '(\d+)\w\w\s+(\w+)\s+(\d\d\d\d)'
    match = re.search(regex_pattern, match_day_string)
    assert match is not None, "Failed to capture date info, regex '%s', string '%s'" % \
                              (regex_pattern, match_day_string)

    day = match.groups()[0]
    month = match.groups()[1]
    year = match.groups()[2]

    logging.debug('Extracted year %s, month %s, day %s' % (year, month, day))
    date = datetime.date.fromtimestamp( datetime.datetime.strptime("%s %s %s" % (year, month, day),
                                                                   '%Y %B %d').timestamp())
    logging.debug('Created a date object %s' % date)
    return date

def get_manager(managers_db_cursor: sqlite3.Cursor, team_name: str, on_date: date) -> str:
    # This is a bit more complicated than might at first be thought because we have to deal with incidences where there
    # is no manager in place, e.g. Watford from 2016-05-30 when Quique departed, to
    # 2016-07-01 when Walter arrived.
    # To do this we bear in mind that we're only really interested in the impact of the manager on the team, so we'll
    # assume that a team will remain largely the same, post manager departure, until the next person arrives. This in
    # turn translates into a query for all managers for the club who have been appointed upto, or prior to the date
    # being checked and to then just take the most recent one for the enquiry date.
    sql = 'SELECT * FROM managers WHERE team_name="%s" AND appointed <="%s" ORDER BY appointed DESC LIMIT 1' % \
          (team_name, on_date )
    logging.debug('Looking up manager with %s' % sql)
    query_out = managers_db_cursor.execute(sql).fetchone()
    logging.debug('Got manager name %s' % query_out['manager_name'])
    return query_out['manager_name']



parser: ArgumentParser = ArgumentParser()
parser.description = "Downloads the Premier PremierLeague results for the current season from the BBC"

parser.add_argument('-u', '--url',
                    help='Specifies the url to download from %s' % LIVE_URL,
                    default=LIVE_URL,
                    required=False,
                    type=str
                    )

parser.add_argument('-o', '--out-db',
                    help='Specifies the name of sqlite DB to OVERWRITE %s' % DB_FILE,
                    default=DB_FILE,
                    required=False,
                    type=str
                    )

parser.add_argument('-d', '--debug',
                    help='Increase log level and rather than downloading fresh data, parse the debug test file %s' %
                         TEST_FILE,
                    default=False,
                    action='store_true'
                    )

parser.add_argument('-f', '--force',
                    help='Overwrite any existing DB, default is not',
                    default=False,
                    action='store_true'
                    )


parser.add_argument('-m', '--managers-db',
                    help='Merges manager information from the specified DB file, default %s ' % None,
                    default=None,
                    required=False,
                    type=str
                    )


parser.add_argument('-t', '--test-file',
                    help='Specifies a test file html file to parse (as opposed to hitting bbc), default %s' % TEST_FILE,
                    default=TEST_FILE,
                    required=False,
                    type=str
                    )


args = parser.parse_args()
url_path = args.url
out_db_file = args.out_db
managers_db_file = args.managers_db
debug = args.debug
drop_table = args.force
test_file = args.test_file


data = None
if debug:
    logging.basicConfig(level=DEBUG)
    with open(file=test_file) as infile:
        data = infile.read()
else:
    request = requests.get(url=url_path)
    data = request.text

soup = BeautifulSoup(data, 'html.parser')



with sqlite3.connect(out_db_file) as db_in_connection:
    db_cursor = db_in_connection.cursor()
    if drop_table:
        db_cursor.execute(SQL_DROP_TABLE)

    db_cursor.execute(SQL_CREATE_TABLE)

    def table_stats_soup_filter(tag)-> bool:
        good_class_str = 'table-stats'
        good_table_str = 'This table charts the fixtures during'
        if tag.has_attr('class') and re.search(good_class_str, str(tag['class'])) :
            # print(tag)
            # print(tag['class'])
            if re.search(good_table_str, tag.caption.string):
                return True
        return False


    if managers_db_file:
        logging.debug('Connecting to managers DB in %s' % managers_db_file)
        db_managers = sqlite3.connect(managers_db_file)
        db_managers.row_factory= sqlite3.Row
        db_managers_cursor = db_managers.cursor()

    for match_day_table_html in soup.find_all(table_stats_soup_filter):
        date = get_match_date(match_day_table_html.caption.string)

        # Match results are contained in <td class="match-details">, however there are also
        # some tags that <td class="match-details" scope='col'> that contain who knows what
        # that need to be filtered out.

        for match_details_html in match_day_table_html.find_all(class_='match-details', scope=''):
            logging.debug('match_details_html %s' % match_details_html)
            home_team = match_details_html.find(class_='team-home').a.string
            logging.debug('Extracted home_team %s' % home_team)
            away_team = match_details_html.find(class_='team-away').a.string
            logging.debug('Extracted away_team %s' % away_team)

            score_h_vs_a = match_details_html.find(class_='score').abbr.string  # This is in the form X-Y
            regex_pattern = '(\d+)-(\d+)'
            match = re.search(regex_pattern, score_h_vs_a)
            assert match is not None, "Failed to capture scores info, regex '%s', string '%s'" % \
                                      (regex_pattern, score_h_vs_a)
            score_home = match.groups()[0]
            score_away = match.groups()[1]

            home_team_str = home_team
            away_team_str = away_team
            if managers_db_file:
                home_manager = get_manager(managers_db_cursor=db_managers_cursor, team_name=home_team, on_date=date)
                home_team_str = '%s\'s %s' % (home_manager, home_team)
                away_manager = get_manager(managers_db_cursor=db_managers_cursor, team_name=away_team, on_date=date)
                away_team_str = '%s\'s %s' % (away_manager, away_team)

            insert_arr = date.isoformat(), str(home_team_str), int(score_home), str(away_team_str), int(score_away)

            logging.info('Persisting %s' % insert_arr.__str__())
            db_cursor.execute(SQL_INSERT, insert_arr).fetchall()
            db_in_connection.commit()
