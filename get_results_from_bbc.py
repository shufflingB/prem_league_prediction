#!/usr/bin/env python

import sqlite3
import requests
import re
import datetime
from bs4 import BeautifulSoup, NavigableString
from argparse import ArgumentParser


DEBUG = True
LIVE_URL = 'http://www.bbc.co.uk/sport/football/premier-league/results'
TEST_FILE = '/Users/jhume/work/fantasy_football/test.html'
DB_FILE = '/Users/jhume/work/fantasy_football/test_out.db'
TABLE_NAME = 'results'


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


def get_match_datetime(match_day_string: NavigableString)->datetime:
    match_day_string = str(match_day_string)
    # Expected format is  a = "\n\           Monday 17th October 2016    "
    regex_pattern = '(\d+)\w\w\s+(\w+)\s+(\d\d\d\d)'
    match = re.search(regex_pattern, match_day_string)
    assert match is not None, "Failed to capture date info, regex '%s', string '%s'" % \
                              (regex_pattern, match_day_string)

    day = match.groups()[0]
    month = match.groups()[1]
    year = match.groups()[2]

    return datetime.datetime.strptime("%s %s %s" % (year, month, day), '%Y %B %d')


parser: ArgumentParser = ArgumentParser()
parser.description = "Downloads the Premier League results for the current season from the BBC"

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
                    help='Rather than downloading fresh data, parse the debug test file %s' % TEST_FILE,
                    default=False,
                    action='store_true'
                    )

parser.add_argument('-f', '--force',
                    help='Overwrite any existing DB, default is not',
                    default=False,
                    action='store_true'
                    )

parser.add_argument('-t', '--test-file',
                    help='Specifies a test file, default %s' % TEST_FILE,
                    default=TEST_FILE,
                    required=False,
                    type=str
                    )


args = parser.parse_args()
url_path = args.url
out_db_file = args.out_db
debug = args.debug
drop_table = args.force
test_file = args.test_file


data = None
if debug:
    with open(file=test_file) as infile:
        data = infile.read()
else:
    request = requests.get(url=url_path)
    data = request.text

soup = BeautifulSoup(data, 'html.parser')

with sqlite3.connect(out_db_file) as db_connection:
    db_cursor = db_connection.cursor()
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

    for match_day_table_html in soup.find_all(table_stats_soup_filter):
        date = get_match_datetime(match_day_table_html.caption.string)
        # print(date)

        # Match results are contained in <td class="match-details">, however there are also
        # some tags that <td class="match-details" scope='col'> that contain who knows what
        # that need to be filtered out.
        for match_details_html in match_day_table_html.find_all(class_='match-details', scope=''):
            # print(match_details)
            home_team = match_details_html.find(class_='team-home').a.string
            away_team = match_details_html.find(class_='team-away').a.string

            score_h_vs_a = match_details_html.find(class_='score').abbr.string  # This is in the form X-Y
            regex_pattern = '(\d+)-(\d+)'
            match = re.search(regex_pattern, score_h_vs_a)
            assert match is not None, "Failed to capture scores info, regex '%s', string '%s'" % \
                                      (regex_pattern, score_h_vs_a)
            score_h = match.groups()[0]
            score_a = match.groups()[1]

            insert_arr =(date.isoformat(' '), str(home_team), int(score_h), str(away_team), int(score_a))

            print(insert_arr)
            db_cursor.execute(SQL_INSERT, insert_arr).fetchall()
            db_connection.commit()
