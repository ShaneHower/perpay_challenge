import logging
import psycopg2
from pathlib import Path
from psycopg2.extensions import connection, cursor
from typing import Union

log = logging.getLogger(__name__)


def init_logging(log_name: str):
    logging.basicConfig(
        filename= Path(__file__).parents[2] / 'logs' / f'{log_name}.log',
        filemode='w',
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)-50s %(message)s"
    )


def open_connection(local_mode: bool) -> Union[connection, cursor]:
    """Establish the connection to the postgres db.  During run time the connection will be made within the
    docker container.  However, you can instead toggle local_mode which will allow you to connect outside of the
    container streamlining development.
    """
    if local_mode:
        host = 'localhost'
        log.info('Establishing connection from local.')
    else:
        host = 'db'
        log.info('Establishing the connection from within the container.')

    conn = psycopg2.connect(
        host=host,
        port='5432',
        dbname='perpay',
        user='postgres',
        password='example',
    )

    cur = conn.cursor()

    # Log Postgres version
    cur.execute("select version()")
    log.info(f'Postgres Version: {cur.fetchone()}')

    log.info('Connection successful.')
    return conn, cur


def remove_bad_chars(column_name: str) -> str:
    """ This function takes a column name and finds the index of the first non-alphanumeric character
    (if it exists). It then slices the string at the problem character's location.  This leaves us with
    the clean first half of the string and removes any unnecessary fluff.

    For example the column "Default (as of 2023-05-09)" would get sliced into "Default".  Strings without
    bad characters will remain the same.
    """
    problem_index = -1
    for char in column_name:
        if char == ' ':
            problem_index += 1
        elif not char.isalnum():
            break
        else:
            problem_index += 1

    # If we made it through the entire string, there are no bad characters.
    if len(column_name) == problem_index + 1:
        return column_name
    else:
        return column_name[:problem_index]


def clean_header(columns: list) -> list:
    """This function does some basic column header normalization.  Remove non-alphanumeric characters,
    replace spaces with underscores, and make everything lower case.
    """
    cleaned_cols = []
    for col in columns:
        col = remove_bad_chars(col)
        col = col.lower()
        col = col.replace(' ', '_')
        cleaned_cols.append(col)
    return cleaned_cols
