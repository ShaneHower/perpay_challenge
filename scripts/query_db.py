import argparse
import datetime
import logging
import traceback
from pathlib import Path

from core.helper_functions import open_connection, init_logging


log = logging.getLogger(__name__)
SQL_PATH = Path(__file__).parent / 'sql'


def main(local_mode: bool):
    try:
        conn, cur = open_connection(local_mode=local_mode)

        # get_ratio_rolling_avg unloads its results into a new table every time it runs.
        with open(SQL_PATH / 'get_ratio_rolling_avg.sql') as sql_file:
            sql = sql_file.read()
            sql = sql.replace('{ts}', datetime.datetime.now().strftime('%Y%m%d%H%M%S'))

        cur.execute(sql)
        conn.commit()

    except Exception as e:
        log.error(traceback.format_exc())
        raise e

    finally:
        # This is set in case the connection itself failed and we never assinged the conn and cur variable
        if 'cur' in locals() and cur is not None:
            conn.close()
            cur.close()


if __name__ == '__main__':
    init_logging(log_name='query_db_log')

    # Initialize CLI args
    parser = argparse.ArgumentParser()
    parser.add_argument('--local-mode', action='store_true', help='Indicates that we are connecting to the postgres db outside of the docker container')
    args = parser.parse_args()
    local_mode = args.local_mode

    main(local_mode)
