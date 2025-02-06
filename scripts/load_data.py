import argparse
import logging
import traceback

from core.helper_functions import open_connection, init_logging
from core.loans import Loans


log = logging.getLogger(__name__)


def main(local_mode: bool):
    try:
        log.info('Begin')
        conn, cur = open_connection(local_mode=local_mode)

        loans = Loans()
        loans.create_stg_table(cur=cur, conn=conn)
        loans.validate_stg(cur=cur)
        loans.create_prod_table(cur=cur, conn=conn)

    except Exception as e:
        log.error(traceback.format_exc())
        raise e

    finally:
        # This is set in case the connection itself failed and we never assinged the conn and cur variable
        if 'cur' in locals() and cur is not None:
            conn.close()
            cur.close()

        log.info('Finished')


if __name__ == '__main__':
    init_logging(log_name='load_data_log')

    # Initialize CLI args
    parser = argparse.ArgumentParser()
    parser.add_argument('--local-mode', action='store_true', help='Indicates that we are connecting to the postgres db outside of the docker container')
    args = parser.parse_args()
    local_mode = args.local_mode

    main(local_mode)
