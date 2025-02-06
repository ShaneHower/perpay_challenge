import csv
import logging
from pathlib import Path
from psycopg2.extensions import cursor, connection

from core.helper_functions import clean_header

log = logging.getLogger(__name__)


class Loans:
    def __init__(self):
        log.info('Instantiate Loans.')

        # Source meta data
        self.source_data = Path(__file__).parents[2] / 'data' / 'loans.csv'
        self.source_header = self._pull_header_from_source()

        # DB meta data
        self.stg_table_name = 'loans_stg'
        self.prod_table_name = 'loans'
        self.approved_header = ['date_of_repayment', 'number_of_loans', 'default', 'liquidation']

        # DDLs
        self.stg_table_ddl = f'''
            create table if not exists {self.stg_table_name} (
                "date_of_repayment" date,
                "number_of_loans" integer,
                "default" integer,
                "liquidation" integer
            );
        '''

        self.prod_table_ddl = f'''
            create table if not exists {self.prod_table_name} (
                "date_of_repayment" date,
                "number_of_loans" integer,
                "default" integer,
                "liquidation" integer,
                "ts" timestamp
            );
        '''

        # Init work
        self._validate_source_header()

        log.info('Loans instantiated.')

    def _pull_header_from_source(self) -> list:
        """We only want to pull the header out of the CSV file to ensure that we are working with the columns that we expect.
        This method doesn't load the entire CSV file into memory which is crucial if the csv becomes very large.
        """
        log.info('Pull column header from source.')
        with open(self.source_data, mode='r') as f:
            reader = csv.reader(f)
            source_header = next(reader)

        log.info('Pulled column header successfully.')
        return source_header

    def _validate_source_header(self):
        """This may be a bit convoluted so it bears some explanation. Since we are using psycopg2's cursor.copy_from()
        method.  We need to ensure that the column order is maintained on every load, otherwise we could upload column
        values to the wrong location within our loans table in the db.

        How do we do this?  Well the simple way is to take the header that is supplied within the file at the development
        stage and use that as our source of truth.  Meaning we would compare all future CSV headers to the original CSV
        header that we received, which is the following:
            Date of Repayment, Number of Loans, Default (as of 2023-05-09), Liquidation (as of 2023-05-09)

        You can instantly see this will not work.  We have variability in the column names, specifically date values in
        the "Default" and "Liquidation" fields. This means we have to consider an alternative approach - clean up the
        header and compare it against an approved header from our loans table in the db.

        To do this, we make some assumptions about the CSV header in order to ensure the data is in the right order.
            1. We expect the number of columns to be consistent on every run (in other words no extra or missing columns)
            2. The data will always supply ' ' for spacing instead of special characters like '_' or '-'
            3. If there is a special character caught, any subsequent character isn't important and can be removed.
            4. All future CSVs will have the same naming format.  The only difference that we may hit is casing (upper or
               lower case) and updated dates within the "Default" and "Liquidation" fields which our algorithm will have
               coverage over.

        Next we will have to normalize the given csv header and compare it against our approved header. Here's how the
        normalization algorithm will work:
            2. Loop through the characters of each column name. Once we hit a non-alphanumeric character (that is not a
               space), we will purge the string from that bad character. So for example:
                   Default (as of 2023-05-09) -> Default
            2. Replace spaces with underscore
            3. Lower case everything

        If we find that we are receiving data with more variable header names we will have to go back to the drawing board
        or work with the source team to come to an agreement on expected formatting.
        """
        log.info('Start column validations.')

        if len(self.source_header) > len(self.approved_header):
            raise Exception('The source data has an extra column.')
        elif len(self.source_header) < len(self.approved_header):
            raise Exception('The source data is missing a column.')
        else:
            log.info('Source data column count validation passed.')

        source_header = clean_header(self.source_header)
        if source_header != self.approved_header:
            raise Exception('The source data may be out of order.')
        else:
            log.info('Column header order validation passed.')

    def create_stg_table(self, cur:cursor, conn:connection):
        """Our staging table is truncate/reloaded on every run.  We do this to ensure validations pass before
        adding it to our production table.  We also timestamp the upload from the staging table within the prod table
        so that we can easily remove compromised uploads if needed.
        """
        log.info(f'Creating {self.stg_table_name} table if it does not exist.')
        cur.execute(self.stg_table_ddl)
        conn.commit()

        cur.execute(f'truncate table {self.stg_table_name}')
        conn.commit()
        log.info(f'{self.stg_table_name} created and truncated.')

        log.info('Uploading source data to loans_stg.')
        with open(self.source_data, mode='r') as source:
            next(source)
            cur.copy_from(source, self.stg_table_name, sep=',')
        conn.commit()
        log.info('Source data added to loans_stg.')

    def validate_stg(self, cur:cursor):
        """If we loaded up the data correctly, the number of loans should not be less than default + liquidation"""

        log.info('Run staging table validations.')
        validate_stg_sql = '''
            select *
            from loans_stg
            where number_of_loans < "default" + liquidation
        '''

        cur.execute(validate_stg_sql)
        if cur.fetchone():
            raise Exception('The staging table may have mismatched columns.  We have some rows that have more defaults + liquadation vs the total number of loans')
        else:
            log.info('Staging table validation passed.')

    def create_prod_table(self, cur:cursor, conn:connection):
        cur.execute(self.prod_table_ddl)
        conn.commit()

        insert_sql = '''
            insert into loans
            (
                date_of_repayment,
                number_of_loans,
                "default",
                liquidation,
                ts
            )
            select
                date_of_repayment,
                number_of_loans,
                "default",
                liquidation,
                date_trunc('second', now())
            from loans_stg
        '''

        cur.execute(insert_sql)
        conn.commit()
