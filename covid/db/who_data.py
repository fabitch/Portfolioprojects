"""
Downloading latest data from WHO on covid19

# TODO: 1. Download cases and deaths
        2. Download Latest reported counts of cases and deaths, and
            transmission classification
        3. Download Vaccination Data
        4. Import all data to DB
        5. Create Task to run daily
"""
from gzip import GzipFile
from urllib.request import urlopen

import pandas as pd
from sqlalchemy.engine import Engine

from covid.db import engine

SCHEMA = 'covid'

# keeping table names here for easier maintainability
CASES = 'cases'
LATEST_REPORTS = 'latest_reports'
VACCINATIONS = 'vaccinations'
VACCINATION_META = 'vaccination_metadata'
WHO_COUNTRY_MAPPING = 'who_country_mapping'

DATA_SOURCES = {
    CASES: "https://covid19.who.int/WHO-COVID-19-global-data.csv",
    LATEST_REPORTS: "https://covid19.who.int/WHO-COVID-19-global-table-data.csv",
    VACCINATIONS: "https://covid19.who.int/who-data/vaccination-data.csv",
    VACCINATION_META: "https://covid19.who.int/who-data/vaccination-metadata.csv"
}


def import_case_data_from_who(connection: Engine):
    """
   Imports case data from who and creates country - WHO-Region mapping

   The file from this download is always a full data set, hence using the
   replace method in pandas.to_sql to avoid duplicates

   :param connection:
   :return:
   """

    # downloading data from WHO
    with urlopen(DATA_SOURCES[CASES]) as file:
        df = pd.read_csv(file)

        df.columns = df.columns.str.lower()
        country_mapping = df.loc[:, ['country_code', 'country', 'who_region']]
        country_mapping.drop_duplicates(inplace=True)

        # the index will be used as primary key in DB
        country_mapping.set_index('country_code', inplace=True)

        country_mapping.to_sql(schema=SCHEMA, name=WHO_COUNTRY_MAPPING,
                               con=connection, if_exists='replace')

        # removing country name and who region from case data
        df.drop(columns=['country', 'who_region'], inplace=True)

        df.set_index('country_code', inplace=True)

        df['date_reported'] = pd.to_datetime(df['date_reported'])
        df.to_sql(schema=SCHEMA, name=CASES, con=connection,
                  if_exists='replace')


def import_vaccination_data(connection: Engine):
    """
    Importing vaccination data from WHO
    The file from this download is always a full data set, hence using the
    replace method in pandas.to_sql to avoid duplicates

    :param connection:
    :return:
    """

    with urlopen(DATA_SOURCES[VACCINATIONS]) as file:
        # vaccination data download is a zip file
        file = GzipFile(fileobj=file)
        df = pd.read_csv(file)

        # converting column names to all lower and renaming ISO3 to country_code
        # for consistency
        df.columns = df.columns.str.lower()
        df.rename(columns={'iso3': 'country_code'}, inplace=True)
        df.drop(columns=['country', 'who_region'], inplace=True)
        df.set_index('country_code', inplace=True)
        df['date_updated'] = pd.to_datetime(df['date_updated'])

        df.to_sql(schema=SCHEMA, name=VACCINATIONS, con=connection,
                  if_exists='replace')


if __name__ == '__main__':
    import_case_data_from_who(engine)
    import_vaccination_data(engine)
