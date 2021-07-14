"""
Downloading latest data from Our World in Data for covid19

"""
import datetime
from urllib.request import urlopen

import pandas as pd
from sqlalchemy.engine import Engine

from covid.db import engine

SCHEMA = 'covid'

# keeping table names here for easier maintainability
CASES = 'cases'
COUNTRY_MAPPING = 'who_country_mapping'
HOSPITAL = 'hospitalization'
TESTS = "tests"
VACCINATIONS = 'vaccinations'
INDICATOR = 'other_indicator'

OUR_WORLD_IN_DATA = "https://covid.ourworldindata.org/data/owid-covid-data.csv"


def import_our_world_in_data(con: Engine):
    """
    Importing data from our world in data. ALl the data comes in one
    big CSV file, which is gonna be split into different tables. If
    this function is used for Updates, the whole tables will be
    replace
    
    :param con: DB connection
    """

    tst = datetime.datetime.now()
    with urlopen(OUR_WORLD_IN_DATA) as f:
        df = pd.read_csv(f)

    # making sure the date is a datetime object
    df.loc[:, 'date'] = pd.to_datetime(df['date'])

    # first separate country identifiers
    country_mapping = df.loc[:, ['iso_code', 'continent', 'location']]
    country_mapping.drop_duplicates(inplace=True)

    # location = country_name
    country_mapping.rename(columns={'location': 'country_name'}, inplace=True)

    country_mapping.to_sql(schema=SCHEMA, name=COUNTRY_MAPPING, index=False,
                           index_label='iso_code', if_exists='replace',
                           con=con)

    # using the same index for all these tables to make joins faster
    ind = ['iso_code', 'date']

    # next table will be cases and deaths
    cols = ['total_cases', 'new_cases', 'total_cases_per_million',
            'new_cases_per_million', 'total_deaths', 'new_deaths',
            'total_deaths_per_million', 'new_deaths_per_million',
            'reproduction_rate', 'excess_mortality']
    cases = df.loc[:, ind + cols].dropna(subset=cols, how='all')
    cases.to_sql(schema=SCHEMA, name=CASES, index=False, index_label=ind,
                 if_exists='replace', con=con)

    # hospitalization data
    cols = ['icu_patients', 'icu_patients_per_million', 'hosp_patients',
            'hosp_patients_per_million', 'weekly_icu_admissions',
            'weekly_icu_admissions_per_million', 'weekly_hosp_admissions',
            'weekly_hosp_admissions_per_million']
    hospital = df.loc[:, ind + cols].dropna(subset=cols, how='all')
    hospital.to_sql(schema=SCHEMA, name=HOSPITAL, index=False, index_label=ind,
                    if_exists='replace', con=con)

    # test data
    cols = ['new_tests', 'total_tests', 'total_tests_per_thousand',
            'new_tests_per_thousand', 'new_tests_smoothed',
            'new_tests_smoothed_per_thousand', 'positive_rate',
            'tests_per_case', 'tests_units']
    tests = df.loc[:, ind + cols].dropna(subset=cols, how='all')
    tests.to_sql(schema=SCHEMA, name=TESTS, index=False, index_label=ind,
                 if_exists='replace', con=con)

    # vaccination data
    cols = ['total_vaccinations', 'people_vaccinated',
            'people_fully_vaccinated', 'new_vaccinations',
            'new_vaccinations_smoothed', 'total_vaccinations_per_hundred',
            'people_vaccinated_per_hundred',
            'people_fully_vaccinated_per_hundred',
            'new_vaccinations_smoothed_per_million']
    vaccination = df.loc[:, ind + cols].dropna(subset=cols, how='all')
    vaccination.to_sql(schema=SCHEMA, name=VACCINATIONS, index=False,
                       index_label=ind, if_exists='replace', con=con)

    # other indicator. These indicators are not a time series, so 
    # dropping duplicates is needed
    ind.remove('date')
    cols = ['stringency_index', 'population', 'population_density',
            'median_age', 'aged_65_older', 'aged_70_older', 'gdp_per_capita',
            'extreme_poverty', 'cardiovasc_death_rate', 'diabetes_prevalence',
            'female_smokers', 'male_smokers', 'handwashing_facilities',
            'hospital_beds_per_thousand', 'life_expectancy',
            'human_development_index']
    indicator = df.loc[:, ind + cols].drop_duplicates(subset=cols)
    indicator.to_sql(schema=SCHEMA, name=INDICATOR, index=False,
                     index_label=ind, if_exists='replace', con=con)

    print(f"Time to import covid data from OWID: {datetime.datetime.now() - tst}")

if __name__ == '__main__':
    import_our_world_in_data(engine)
