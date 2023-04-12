from nhs_foundry_common.test_utils import assert_df
from myproject.datasets.direct_patient_care_fte.arrs import fin_year_to_end_dates
from pyspark.sql import functions as F


def test_perform_calculation(spark_session):

    df = spark_session.createDataFrame(
        [
            ['1950_51',  'a'],
            ['2020_21',  'a'],
            ['2100_01',  'a'],
            ['1950_51',  'b'],
            ['2020_21',  'b'],
            ['2100_01',  'b']
        ],
        ['financial_year',  'other'])

    dates_ref = spark_session.createDataFrame(
        [
            ['1951-03-31', 4, 195051, 'x'],
            ['2021-03-31', 4, 202021, 'x'],
            ['2101-03-31', 4, 210001, 'x'],
            ['1951-03-31', 4, 195051, 'y'],
            ['2021-03-31', 4, 202021, 'y'],
            ['2101-03-31', 4, 210001, 'y'],
            ['1950-06-30', 1, 195051, 'x'],
            ['2020-06-30', 1, 202021, 'x'],
            ['2100-06-30', 1, 210001, 'x'],
            ['1950-09-30', 2, 195051, 'x'],
            ['2020-09-30', 2, 202021, 'x'],
            ['2100-09-30', 2, 210001, 'x'],
            ['1950-12-31', 3, 195051, 'x'],
            ['2020-12-31', 3, 202021, 'x'],
            ['2100-12-31', 3, 210001, 'x']
        ],
        ['quarter_end', 'fin_quarter_no', 'fin_year', 'other']) \
        .withColumn('quarter_end', F.to_date(F.col('quarter_end')))

    expected_df = spark_session.createDataFrame(
        [
            ['1950_51',  'a',  '1951-03-31', 195051],
            ['2020_21',  'a',  '2021-03-31', 202021],
            ['2100_01',  'a',  '2101-03-31', 210001],
            ['1950_51',  'b',  '1951-03-31', 195051],
            ['2020_21',  'b',  '2021-03-31', 202021],
            ['2100_01',  'b',  '2101-03-31', 210001]
        ],
        ['financial_year',  'other',  'financial_year_end_date', 'fin-year']) \
        .withColumn('financial_year_end_date', F.to_date(F.col('financial_year_end_date')))

    result_df = fin_year_to_end_dates(df, dates_ref)

    assert_df(expected_df, result_df)
