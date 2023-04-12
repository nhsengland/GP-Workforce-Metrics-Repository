from myproject.datasets.utils import get_metrics, generate_percentage_baseline
from nhs_foundry_common.test_utils import assert_df
from pyspark.sql import functions as F
import myproject.datasets.utils as utils

dimension_cols = [
    'staff_group',
    'staff_role',
    'detailed_staff_role']
date_col = "effective_snapshot_date"
loc_dict = {"country_id": "nhs-country",
            "nhs_region_id": "nhs-region",
            "stp_code": "nhs-stp",
            "current_ccg_code": "nhs-ccg"}
metric_id_prefix = "GP_FTEs"
source_of_data = "GP Level Census"
unit_type = "FTE"


def test_only_required_arguments_generate_metric_data(spark_session):
    df = spark_session.createDataFrame(
        [
            ['a', 'nhs-ccg', '1', '2021-04-03', 'square', 'red', 'x'],
            ['b', 'nhs-stp', '2', '2021-04-03', 'square', 'red', 'x'],
            ['c', 'nhs-region', '3', '2021-04-03', 'square', 'pink', 'x'],
            ['d', 'nhs-country', '4', '2021-04-03', 'circle', 'yellow', 'x'],
            ['e', 'somwhere else', '5', '2021-04-03', 'circle', 'blue', 'x']
        ],
        ['location_id', 'location_type', 'metric_value', 'the_date', 'shape', 'colour', 'other'])

    metadata = spark_session.createDataFrame(
        [
            ['block_square_red', 'yards', 'square', 'red'],
            ['block_square_pink', 'cm', 'square', 'pink'],
            ['block_circle_yellow', 'inches', 'circle', 'yellow'],
            ['block_circle_blue', 'ft', 'circle', 'blue'],

        ],
        ['metric_id', 'unit', 'dimension_1', 'dimension_2'])

    expected_df = spark_session.createDataFrame(
        [
            ['a', 'nhs-ccg', '1', '2021-04-03', 'block_square_red', 'yards', 'data source'],
            ['b', 'nhs-stp', '2', '2021-04-03', 'block_square_red', 'yards', 'data source'],
            ['c', 'nhs-region', '3', '2021-04-03', 'block_square_pink', 'cm', 'data source'],
            ['d', 'nhs-country', '4', '2021-04-03', 'block_circle_yellow', 'inches', 'data source'],
            ['e', 'somwhere else', '5', '2021-04-03', 'block_circle_blue', 'ft', 'data source']
        ],
        ['location_id', 'location_type', 'metric_value', 'timestamp', 'metric_id', 'unit', 'source']) \
        .withColumn('metric_value_12monthrolling', F.lit(None).cast('double')) \
        .withColumn('datapoint_id', F.concat_ws(
                '_',
                F.date_format(F.col('timestamp'), 'yyyy-MM-dd'),
                F.col('metric_id'),
                F.col('location_type'),
                F.col('location_id'),
                F.col('source')
                ))

    result_df = utils.generate_metric_data(df, metadata, 'block', 'the_date', ['shape', 'colour'], 'data source')
    assert_df(expected_df, result_df)


def test_with_optional_arguments_generate_metric_data(spark_session):
    df = spark_session.createDataFrame(
        [
            ['a', 'nhs-ccg', '1', '2021-04-03', 'square', 'red', 'x', 'Y'],
            ['b', 'nhs-stp', '2', '2021-04-03', 'square', 'red', 'x', 'N'],
            ['c', 'nhs-region', '3', '2021-04-03', 'square', 'pink', 'x', 'Y'],
            ['d', 'nhs-country', '4', '2021-04-03', 'circle', 'yellow', 'x', 'N'],
            ['e', 'somwhere else', '5', '2021-04-03', 'circle', 'blue', 'x', 'Y']

        ],
        ['location_id', 'location_type', 'metric_value', 'the_date', 'shape', 'colour', 'other', 'is_favourite'])

    metadata = spark_session.createDataFrame(
        [
            ['block_square_red_Total', 'yards', 'square', 'red'],
            ['block_square_pink_Total', 'cm', 'square', 'pink'],
            ['block_circle_yellow_Total', 'inches', 'circle', 'yellow'],
            ['block_circle_blue_Total', 'ft', 'circle', 'blue'],

        ],
        ['metric_id', 'unit', 'dimension_1', 'dimension_2'])

    expected_df = spark_session.createDataFrame(
        [
            ['a', 'nhs-ccg', '1', '2021-04-03', 'block_square_red_Total', 'yards', 'data source', 'Y'],
            ['b', 'nhs-stp', '2', '2021-04-03', 'block_square_red_Total', 'yards', 'data source', 'N'],
            ['c', 'nhs-region', '3', '2021-04-03', 'block_square_pink_Total', 'cm', 'data source', 'Y'],
            ['d', 'nhs-country', '4', '2021-04-03', 'block_circle_yellow_Total', 'inches', 'data source', 'N'],
            ['e', 'somwhere else', '5', '2021-04-03', 'block_circle_blue_Total', 'ft', 'data source', 'Y']
        ],
        ['location_id', 'location_type', 'metric_value', 'timestamp', 'metric_id', 'unit', 'source', 'is_favourite']) \
        .withColumn('metric_value_12monthrolling', F.lit(None).cast('double')) \
        .withColumn('datapoint_id', F.concat_ws(
                '_',
                F.date_format(F.col('timestamp'), 'yyyy-MM-dd'),
                F.col('metric_id'),
                F.col('location_type'),
                F.col('location_id'),
                F.col('source')
                ))

    result_df = utils.generate_metric_data(df, metadata, 'block', 'the_date', ['shape', 'colour'], 'data source',
                                           True, ['is_favourite'])
    assert_df(expected_df, result_df)


def test_get_metrics(spark_session):

    synthetic = spark_session.createDataFrame(
        [
            ['GP', 'role1', 'detailed_role1', '17/10/2015', 1.0, 'england', 'Region1', 'STP1', 'CCG1'],
            ['GP', 'role1', 'detailed_role1', '17/10/2015', 1.0, 'england', 'Region2', 'STP3', 'CCG3'],
            ['GP', 'role1', 'detailed_role1', '18/10/2015', 2.0, 'england', 'Region1', 'STP1', 'CCG2'],
            ['GP', 'role1', 'detailed_role1', '18/10/2015', 5.0, 'england', 'Region1', 'STP2', 'CCG2'],
            ['GP', 'role1', 'detailed_role2', '18/10/2015', 5.0, 'england', 'Region1', 'STP1', 'CCG2'],
            ['GP', 'role1', 'detailed_role2', '18/10/2015', 3.0, 'england', 'Region1', 'STP2', 'CCG2'],
            ['GP', 'role1', 'detailed_role2', '18/10/2015', 4.0, 'england', 'Region1', 'STP2', 'CCG2'],
            ['GP', 'role1', 'detailed_role3', '17/10/2015', 3.0, 'england', 'Region1', 'STP1', 'CCG1'],
            ['GP', 'role1', 'detailed_role3', '17/10/2015', 3.0, 'england', 'Region2', 'STP3', 'CCG3'],
            ['GP', 'role2', 'detailed_role1', '18/10/2015', 4.0, 'england', 'Region1', 'STP1', 'CCG1'],
            ['GP', 'role2', 'detailed_role2', '17/10/2015', 2.0, 'england', 'Region1', 'STP1', 'CCG1'],
            ['GP', 'role2', 'detailed_role2', '17/10/2015', 2.0, 'england', 'Region2', 'STP3', 'CCG3'],
            ['GP', 'role2', 'detailed_role3', '18/10/2015', 1.0, 'england', 'Region1', 'STP1', 'CCG2'],
            ['Nurse', 'role3', 'detailed_role4', '17/10/2015', 4.0, 'england', 'Region2', 'STP4', 'CCG4'],
            ['Nurse', 'role3', 'detailed_role4', '17/10/2015', 5.0, 'england', 'Region2', 'STP4', 'CCG4'],
            ['Nurse', 'role4', 'detailed_role5', '17/10/2015', 1.0, 'england', 'Region2', 'STP4', 'CCG4']
        ],
        ['staff_group', 'staff_role', 'detailed_staff_role', 'effective_snapshot_date', 'fte', 'country_id',
         'nhs_region_id', 'stp_code', 'current_ccg_code']

    )

    expected_df = spark_session.createDataFrame(
        [
            ['GP', 'role1', 'detailed_role1', '17/10/2015', 1.0, 'CCG1', 'nhs-ccg'],
            ['GP', 'role1', 'detailed_role3', '17/10/2015', 3.0, 'CCG1', 'nhs-ccg'],
            ['GP', 'role2', 'detailed_role1', '18/10/2015', 4.0, 'CCG1', 'nhs-ccg'],
            ['GP', 'role2', 'detailed_role2', '17/10/2015', 2.0, 'CCG1', 'nhs-ccg'],
            ['GP', 'role1', 'detailed_role1', '18/10/2015', 7.0, 'CCG2', 'nhs-ccg'],
            ['GP', 'role1', 'detailed_role2', '18/10/2015', 12.0, 'CCG2', 'nhs-ccg'],
            ['GP', 'role2', 'detailed_role3', '18/10/2015', 1.0, 'CCG2', 'nhs-ccg'],
            ['GP', 'role1', 'detailed_role1', '17/10/2015', 1.0, 'CCG3', 'nhs-ccg'],
            ['GP', 'role1', 'detailed_role3', '17/10/2015', 3.0, 'CCG3', 'nhs-ccg'],
            ['GP', 'role2', 'detailed_role2', '17/10/2015', 2.0, 'CCG3', 'nhs-ccg'],
            ['Nurse', 'role3', 'detailed_role4', '17/10/2015', 9.0, 'CCG4', 'nhs-ccg'],
            ['Nurse', 'role4', 'detailed_role5', '17/10/2015', 1.0, 'CCG4', 'nhs-ccg'],
            ['GP', 'role1', 'detailed_role1', '17/10/2015', 1.0, 'STP1', 'nhs-stp'],
            ['GP', 'role1', 'detailed_role1', '18/10/2015', 2.0, 'STP1', 'nhs-stp'],
            ['GP', 'role1', 'detailed_role2', '18/10/2015', 5.0, 'STP1', 'nhs-stp'],
            ['GP', 'role1', 'detailed_role3', '17/10/2015', 3.0, 'STP1', 'nhs-stp'],
            ['GP', 'role2', 'detailed_role1', '18/10/2015', 4.0, 'STP1', 'nhs-stp'],
            ['GP', 'role2', 'detailed_role2', '17/10/2015', 2.0, 'STP1', 'nhs-stp'],
            ['GP', 'role2', 'detailed_role3', '18/10/2015', 1.0, 'STP1', 'nhs-stp'],
            ['GP', 'role1', 'detailed_role1', '18/10/2015', 5.0, 'STP2', 'nhs-stp'],
            ['GP', 'role1', 'detailed_role2', '18/10/2015', 7.0, 'STP2', 'nhs-stp'],
            ['GP', 'role1', 'detailed_role1', '17/10/2015', 1.0, 'STP3', 'nhs-stp'],
            ['GP', 'role1', 'detailed_role3', '17/10/2015', 3.0, 'STP3', 'nhs-stp'],
            ['GP', 'role2', 'detailed_role2', '17/10/2015', 2.0, 'STP3', 'nhs-stp'],
            ['Nurse', 'role3', 'detailed_role4', '17/10/2015', 9.0, 'STP4', 'nhs-stp'],
            ['Nurse', 'role4', 'detailed_role5', '17/10/2015', 1.0, 'STP4', 'nhs-stp'],
            ['GP', 'role1', 'detailed_role1', '17/10/2015', 1.0, 'Region1', 'nhs-region'],
            ['GP', 'role1', 'detailed_role1', '18/10/2015', 7.0, 'Region1', 'nhs-region'],
            ['GP', 'role1', 'detailed_role2', '18/10/2015', 12.0, 'Region1', 'nhs-region'],
            ['GP', 'role1', 'detailed_role3', '17/10/2015', 3.0, 'Region1', 'nhs-region'],
            ['GP', 'role2', 'detailed_role1', '18/10/2015', 4.0, 'Region1', 'nhs-region'],
            ['GP', 'role2', 'detailed_role2', '17/10/2015', 2.0, 'Region1', 'nhs-region'],
            ['GP', 'role2', 'detailed_role3', '18/10/2015', 1.0, 'Region1', 'nhs-region'],
            ['GP', 'role1', 'detailed_role1', '17/10/2015', 1.0, 'Region2', 'nhs-region'],
            ['GP', 'role1', 'detailed_role3', '17/10/2015', 3.0, 'Region2', 'nhs-region'],
            ['GP', 'role2', 'detailed_role2', '17/10/2015', 2.0, 'Region2', 'nhs-region'],
            ['Nurse', 'role3', 'detailed_role4', '17/10/2015', 9.0, 'Region2', 'nhs-region'],
            ['Nurse', 'role4', 'detailed_role5', '17/10/2015', 1.0, 'Region2', 'nhs-region'],
            ['GP', 'role1', 'detailed_role1', '17/10/2015', 2.0, 'england', 'nhs-country'],
            ['GP', 'role1', 'detailed_role1', '18/10/2015', 7.0, 'england', 'nhs-country'],
            ['GP', 'role1', 'detailed_role2', '18/10/2015', 12.0, 'england', 'nhs-country'],
            ['GP', 'role1', 'detailed_role3', '17/10/2015', 6.0, 'england', 'nhs-country'],
            ['GP', 'role2', 'detailed_role1', '18/10/2015', 4.0, 'england', 'nhs-country'],
            ['GP', 'role2', 'detailed_role2', '17/10/2015', 4.0, 'england', 'nhs-country'],
            ['GP', 'role2', 'detailed_role3', '18/10/2015', 1.0, 'england', 'nhs-country'],
            ['Nurse', 'role3', 'detailed_role4', '17/10/2015', 9.0, 'england', 'nhs-country'],
            ['Nurse', 'role4', 'detailed_role5', '17/10/2015', 1.0, 'england', 'nhs-country']
        ],
        ['staff_group', 'staff_role', 'detailed_staff_role', 'effective_snapshot_date',
         'metric_value', 'location_id', 'location_type']
    )

    metrics_calc = F.sum("fte")

    output_df = get_metrics(synthetic, date_col, dimension_cols, metrics_calc, loc_dict)

    assert_df(output_df, expected_df)


def test_fin_quarter_to_end_dates(spark_session):
    ref_dates = spark_session.createDataFrame(
        [
            ["30-06-2020", "1", "202021", "abc"],
            ["30-09-2020", "2", "202021", "dafd"],
            ["31-12-2020", "3", "202021", "jafld"],
            ["31-03-2021", "4", "202021", "mfad"],
            ["30-06-2021", "1", "202122", "moer"],
            ["30-09-2021", "2", "202122", "viioo"],
            ["30-09-2021", "2", "202122", "lalee"]
        ],
        ["quarter_end", "fin_quarter_no", "fin_year", "random_col"]
    )

    initial_df = spark_session.createDataFrame(
        [
            ["2020/21 Q1", 25, 3],
            ["2020/21 Q3", None, 77],
            ["2021/22 Q1", 50, 43],
            ["2021/22 Q2", 300, 90]
        ],
        ["year_and_quarter", "nwrs", "baseline"]
    )

    expected_df = spark_session.createDataFrame(
        [
            ["30-06-2020", "2020/21 Q1", 25, 3],
            ["31-12-2020", "2020/21 Q3", None, 77],
            ["30-06-2021", "2021/22 Q1", 50, 43],
            ["30-09-2021", "2021/22 Q2", 300, 90]
        ],
        ["quarter_end_date", "year_and_quarter", "nwrs", "baseline"],
    )

    result_df = utils.fin_quarter_to_end_dates(initial_df, ref_dates)

    assert_df(expected_df, result_df)


def test_generate_percentage_baseline(spark_session):
    initial_df = spark_session.createDataFrame(
        [
            ["C1", "S1", "R1", "2010/11 Q2", 100, 80],
            ["C2", "S2A", "R2", "2018/19 Q4", 50, 50],
            ["C3", "S3", "R3A", "2020/21 Q1", 24, 20],
            ["C4", "S4", "R4", "1995/96", 90, 100],
            ["#N/A", "#N/A", "#N/A", "2010/11 Q1", 8, 10]
        ],
        ["ccg_code", "ics_stp_code", "region_code", "year_and_quarter", "metric_value", "baseline"]
    )

    expected_df = spark_session.createDataFrame(
        [
            ["C1", "S1", "R1", "2010/11 Q2", 100, 80, 20/80],
            ["C2", "S2A", "R2", "2018/19 Q4", 50, 50, 0/50],
            ["C3", "S3", "R3A", "2020/21 Q1", 24, 20, 4/20],
            ["C4", "S4", "R4", "1995/96", 90, 100, -10/100],
            ["#N/A", "#N/A", "#N/A", "2010/11 Q1", 8, 10, -2/10]
        ],
        ["ccg_code", "ics_stp_code", "region_code", "year_and_quarter", "metric_value", "baseline",
         "percentage_baseline"]
    )

    result_df = generate_percentage_baseline(initial_df)

    assert_df(expected_df, result_df)
