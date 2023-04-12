from myproject.datasets.fte.gp_fte_metrics import (perform_aggregation, generate_baseline,
                                                   generate_target, generate_progress,
                                                   join_locations, generate_12m_baseline)
from nhs_foundry_common.test_utils import assert_df
from datetime import datetime


def test_aggregation(spark_session):

    dimension_cols = [
      'staff_group',
      'staff_role',
      'detailed_staff_role']

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

    output_df = perform_aggregation(synthetic, dimension_cols).drop('metric_value_prev_year', 'metric_value_prev_month')

    assert_df(output_df, expected_df)


def test_generate_baseline(spark_session):

    random_string = 87238944895.0

    input_df = spark_session.createDataFrame(
        [
            ['GP_role1_detailed_role1', '2019-03-30', 1.0, 'CCG1', 'nhs-ccg'],
            ['GP_role1_detailed_role1', '2019-03-31', 2.0, 'CCG1', 'nhs-ccg'],  # this is the baseline
            ['GP_role1_detailed_role1', '2019-04-01', 3.0, 'CCG1', 'nhs-ccg'],
            ['DPC_role2_detailed_role4', '2020-03-30', 4.0, 'CCG4', 'nhs-ccg'],
            ['DPC_role2_detailed_role4', '2020-03-31', 5.0, 'CCG4', 'nhs-ccg'],  # no baseline
            ['DPC_role2_detailed_role4', '2020-04-01', 6.0, 'CCG4', 'nhs-ccg'],
            ['GP_role1_detailed_role1', '2019-03-31', 7.0, 'england', 'nhs-country'],  # this is the baseline
            ['GP_role1_detailed_role1', '2019-04-01', 8.0, 'england', 'nhs-country'],
            ['GP_role1_detailed_role1', '2019-05-01', 9.0, 'england', 'nhs-country']

        ],
        ['metric_id', 'timestamp', 'metric_value', 'location_id', 'location_type']
    )

    expected_df = spark_session.createDataFrame(
        [
            ['GP_role1_detailed_role1', '2019-03-30', 1.0, 'CCG1', 'nhs-ccg', 2.0],
            ['GP_role1_detailed_role1', '2019-03-31', 2.0, 'CCG1', 'nhs-ccg', 2.0],
            ['GP_role1_detailed_role1', '2019-04-01', 3.0, 'CCG1', 'nhs-ccg', 2.0],

            ['DPC_role2_detailed_role4', '2020-03-30', 4.0, 'CCG4', 'nhs-ccg', None],
            ['DPC_role2_detailed_role4', '2020-03-31', 5.0, 'CCG4', 'nhs-ccg', None],  # no baseline
            ['DPC_role2_detailed_role4', '2020-04-01', 6.0, 'CCG4', 'nhs-ccg', None],

            # same metric ID but at a different aggregation level
            ['GP_role1_detailed_role1', '2019-03-31', 7.0, 'england', 'nhs-country', 7.0],
            ['GP_role1_detailed_role1', '2019-04-01', 8.0, 'england', 'nhs-country', 7.0],
            ['GP_role1_detailed_role1', '2019-05-01', 9.0, 'england', 'nhs-country', 7.0],

        ],
        ['metric_id', 'timestamp', 'metric_value', 'location_id', 'location_type', 'baseline']
    ).fillna(random_string)

    output_df = generate_baseline(input_df).fillna(random_string)

    assert_df(output_df, expected_df)


def test_generate_target(spark_session):

    random_number = 454176972
    random_string = 'q3acklmsw4908acc24m908'

    input_df = spark_session.createDataFrame(
        [
            ['GP_role1_detailed_role1', '2019-03-30', 1.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None],
            ['GP_role1_detailed_role1', '2019-03-31', 2.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None],
            ['GP_role1_detailed_role1', '2019-04-01', 3.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None],
            ['DPC_role2_detailed_role4', '2020-03-30', 4.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'Total', None],
            ['DPC_role2_detailed_role4', '2020-03-31', 5.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'Total', None],
            ['DPC_role2_detailed_role4', '2020-04-01', 6.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'role1', 'sub'],
            ['GP', '2019-03-31', 7.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None],
            ['GP_role1_detailed_role1', '2019-04-01', 8.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'role1', 'Total'],
            ['GP', '2019-05-01', 9.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None],
        ],
        ['metric_id', 'timestamp', 'metric_value', 'location_id', 'location_type', 'baseline',
         'dimension_1', 'dimension_2', 'dimension_3']
    )

    expected_df = spark_session.createDataFrame(
        [
            ['GP_role1_detailed_role1', '2019-03-30', 1.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None, None],
            ['GP_role1_detailed_role1', '2019-03-31', 2.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None, None],
            ['GP_role1_detailed_role1', '2019-04-01', 3.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None, None],
            ['DPC_role2_detailed_role4', '2020-03-30', 4.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'Total', None, None],
            ['DPC_role2_detailed_role4', '2020-03-31', 5.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'Total', None, None],
            ['DPC_role2_detailed_role4', '2020-04-01', 6.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'role1', 'sub', None],
            ['GP', '2019-03-31', 7.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None, 6000],
            ['GP_role1_detailed_role1', '2019-04-01', 8.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'role1', 'Total', None],
            ['GP', '2019-05-01', 9.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None, 6000],
        ],
        ['metric_id', 'timestamp', 'metric_value', 'location_id', 'location_type', 'baseline',
         'dimension_1', 'dimension_2', 'dimension_3', 'target_value']
    ).fillna(random_number).fillna(random_string)

    output_df = generate_target(input_df).fillna(random_number).fillna(random_string)

    assert_df(output_df, expected_df)


def test_generate_progress(spark_session):

    random_number = 234921348
    random_string = 'cj058d903j894dj834j590'

    input_df = spark_session.createDataFrame(
        [
            ['GP_role1_detailed_role1', '2019-03-30', 1.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None],
            ['GP_role1_detailed_role1', '2019-03-31', 2.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None],
            ['GP_role1_detailed_role1', '2019-04-01', 3.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None],
            ['DPC_role2_detailed_role4', '2020-03-30', 4.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'Total', None],
            ['DPC_role2_detailed_role4', '2020-03-31', 5.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'Total', None],
            ['DPC_role2_detailed_role4', '2020-04-01', 6.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'role1', 'sub'],
            ['GP', '2019-03-31', 7.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None],
            ['GP_role1_detailed_role1', '2019-04-01', 8.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'role1', 'Total'],
            ['GP', '2019-05-01', 4.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None],
            ['GP', '2019-06-01', 9.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None]
        ],
        ['metric_id', 'timestamp', 'metric_value', 'location_id', 'location_type', 'baseline',
         'dimension_1', 'dimension_2', 'dimension_3']
    )

    expected_df = spark_session.createDataFrame(
        [
            ['GP_role1_detailed_role1', '2019-03-30', 1.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None, None],
            ['GP_role1_detailed_role1', '2019-03-31', 2.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None, None],
            ['GP_role1_detailed_role1', '2019-04-01', 3.0, 'CCG1', 'nhs-ccg', 2.0, 'GP - PRACTICE', 'Total', None, None],
            ['DPC_role2_detailed_role4', '2020-03-30', 4.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'Total', None, None],
            ['DPC_role2_detailed_role4', '2020-03-31', 5.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'Total', None, None],
            ['DPC_role2_detailed_role4', '2020-04-01', 6.0, 'CCG4', 'nhs-ccg', None, 'DPC', 'role1', 'sub', None],
            ['GP', '2019-03-31', 7.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None, 6000],
            ['GP_role1_detailed_role1', '2019-04-01', 8.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'role1', 'Total', None],
            ['GP', '2019-05-01', 4.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None, 6003],
            ['GP', '2019-06-01', 9.0, 'england', 'nhs-country', 7.0, 'GP - PRACTICE', 'Total', None, 5998],
        ],
        ['metric_id', 'timestamp', 'metric_value', 'location_id', 'location_type', 'baseline',
         'dimension_1', 'dimension_2', 'dimension_3', 'target_value']
    ).fillna(random_number).fillna(random_string)

    output_df = generate_progress(input_df).fillna(random_number).fillna(random_string)

    assert_df(output_df, expected_df)


def test_ccg_join(spark_session):

    ccgs = spark_session.createDataFrame(
        [
            ['1', '1', 'ccg_1', 'R1', 'reg_1', 'S1', 'stp_1', 'England'],
            ['2', '2', 'ccg_2', 'R1', 'reg_1', 'S1', 'stp_1', 'England'],
            ['3', '3', 'ccg_3', 'R2', 'reg_2', 'S2', 'stp_2', 'England'],
        ],
        ['ccg_code', 'current_ccg_code', 'current_ccg_name', 'nhs_region_id', 'nhs_region_name',
         'stp_code', 'stp_name', 'country_id']
    )

    df = spark_session.createDataFrame(
        [
            ['a', '1'],
            ['b', '2'],
            ['c', '3'],
            ['d', '4'],
        ],
        ['id', 'commissioner_code']
    )

    expected_df = spark_session.createDataFrame(
        [
            ['a', '1', '1', 'ccg_1', 'R1', 'reg_1', 'S1', 'stp_1', 'England'],
            ['b', '2', '2', 'ccg_2', 'R1', 'reg_1', 'S1', 'stp_1', 'England'],
            ['c', '3', '3', 'ccg_3', 'R2', 'reg_2', 'S2', 'stp_2', 'England'],
            ['d', '4', None, None, None, None, None, None, None],
        ],
        ['id', 'ccg_code', 'current_ccg_code', 'current_ccg_name', 'nhs_region_id', 'nhs_region_name',
         'stp_code', 'stp_name', 'country_id']
    )

    joined = join_locations(df, ccgs, spark_session)
    assert_df(joined, expected_df)


def test_12m_baseline(spark_session):
    # Test calculating 12 month baseline sum

    initial_df = spark_session.createDataFrame(
        [
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-01-31', "%Y-%m-%d"), 0.2],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-02-28', "%Y-%m-%d"), 0.6],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-03-31', "%Y-%m-%d"), 1.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-04-01', "%Y-%m-%d"), 1.5],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-04-30', "%Y-%m-%d"), 2.0],
            ['abc', 'nhs-ccg', 'metric1', datetime.strptime('2018-05-31', "%Y-%m-%d"), 15.0],  # different location_id
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-06-30', "%Y-%m-%d"), 8.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-05-31', "%Y-%m-%d"), 5.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-06-30', "%Y-%m-%d"), 8.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-07-31', "%Y-%m-%d"), 11.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-08-31', "%Y-%m-%d"), 14.0],
            ['xyz', 'nhs-ccg', 'metric2', datetime.strptime('2018-08-31', "%Y-%m-%d"), 9.0],  # different metric_id
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-09-30', "%Y-%m-%d"), 15.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-10-31', "%Y-%m-%d"), 17.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-11-30', "%Y-%m-%d"), 22.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-12-31', "%Y-%m-%d"), 25.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-01-31', "%Y-%m-%d"), 29.0],
            ['xyz', 'nhs-stp', 'metric1', datetime.strptime('2019-01-31', "%Y-%m-%d"), 29.0],  # different location_type
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-02-28', "%Y-%m-%d"), 32.0],
            ['xyz', 'nhs-stp', 'metric1', datetime.strptime('2019-02-28', "%Y-%m-%d"), 12.0],  # different location_type
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-03-31', "%Y-%m-%d"), 35.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-04-30', "%Y-%m-%d"), 39.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-05-31', "%Y-%m-%d"), 42.0],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-06-30', "%Y-%m-%d"), 48.0]
        ],
        ['location_id', 'location_type', 'metric_id', 'timestamp', 'metric_value']
        )

    baseline_12m_sum = 224.5

    expected_df = spark_session.createDataFrame(
        [
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-01-31', "%Y-%m-%d"), 0.2, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-02-28', "%Y-%m-%d"), 0.6, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-03-31', "%Y-%m-%d"), 1.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-04-01', "%Y-%m-%d"), 1.5, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-04-30', "%Y-%m-%d"), 2.0, baseline_12m_sum],
            ['abc', 'nhs-ccg', 'metric1', datetime.strptime('2018-05-31', "%Y-%m-%d"), 15.0, 15.0],  # diff loc id
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-06-30', "%Y-%m-%d"), 8.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-05-31', "%Y-%m-%d"), 5.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-06-30', "%Y-%m-%d"), 8.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-07-31', "%Y-%m-%d"), 11.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-08-31', "%Y-%m-%d"), 14.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric2', datetime.strptime('2018-08-31', "%Y-%m-%d"), 9.0, 9.0],  # different metric_id
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-09-30', "%Y-%m-%d"), 15.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-10-31', "%Y-%m-%d"), 17.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-11-30', "%Y-%m-%d"), 22.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2018-12-31', "%Y-%m-%d"), 25.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-01-31', "%Y-%m-%d"), 29.0, baseline_12m_sum],
            ['xyz', 'nhs-stp', 'metric1', datetime.strptime('2019-01-31', "%Y-%m-%d"), 29.0, 41.0],  # diff loc type
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-02-28', "%Y-%m-%d"), 32.0, baseline_12m_sum],
            ['xyz', 'nhs-stp', 'metric1', datetime.strptime('2019-02-28', "%Y-%m-%d"), 12.0, 41.0],  # diff loc type
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-03-31', "%Y-%m-%d"), 35.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-04-30', "%Y-%m-%d"), 39.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-05-31', "%Y-%m-%d"), 42.0, baseline_12m_sum],
            ['xyz', 'nhs-ccg', 'metric1', datetime.strptime('2019-06-30', "%Y-%m-%d"), 48.0, baseline_12m_sum]
        ],
        ['location_id', 'location_type', 'metric_id', 'timestamp', 'metric_value', 'baseline_12m_sum']
        )

    result_df = generate_12m_baseline(initial_df)

    assert_df(result_df, expected_df)
