from nhs_foundry_common.test_utils import assert_df
import myproject.datasets.direct_patient_care_fte.dpc_utils as dpc_utils


def test_join_locations(spark_session):

    ccg = spark_session.createDataFrame(
        [
            ["C1", "C2", "S1", "R1", "england"],
            ["C2", "C2", "S2", "R2", "england"],
            ["C3", "C3", "S3", "R3", "england"],
            ["C4", "C4", "S4", "R4", "england"],
            ["C5", "C5", "S5", "R5", "england"]
        ],
        ["ccg_code", "current_ccg_code", "stp_code", "nhs_region_id", "country_id"]
    )

    initial_df = spark_session.createDataFrame(
        [
            ["C1", "S1", "R1", "2010/11 Q2", 100],
            ["C2", "S2A", "R2", "2018/19 Q4", 70],
            ["C3", "S3", "R3A", "2020/21 Q1", 24],
            ["C4", "S4", "R4", "1995/96", 900],
            ["#N/A", "#N/A", "#N/A", "2010/11 Q1", 8]
        ],
        ["ccg_code", "ics_stp_code", "region_code", "year_and_quarter", "nwrs"]
    )

    expected_df = spark_session.createDataFrame(
        [
            ["C1", "C2", "S1", "R1", "england", "S1", "R1", "2010/11 Q2", 100],
            ["C2", "C2", "S2", "R2", "england", "S2A", "R2", "2018/19 Q4", 70],
            ["C3", "C3", "S3", "R3", "england", "S3", "R3A", "2020/21 Q1", 24],
            ["C4", "C4", "S4", "R4", "england", "S4", "R4", "1995/96", 900],
            ["#N/A", "Unaligned", "Unaligned", "Unaligned", "england", "#N/A", "#N/A", "2010/11 Q1", 8]
        ],
        ["ccg_code", "current_ccg_code", "stp_code", "nhs_region_id", "country_id",
         "ics_stp_code", "region_code", "year_and_quarter", "nwrs"
         ],
    )

    result_df = dpc_utils.join_location(ccg, initial_df, "ccg_code", spark_session)

    assert_df(expected_df, result_df)


def test_generate_progress(spark_session):
    dim_1 = "SQUARE"

    df = spark_session.createDataFrame(
        [
            ['england', 'SQUARE', 'Total', None, None, 2, 1, 'a'],
            ['england', 'SQUARE', 'Total', None, None, 2, 2, 'a'],
            ['england', 'SQUARE', 'Total', None, None, 2, -3, 'a'],
            ['y', 'SQUARE', 'Total', None, None, 0, 1, 'a'],
            ['england', 'SQUARE', 'Total', 'smth', None, 100, 51, 'a'],
            ['england', 'SQUARE', 'Total', 'smth', 'smth', 200, 52, 'a'],
            ['england', 'triangle', 'Total', None, None, 300, 53, 'a']
        ],
        ['location_id', 'dimension_1', 'dimension_2', 'dimension_3', 'dimension_4', 'baseline', 'metric_value', 'other']
)

    expected_df = spark_session.createDataFrame(
        [
            ['england', 'SQUARE', 'Total', None, None, 2, 1, 'a', 26001],
            ['england', 'SQUARE', 'Total', None, None, 2, 2, 'a', 26000],
            ['england', 'SQUARE', 'Total', None, None, 2, -3, 'a', 26005],
            ['y', 'SQUARE', 'Total', None, None, 0, 1, 'a', None],
            ['england', 'SQUARE', 'Total', 'smth', None, 100, 51, 'a', None],
            ['england', 'SQUARE', 'Total', 'smth', 'smth', 200, 52, 'a', None],
            ['england', 'triangle', 'Total', None, None, 300, 53, 'a', None]
        ],
        ['location_id', 'dimension_1', 'dimension_2', 'dimension_3', 'dimension_4', 'baseline', 'metric_value', 'other', 'progress_towards_target_value'])

    result_df = dpc_utils.generate_progress(df, dim_1)

    assert_df(expected_df, result_df)
