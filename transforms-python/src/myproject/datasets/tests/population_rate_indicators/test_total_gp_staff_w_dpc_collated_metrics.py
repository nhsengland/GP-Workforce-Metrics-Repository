from pyspark.sql import functions as f, types as T
from nhs_foundry_common.test_utils import assert_df
from myproject.datasets.population_rate_indicators.total_gp_staff_w_dpc_collated_metrics \
    import compute_metrics_and_metadata as total_gp_compute, prepare_source_dataset


def test_compute(spark_session):
    gp_fte_df = spark_session.createDataFrame(
        [
            ['GP_FTE_GP_Total', 'nhs-ccg', 'a', '2020-06-30', 1.00, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-ccg', 'a', '2020-06-30', 2.00, 'x'],
            ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'nhs-ccg', 'a', '2020-06-30', 3.00, 'x'],

            ['GP_FTE_GP_Total', 'nhs-ccg', 'a', '2020-09-30', 4.00, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-ccg', 'a', '2020-09-30', 5.00, 'x'],
            ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'nhs-ccg', 'a', '2020-09-30', 6.00, 'x'],

            ['GP_FTE_GP_Total', 'nhs-ccg', 'b', '2020-06-30', 7.00, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-ccg', 'b', '2020-06-30', 8.00, 'x'],
            ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'nhs-ccg', 'b', '2020-06-30', 9.00, 'x'],

            ['GP_FTE_GP_Total', 'nhs-ccg', 'b', '2020-09-30', 10.00, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-ccg', 'b', '2020-09-30', 11.00, 'x'],
            ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'nhs-ccg', 'b', '2020-09-30', 12.00, 'x'],

            ['GP_FTE_GP_Total', 'nhs-stp', 'c', '2020-06-30', 101.00, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-stp', 'c', '2020-06-30', 102.00, 'x'],
            ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'nhs-stp', 'c', '2020-06-30', 103.00, 'x'],

            ['GP_FTE_GP_Total', 'nhs-region', 'd', '2020-06-30', 1001.00, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-region', 'd', '2020-06-30', 1002.00, 'x'],
            ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'nhs-region', 'd', '2020-06-30', 1003.00, 'x'],

            ['GP_FTE_GP_Total', 'nhs-country', 'england', '2020-06-30', 10001.00, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-country', 'england', '2020-06-30', 10002.00, 'x'],
            ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'nhs-country', 'england', '2020-06-30', 10003.00, 'x'],

            # Entry with only nwrs match, not uplift
            ['GP_FTE_GP_Total', 'nhs-country', 'england', '2016-06-30', 33.00, 'x'],
            # Entry without a match (date)
            ['GP_FTE_GP_Total', 'nhs-country', 'england', '2015-06-30', 25.00, 'x']
        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'other']
    )

    dpc_collated_df = spark_session.createDataFrame(
        [
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'a', '2020-06-30', 0.10, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'a', '2020-09-30', 0.20, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'b', '2020-06-30', 0.30, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'b', '2020-09-30', 0.40, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-stp', 'c', '2020-06-30', 0.50, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-region', 'd', '2020-06-30', 0.60, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-country', 'england', '2020-06-30', 0.70, 'x'],

            # Entry without a match (location_id)
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'c', '2020-09-30', 0.80, 'x']
        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'other']
    )

    expected_metrics_df = spark_session.createDataFrame(
        [
            ['GP_FTE_w_DPC_Collated_Total (inc. DPC – Collated figure)', 'nhs-ccg', 'a', '2020-06-30',
             1.0+2.0+3.0+0.1, 'FTE', 'TBC', None],
            ['GP_FTE_w_DPC_Collated_Total (inc. DPC – Collated figure)', 'nhs-ccg', 'a', '2020-09-30',
             4.0+5.0+6.0+0.2, 'FTE', 'TBC', 1.0+2.0+3.0+0.1],
            ['GP_FTE_w_DPC_Collated_Total (inc. DPC – Collated figure)', 'nhs-ccg', 'b', '2020-06-30',
             7.0+8.0+9.0+0.3, 'FTE', 'TBC', None],
            ['GP_FTE_w_DPC_Collated_Total (inc. DPC – Collated figure)', 'nhs-ccg', 'b', '2020-09-30',
             10.0+11.0+12.0+0.4, 'FTE', 'TBC', 7.0+8.0+9.0+0.3],
            ['GP_FTE_w_DPC_Collated_Total (inc. DPC – Collated figure)', 'nhs-stp', 'c', '2020-06-30',
             101.0+102.0+103.0+0.5, 'FTE', 'TBC', None],
            ['GP_FTE_w_DPC_Collated_Total (inc. DPC – Collated figure)', 'nhs-region', 'd', '2020-06-30',
             1001.0+1002.0+1003.0+0.6, 'FTE', 'TBC', None],
            ['GP_FTE_w_DPC_Collated_Total (inc. DPC – Collated figure)', 'nhs-country', 'england', '2020-06-30',
             10001.0+10002.0+10003.0+0.7, 'FTE', 'TBC', None]

        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'unit', 'source',
         'metric_value_prev_quarter']
    ).withColumn('datapoint_id', f.concat_ws(
                '_',
                f.date_format(f.col('timestamp'), 'yyyy-MM-dd'),
                f.col('metric_id'),
                f.col('location_type'),
                f.col('location_id'),
                f.col('source')
                )) \
     .withColumn("metric_value_12monthrolling", f.lit(None).cast(T.DoubleType()))

    result_metrics_df, result_metadata_df = total_gp_compute(gp_fte_df, dpc_collated_df)

    assert_df(expected_metrics_df, result_metrics_df)


def test_prepare_source_dataset(spark_session):
    alias = "new"
    required_cols = ["shapes"]
    required_metric_ids = ["m1"]

    df = spark_session.createDataFrame(
        [
            ['m1',  '1',  'triangle',  'pink'],
            ['m1',  '2',  'oval',  'purple'],
            ['m2',  '3',  'square',  'yellow']
        ],
        ['metric_id',  'metric_value',  'shapes',  'colours'])

    expected_df = spark_session.createDataFrame(
        [
            ['m1',  '1',  'triangle'],
            ['m1',  '2',  'oval']
        ],
        ['metric_id_new',  'metric_value_new',  'shapes'])

    result_df = prepare_source_dataset(alias, df, required_cols, required_metric_ids)

    assert_df(expected_df, result_df)
