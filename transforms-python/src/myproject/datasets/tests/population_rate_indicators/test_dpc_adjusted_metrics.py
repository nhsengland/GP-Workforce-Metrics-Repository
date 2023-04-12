from pyspark.sql import functions as f, types as T
from nhs_foundry_common.test_utils import assert_df
from myproject.datasets.population_rate_indicators.dpc_collated_metrics import compute_metrics_and_metadata


def test_compute(spark_session):

    dpc_nwrs_df = spark_session.createDataFrame(
        [
            ['DPC_FTE_NWRS_DPC NWRS_Total', 'nhs-ccg', 'a', '2020-06-30', 0.01, 'x'],
            ['DPC_FTE_NWRS_DPC NWRS_Total', 'nhs-ccg', 'a', '2020-09-30', 0.02, 'x'],
            ['DPC_FTE_NWRS_DPC NWRS_Total', 'nhs-ccg', 'b', '2020-06-30', 0.03, 'x'],
            ['DPC_FTE_NWRS_DPC NWRS_Total', 'nhs-ccg', 'b', '2020-09-30', 0.04, 'x'],
            ['DPC_FTE_NWRS_DPC NWRS_Total', 'nhs-stp', 'c', '2020-06-30', 0.05, 'x'],
            ['DPC_FTE_NWRS_DPC NWRS_Total', 'nhs-region', 'd', '2020-06-30', 0.06, 'x'],
            ['DPC_FTE_NWRS_DPC NWRS_Total', 'nhs-country', 'england', '2020-06-30', 0.07, 'x'],

            # Entry without a match (location_id)
            ['DPC_FTE_NWRS_DPC NWRS_Total', 'nhs-ccg', 'c', '2020-09-30', 0.09, 'x']
        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'other']
    )

    dpc_uplift_df = spark_session.createDataFrame(
        [
            ['DPC_FTE_Uplift_from_Collated_DPC Uplift from Collated_Total', 'nhs-ccg', 'a', '2020-06-30', 0.10, 'x'],
            ['DPC_FTE_Uplift_from_Collated_DPC Uplift from Collated_Total', 'nhs-ccg', 'a', '2020-09-30', 0.20, 'x'],
            ['DPC_FTE_Uplift_from_Collated_DPC Uplift from Collated_Total', 'nhs-ccg', 'b', '2020-06-30', 0.30, 'x'],
            ['DPC_FTE_Uplift_from_Collated_DPC Uplift from Collated_Total', 'nhs-ccg', 'b', '2020-09-30', 0.40, 'x'],
            ['DPC_FTE_Uplift_from_Collated_DPC Uplift from Collated_Total', 'nhs-stp', 'c', '2020-06-30', 0.50, 'x'],
            ['DPC_FTE_Uplift_from_Collated_DPC Uplift from Collated_Total', 'nhs-region', 'd', '2020-06-30', 0.60, 'x'],
            ['DPC_FTE_Uplift_from_Collated_DPC Uplift from Collated_Total', 'nhs-country', 'england',
             '2020-06-30', 0.70, 'x'],

            # Entry without a match (location_id)
            ['DPC_FTE_Uplift_from_Collated_DPC Uplift from Collated_Total', 'nhs-region', 'x', '2020-06-30', 0.80, 'x'],
        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'other'],
    )

    expected_metrics_df = spark_session.createDataFrame(
        [
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'a', '2020-06-30',
             0.01+0.1, 'FTE', 'TBC'],
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'a', '2020-09-30',
             0.02+0.2, 'FTE', 'TBC'],
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'b', '2020-06-30',
             0.03+0.3, 'FTE', 'TBC'],
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'b', '2020-09-30',
             0.04+0.4, 'FTE', 'TBC'],
            ['GP_FTE_DPC Collated', 'nhs-stp', 'c', '2020-06-30',
             0.05+0.5, 'FTE', 'TBC'],
            ['GP_FTE_DPC Collated', 'nhs-region', 'd', '2020-06-30',
             0.06+0.6, 'FTE', 'TBC'],
            ['GP_FTE_DPC Collated', 'nhs-country', 'england', '2020-06-30',
             0.07+0.7, 'FTE', 'TBC']

        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'unit', 'source']
    ).withColumn('datapoint_id', f.concat_ws(
                '_',
                f.date_format(f.col('timestamp'), 'yyyy-MM-dd'),
                f.col('metric_id'),
                f.col('location_type'),
                f.col('location_id'),
                f.col('source')
                )) \
     .withColumn("metric_value_12monthrolling", f.lit(None).cast(T.DoubleType()))

    result_metrics_df, result_metadata_df = compute_metrics_and_metadata(dpc_nwrs_df, dpc_uplift_df)

    assert_df(expected_metrics_df, result_metrics_df)
