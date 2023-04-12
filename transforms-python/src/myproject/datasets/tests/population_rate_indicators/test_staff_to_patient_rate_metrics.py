from pyspark.sql import functions as f, types as T
from nhs_foundry_common.test_utils import assert_df
from myproject.datasets.population_rate_indicators.staff_to_patient_rate_metrics import compute_metrics_and_metadata


def test_compute(spark_session):

    random_string = "sakdjgsaose"

    gp_fte_df = spark_session.createDataFrame(
        [
            # Irrelevant entries
            ['GP_FTE_Total', 'nhs-ccg', 'ccg1', '2020-03-31', 1.03, 'x'],
            ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'nhs-stp', 'stp1', '2020-03-31', 1.04, 'x'],
            ['GP_FTE_DIRECT PATIENT CARE_Total', 'nhs-region', 'r1', '2020-03-31', 1.05, 'x'],
            ['GP_FTE_GP_SALARIED GPS_Total', 'nhs-country', 'england', '2020-03-31', 1.06, 'x'],
            ['GP_FTE_NURSES_PRACTICE NURSES_PRACTICE NURSE', 'nhs-ccg', 'ccg1', '2020-03-31', 1.07, 'x'],
            ['other', 'nhs-stp', 'stp1', '2020-03-31', 1.08, 'x'],

            # Relevant GP entries

            ['GP_FTE_GP_Total', 'nhs-ccg', 'ccg1', '2020-03-31', 10.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-stp', 'stp1', '2020-03-31', 20.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-region', 'r1', '2020-03-31', 30.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-country', 'england', '2020-03-31', 40.0, 'x'],

            # same locations, different timestamp
            ['GP_FTE_GP_Total', 'nhs-ccg', 'ccg1', '2020-06-30', 1000.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-stp', 'stp1', '2020-06-30', 2000.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-region', 'r1', '2020-06-30', 3000.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-country', 'england', '2020-06-30', 4000.0, 'x'],

            # different locations, same timestamp
            ['GP_FTE_GP_Total', 'nhs-ccg', 'ccg2', '2020-06-30', 99.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-stp', 'stp2', '2020-06-30', 108.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-region', 'r2', '2020-06-30', 117.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-country', 'not england', '2020-06-30', 126.0, 'x'],

            # unexpected timestamp
            # ['GP_FTE_GP_Total', 'nhs-ccg', 'ccg1', '2020-03-31', 1.0, ''],

            # unknown locations
            ['GP_FTE_GP_Total', 'nhs-ccg', 'Unknown', '2020-03-31', 1.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-stp', 'Unknown', '2020-03-31', 1.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-region', 'Unknown', '2020-03-31', 1.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-country', 'Unknown', '2020-03-31', 1.0, 'x'],

            # same location code, different location type
            ['GP_FTE_GP_Total', 'nhs-ccg', 'a', '2020-03-31', 222.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-stp', 'a', '2020-03-31', 333.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-region', 'a', '2020-03-31', 444.0, 'x'],
            ['GP_FTE_GP_Total', 'nhs-country', 'a', '2020-03-31', 555.0, 'x'],

            # null location code
            ['GP_FTE_GP_Total', 'nhs-ccg', None, '2020-03-31', 20.5, 'x'],
            ['GP_FTE_GP_Total', 'nhs-stp', None, '2020-03-31', 20.5, 'x'],
            ['GP_FTE_GP_Total', 'nhs-region', None, '2020-03-31', 20.5, 'x'],
            ['GP_FTE_GP_Total', 'nhs-country', None, '2020-03-31', 20.5, 'x'],

            # unmatched location code
            ['GP_FTE_GP_Total', 'nhs-ccg', 'b', '2020-03-31', 20.5, 'x'],
            ['GP_FTE_GP_Total', 'nhs-stp', 'b', '2020-03-31', 20.5, 'x'],
            ['GP_FTE_GP_Total', 'nhs-region', 'b', '2020-03-31', 20.5, 'x'],
            ['GP_FTE_GP_Total', 'nhs-country', 'b', '2020-03-31', 20.5, 'x'],

            # Relevant Nurses entries

            ['GP_FTE_NURSES_Total', 'nhs-ccg', 'ccg1', '2020-03-31', 10.0, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-stp', 'stp1', '2020-03-31', 20.0, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-region', 'r1', '2020-03-31', 30.0, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-country', 'england', '2020-03-31', 40.0, 'x'],

            # same locations, different timestamp
            ['GP_FTE_NURSES_Total', 'nhs-ccg', 'ccg1', '2020-06-30', 1000.0, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-stp', 'stp1', '2020-06-30', 2000.0, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-region', 'r1', '2020-06-30', 3000.0, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-country', 'england', '2020-06-30', 4000.0, 'x'],

            # different locations, same timestamp
            ['GP_FTE_NURSES_Total', 'nhs-ccg', 'ccg2', '2020-06-30', 99.0, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-stp', 'stp2', '2020-06-30', 108.0, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-region', 'r2', '2020-06-30', 117.0, 'x'],
            ['GP_FTE_NURSES_Total', 'nhs-country', 'not england', '2020-06-30', 126.0, 'x']
        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'other']
    )

    gp_fte_metadata_df = spark_session.createDataFrame(
        [
            # Irrelevant entries
            ['GP_FTE_Total', 'Total', None, None, 'z'],
            ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'ADMIN/NON-CLINICAL', 'Total', None, 'z'],
            ['GP_FTE_DIRECT PATIENT CARE_Total', 'DIRECT PATIENT CARE', 'Total', None, 'z'],
            ['GP_FTE_GP_SALARIED GPS_Total', 'GP', 'SALARIED GPS', 'Total', 'z'],
            ['GP_FTE_NURSES_PRACTICE NURSES_PRACTICE NURSE', 'NURSES', 'PRACTICE NURSES', 'PRACTICE NURSE', 'z'],
            ['other', None, None, None, 'z'],

            # Relevant entries
            ['GP_FTE_GP_Total', 'GP - PRACTICE', 'Total', None, 'z'],
            ['GP_FTE_NURSES_Total', 'NURSES - PRACTICE', 'Total', None, 'z']
        ],
        ['metric_id', 'dimension_1', 'dimension_2', 'dimension_3', 'other']
    )

    dpc_collated_df = spark_session.createDataFrame(
        [
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'ccg1', '2020-03-31', 10.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-stp', 'stp1', '2020-03-31', 20.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-region', 'r1', '2020-03-31', 30.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-country', 'england', '2020-03-31', 40.0, 'x'],

            # same locations, different timestamp
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'ccg1', '2020-06-30', 1000.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-stp', 'stp1', '2020-06-30', 2000.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-region', 'r1', '2020-06-30', 3000.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-country', 'england', '2020-06-30', 4000.0, 'x'],

            # different locations, same timestamp
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'ccg2', '2020-06-30', 99.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-stp', 'stp2', '2020-06-30', 108.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-region', 'r2', '2020-06-30', 117.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-country', 'not england', '2020-06-30', 126.0, 'x'],

            # unexpected timestamp
            # ['GP_FTE_DPC Collated', 'nhs-ccg', 'ccg1', '2020-03-31', 1.0, ''],

            # unknown locations
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'Unknown', '2020-03-31', 1.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-stp', 'Unknown', '2020-03-31', 1.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-region', 'Unknown', '2020-03-31', 1.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-country', 'Unknown', '2020-03-31', 1.0, 'x'],

            # same location code, different location type
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'a', '2020-03-31', 222.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-stp', 'a', '2020-03-31', 333.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-region', 'a', '2020-03-31', 444.0, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-country', 'a', '2020-03-31', 555.0, 'x'],

            # null location code
            ['GP_FTE_DPC Collated', 'nhs-ccg', None, '2020-03-31', 20.5, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-stp', None, '2020-03-31', 20.5, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-region', None, '2020-03-31', 20.5, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-country', None, '2020-03-31', 20.5, 'x'],

            # unmatched location code
            ['GP_FTE_DPC Collated', 'nhs-ccg', 'b', '2020-03-31', 20.5, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-stp', 'b', '2020-03-31', 20.5, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-region', 'b', '2020-03-31', 20.5, 'x'],
            ['GP_FTE_DPC Collated', 'nhs-country', 'b', '2020-03-31', 20.5, 'x'],
        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'other']
    )

    dpc_collated_metadata_df = spark_session.createDataFrame(
        [
            ['DPC Collated', 'GP_FTE_DPC Collated',
             'title', 'name', 'description', 'Primary Care', 'Workforce', 'FTE']
        ],
        ['dimension_1', 'metric_id', 'metric_title', 'metric_name', 'metric_description',
         'programme_area', 'programme_sub_area', 'unit']
    )

    population_df = spark_session.createDataFrame(
        [
            ['Weighted_Patient_Population', 'nhs-ccg', 'ccg1', '2022-04-01', 2.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-stp', 'stp1', '2022-04-01', 5.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-region', 'r1', '2022-04-01', 10.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-country', 'england', '2022-04-01', 20.0, 'y'],

            # different locations, same timestamp
            ['Weighted_Patient_Population', 'nhs-ccg', 'ccg2', '2022-04-01', 11.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-stp', 'stp2', '2022-04-01', 12.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-region', 'r2', '2022-04-01', 13.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-country', 'not england', '2022-04-01', 14.0, 'y'],

            # unknown locations
            ['Weighted_Patient_Population', 'nhs-ccg', 'Unknown', '2022-04-01', 2.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-stp', 'Unknown', '2022-04-01', 4.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-region', 'Unknown', '2022-04-01', 5.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-country', 'Unknown', '2022-04-01', 10.0, 'y'],

            # same location code, different location type
            ['Weighted_Patient_Population', 'nhs-ccg', 'a', '2022-04-01', 10.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-stp', 'a', '2022-04-01', 10.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-region', 'a', '2022-04-01', 10.0, 'y'],
            ['Weighted_Patient_Population', 'nhs-country', 'a', '2022-04-01', 10.0, 'y'],

        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'other'],
    )

    expected_metrics_df = spark_session.createDataFrame(
        [
            # Relevant GP entries

            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-ccg', 'ccg1', '2020-03-31', 50000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-stp', 'stp1', '2020-03-31', 40000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-region', 'r1', '2020-03-31', 30000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-country', 'england', '2020-03-31', 20000.0, 'TBC', None],

            # same locations, different timestamp
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-ccg', 'ccg1', '2020-06-30', 5000000.0, 'TBC', 50000.0],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-stp', 'stp1', '2020-06-30', 4000000.0, 'TBC', 40000.0],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-region', 'r1', '2020-06-30', 3000000.0, 'TBC', 30000.0],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-country', 'england', '2020-06-30', 2000000.0, 'TBC', 20000.0],

            # different locations, same timestamp
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-ccg', 'ccg2', '2020-06-30', 90000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-stp', 'stp2', '2020-06-30', 90000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-region', 'r2', '2020-06-30', 90000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-country', 'not england', '2020-06-30', 90000.0, 'TBC', None],

            # unknown locations
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-ccg', 'Unknown', '2020-03-31', 5000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-stp', 'Unknown', '2020-03-31', 2500.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-region', 'Unknown', '2020-03-31', 2000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-country', 'Unknown', '2020-03-31', 1000.0, 'TBC', None],

            # same location code, different location type
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-ccg', 'a', '2020-03-31', 222000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-stp', 'a', '2020-03-31', 333000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-region', 'a', '2020-03-31', 444000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_GP Doctors', 'nhs-country', 'a', '2020-03-31', 555000.0, 'TBC', None],

            # Relevant Nurses entries

            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-ccg', 'ccg1', '2020-03-31', 50000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-stp', 'stp1', '2020-03-31', 40000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-region', 'r1', '2020-03-31', 30000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-country', 'england', '2020-03-31', 20000.0, 'TBC', None],

            # same locations, different timestamp
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-ccg', 'ccg1', '2020-06-30', 5000000.0, 'TBC', 50000.0],
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-stp', 'stp1', '2020-06-30', 4000000.0, 'TBC', 40000.0],
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-region', 'r1', '2020-06-30', 3000000.0, 'TBC', 30000.0],
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-country', 'england', '2020-06-30', 2000000.0, 'TBC', 20000.0],

            # different locations, same timestamp
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-ccg', 'ccg2', '2020-06-30', 90000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-stp', 'stp2', '2020-06-30', 90000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-region', 'r2', '2020-06-30', 90000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_Nurses', 'nhs-country', 'not england', '2020-06-30', 90000.0, 'TBC', None],

            # Relevant all staff entries

            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-ccg', 'ccg1', '2020-03-31', 50000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-stp', 'stp1', '2020-03-31', 40000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-region', 'r1', '2020-03-31', 30000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-country', 'england', '2020-03-31', 20000.0, 'TBC', None],

            # same locations, different timestamp
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-ccg', 'ccg1', '2020-06-30', 5000000.0, 'TBC', 50000.0],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-stp', 'stp1', '2020-06-30', 4000000.0, 'TBC', 40000.0],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-region', 'r1', '2020-06-30', 3000000.0, 'TBC', 30000.0],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-country', 'england', '2020-06-30', 2000000.0, 'TBC', 20000.0],

            # different locations, same timestamp
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-ccg', 'ccg2', '2020-06-30', 90000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-stp', 'stp2', '2020-06-30', 90000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-region', 'r2', '2020-06-30', 90000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-country', 'not england', '2020-06-30', 90000.0,
             'TBC', None],

            # unknown locations
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-ccg', 'Unknown', '2020-03-31', 5000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-stp', 'Unknown', '2020-03-31', 2500.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-region', 'Unknown', '2020-03-31', 2000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-country', 'Unknown', '2020-03-31', 1000.0, 'TBC', None],

            # same location code, different location type
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-ccg', 'a', '2020-03-31', 222000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-stp', 'a', '2020-03-31', 333000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-region', 'a', '2020-03-31', 444000.0, 'TBC', None],
            ['Rate_per_pop_GP_FTE_DPC Collated', 'nhs-country', 'a', '2020-03-31', 555000.0, 'TBC', None],

        ],
        ['metric_id', 'location_type', 'location_id', 'timestamp', 'metric_value', 'source',
         'metric_value_prev_quarter']
    ).withColumn('datapoint_id', f.concat_ws(
                '_',
                f.date_format(f.col('timestamp'), 'yyyy-MM-dd'),
                f.col('metric_id'),
                f.col('location_type'),
                f.col('location_id'),
                f.col('source')
                )) \
     .withColumn('unit', f.lit(None).cast(f.StringType())) \
     .withColumn("metric_value_12monthrolling", f.lit(None).cast(T.DoubleType())) \
     .withColumn("metric_value_prev_month", f.lit(None).cast(T.DoubleType())) \
     .fillna(random_string)

    result_metrics_df, result_metadata_df = compute_metrics_and_metadata(dpc_collated_df,
                                                                         dpc_collated_metadata_df,
                                                                         gp_fte_df, gp_fte_metadata_df,
                                                                         population_df)
    result_metrics_df = result_metrics_df.fillna(random_string)

    assert_df(expected_metrics_df, result_metrics_df)
