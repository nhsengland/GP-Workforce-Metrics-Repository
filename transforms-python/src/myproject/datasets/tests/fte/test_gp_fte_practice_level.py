from myproject.datasets.fte.gp_fte_practice_level import perform_compute
from nhs_foundry_common.test_utils import assert_df
from pyspark.sql import functions as f, types as T


def test_aggregation(spark_session):

    synthetic = spark_session.createDataFrame(
        [
            ['TOTAL_GP_FTE', '17/10/2015', 1.0, 'L1'],
            ['TOTAL_GP_FTE', '17/10/2015', 4.0, 'L2'],
            ['TOTAL_GP_FTE', '18/10/2015', 2.0, 'L1'],
            ['TOTAL_GP_FTE', '18/10/2015', 5.0, 'L2'],
            ['FEMALE_GP_FTE', '18/10/2015', 2.0, 'L1'],
            ['MALE_GP_FTE', '18/10/2015', 6.0, 'L2'],
        ],
        ['measure', 'effective_snapshot_date', 'measure_value', 'practice_code']
    )

    gp_location = spark_session.createDataFrame(
        [
            ['L1'],
        ],
        ['organisation_code']
    )

    expected_metrics = spark_session.createDataFrame(
        [
            ['GP_Practice_FTE_GP_Total', 'nhs-gp', 'L1', '18/10/2015', 2.0, 'FTE', 'Practice Level Census Data'],
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

    expected_metadata = spark_session.createDataFrame(
        [
            ['GP', 'Total', 'GP_Practice_FTE_GP_Total', 'Primary Care', 'Workforce', 'FTE'],
        ],
        ['dimension_1', 'dimension_2', 'metric_id', 'programme_area', 'programme_sub_area', 'unit']
    ).withColumn("metric_title", f.lit("GP Full Time Equivalent (FTE) - Practice level")) \
     .withColumn("metric_name", f.lit("GP_Full_Time_Equivalent_(FTE)_-_Practice_level")) \
     .withColumn("metric_description", f.lit("The number of full time equivalent (FTE) staff working in General Practice - Practice level")) \
     .withColumn("is_sensitive", f.lit(False)) \
     .withColumn("is_published", f.lit(True)) \
     .withColumn("metric_short_name", f.lit(None).cast(T.StringType())) \
     .withColumn("detailed_programme_area", f.lit(None).cast(T.StringType())) \
     .withColumn("technical_description", f.lit(None).cast(T.StringType())) \
     .withColumn("description_is_visible", f.lit(True)) \
     .withColumn("tab_reference", f.lit(None).cast(T.StringType()))

    output_metrics, output_metadata = perform_compute(synthetic, gp_location)

    assert_df(output_metrics, expected_metrics)
    assert_df(output_metadata, expected_metadata)
