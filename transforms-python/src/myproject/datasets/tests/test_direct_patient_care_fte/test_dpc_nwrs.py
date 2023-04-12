from nhs_foundry_common.test_utils import assert_df
from myproject.datasets.direct_patient_care_fte.dpc_nwrs import perform_calculation
from pyspark.sql import functions as f, types as t

programme_area = 'Primary Care'
programme_sub_area = 'Workforce'
metric_title = 'Direct Patient Care (DPC) full time equivalent (FTE) national workforce reporting service (NWRS)'
metric_description = 'Number of full time equivalent (FTE) Direct Patient Care (DPC) roles in General Practice and PCNs from national workforce reporting service (NWRS)'
short_name = 'NWRS'
metric_name = metric_title.replace(' ', '_')
source = "FTE DPC Staff GP PCN Collated"


def test_perform_calculation(spark_session):

    ccg = spark_session.createDataFrame(
        [
            ["C1", "C2", "S1", "R1", "england"],
            ["C2", "C2", "S1", "R1", "england"],
            ["C3", "C3", "S2", "R1", "england"],
            ["C4", "C4", "S2", "R1", "england"],
            ["C5", "C5", "S1", "R2", "england"],
        ],
        ["ccg_code", "current_ccg_code", "stp_code", "nhs_region_id", "country_id"],
    )

    df_dpc = spark_session.createDataFrame(
        [
            ["2020/21 Q1", "C1", "staffA", "role1", "arrs", 15, 11, "2020-06-30"],
            ["2020/21 Q1", "C3", "staffA", "role2", "non-arrs", 24, 21, "2020-06-30"],
            ["2021/22 Q2", "C2", "staffB", "role3", "non-arrs", 32, 30, "2021-09-30"],
            ["2021/22 Q2", "C4", "staffB", "role3", "non-arrs", 35, 30, "2021-09-30"],
            ["2021/22 Q2", "#N/A", "staffA", "role1", "arrs", 17, 11, "2021-09-30"],
        ],
        ["year_and_quarter", "ccg_code", "staff_role", "cleaned_role_name", "arrs",
         "all_nwrs_absolute", "baseline_value", "effective_snapshot_date_initial"],
    ).withColumn('effective_snapshot_date', f.col("effective_snapshot_date_initial").cast(t.DateType()))

    expected_metadata = spark_session.createDataFrame(
        [
            ["DPC NWRS", "staffA", "role1", "arrs", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_arrs"],
            ["DPC NWRS", "staffA", "role2", "non-arrs", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_non-arrs"],
            ["DPC NWRS", "staffB", "role3", "non-arrs", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_non-arrs"],

            ["DPC NWRS", "staffA", "role1", "Total", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_Total"],
            ["DPC NWRS", "staffA", "role2", "Total", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_Total"],
            ["DPC NWRS", "staffB", "role3", "Total", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_Total"],

            ["DPC NWRS", "staffA", "Total", None, "DPC_FTE_NWRS_DPC NWRS_staffA_Total"],
            ["DPC NWRS", "staffB", "Total", None, "DPC_FTE_NWRS_DPC NWRS_staffB_Total"],

            ["DPC NWRS", "Total", None, None, "DPC_FTE_NWRS_DPC NWRS_Total"],
        ],
        ["dimension_1", "dimension_2", "dimension_3", "dimension_4", "metric_id"]
    ).withColumn("metric_title", f.lit(metric_title)) \
     .withColumn("metric_name", f.lit(metric_name)) \
     .withColumn("metric_description", f.lit(metric_description)) \
     .withColumn("metric_short_name", f.lit(short_name)) \
     .withColumn("is_sensitive", f.lit(False)) \
     .withColumn("is_published", f.lit(True)) \
     .withColumn("programme_area", f.lit(programme_area)) \
     .withColumn("programme_sub_area", f.lit(programme_sub_area)) \
     .withColumn("unit", f.lit('FTE')) \
     .withColumn("detailed_programme_area", f.lit(None).cast(t.StringType())) \
     .withColumn("technical_description", f.lit(None).cast(t.StringType())) \
     .withColumn("description_is_visible", f.lit(True)) \
     .withColumn("tab_reference", f.lit(None).cast(t.StringType()))

    expected_metric = spark_session.createDataFrame(
        [
            # 4 dims, ccg
            ["2020-06-30", "C2", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_arrs", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "C3", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_non-arrs", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "C2", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_non-arrs", "FTE", source, 32.0, None, None, None],
            ["2021-09-30", "C4", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_non-arrs", "FTE", source, 35.0, None, None, None],
            ["2021-09-30", "Unaligned", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_arrs", "FTE", source, 17.0, None, None, None],

            # 4 dims, stp
            ["2020-06-30", "S1", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_arrs", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "S2", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_non-arrs", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "S1", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_non-arrs", "FTE", source, 32.0, None, None, None],
            ["2021-09-30", "S2", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_non-arrs", "FTE", source, 35.0, None, None, None],
            ["2021-09-30", "Unaligned", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_arrs", "FTE", source, 17.0, None, None, None],

            # 4 dims, region
            ["2020-06-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_arrs", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_non-arrs", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "Unaligned", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_arrs", "FTE", source, 17.0, None, None, None],
            ["2021-09-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_non-arrs", "FTE", source, 67.0, None, None, None],

            # 4 dims, country
            ["2020-06-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_arrs", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_non-arrs", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_arrs", "FTE", source, 17.0, 11.0, None, None],
            ["2021-09-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_non-arrs", "FTE", source, 67.0, None, None, None],

            # 3 dims, ccg
            ["2020-06-30", "C2", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_Total", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "C3", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_Total", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "C2", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_Total", "FTE", source, 32.0, None, None, None],
            ["2021-09-30", "C4", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_Total", "FTE", source, 35.0, None, None, None],
            ["2021-09-30", "Unaligned", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_Total", "FTE", source, 17.0, None, None, None],

            # 3 dims, stp
            ["2020-06-30", "S1", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_Total", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "S2", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_Total", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "S1", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_Total", "FTE", source, 32.0, None, None, None],
            ["2021-09-30", "S2", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_Total", "FTE", source, 35.0, None, None, None],
            ["2021-09-30", "Unaligned", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_Total", "FTE", source, 17.0, None, None, None],

            # 3 dims, region
            ["2020-06-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_Total", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_Total", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "Unaligned", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_Total", "FTE", source, 17.0, None, None, None],
            ["2021-09-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_Total", "FTE", source, 67.0, None, None, None],

            # 3 dims, country
            ["2020-06-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_Total", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffA_role2_Total", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffA_role1_Total", "FTE", source, 17.0, 11.0, None, None],
            ["2021-09-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffB_role3_Total", "FTE", source, 67.0, None, None, None],

            # 2 dims, ccg
            ["2020-06-30", "C2", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "C3", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "C2", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffB_Total", "FTE", source, 32.0, None, None, None],
            ["2021-09-30", "C4", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffB_Total", "FTE", source, 35.0, None, None, None],
            ["2021-09-30", "Unaligned", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 17.0, None, None, None],

            # 2 dims, stp
            ["2020-06-30", "S1", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "S2", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "S1", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffB_Total", "FTE", source, 32.0, None, None, None],
            ["2021-09-30", "S2", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffB_Total", "FTE", source, 35.0, None, None, None],
            ["2021-09-30", "Unaligned", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 17.0, None, None, None],

            # 2 dims, region
            ["2020-06-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 39.0, 32.0, None, None],
            ["2021-09-30", "Unaligned", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 17.0, None, None, None],
            ["2021-09-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_staffB_Total", "FTE", source, 67.0, None, None, None],

            # 2 dims, country
            ["2020-06-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 39.0, 32.0, None, None],
            ["2021-09-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffA_Total", "FTE", source, 17.0, 32.0, None, None],
            ["2021-09-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_staffB_Total", "FTE", source, 67.0, None, None, None],

            # 1 dim, ccg
            ["2020-06-30", "C2", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "C3", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "C2", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 32.0, 11.0, None, None],
            ["2021-09-30", "C4", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 35.0, None, None, None],
            ["2021-09-30", "Unaligned", "nhs-ccg", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 17.0, None, None, None],

            # 1 dim stp
            ["2020-06-30", "S1", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 15.0, 11.0, None, None],
            ["2020-06-30", "S2", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 24.0, 21.0, None, None],
            ["2021-09-30", "S1", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 32.0, 11.0, None, None],
            ["2021-09-30", "S2", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 35.0, 21.0, None, None],
            ["2021-09-30", "Unaligned", "nhs-stp", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 17.0, None, None, None],

            # 1 dim region
            ["2020-06-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 39.0, 32.0, None, None],
            ["2021-09-30", "R1", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 67.0, 32.0, None, None],
            ["2021-09-30", "Unaligned", "nhs-region", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 17.0, None, None, None],

            # 1 dim country
            ["2020-06-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 39.0, 32.0, 26000.0, 25993.0],
            ["2021-09-30", "england", "nhs-country", "DPC_FTE_NWRS_DPC NWRS_Total", "FTE", source, 84.0, 32.0, 26000.0, 25948.0],
        ],
        ["timestamp_initial", "location_id", "location_type", "metric_id", "unit", "source", "metric_value", "baseline",
         "target_value", "progress_towards_target_value"],
    ).fillna(0.0).withColumn('timestamp', f.col("timestamp_initial").cast(t.DateType())) \
     .drop("timestamp_initial") \
     .withColumn('datapoint_id', f.concat_ws(
                '_',
                f.date_format(f.col('timestamp'), 'yyyy-MM-dd'),
                f.col('metric_id'),
                f.col('location_type'),
                f.col('location_id'),
                f.col('source')
                )) \
     .withColumn("metric_value_12monthrolling", f.lit(0.0).cast(t.DoubleType()))

    # metric, metadata = perform_calculation(df_dpc, ccg, spark_session)

    # metric = metric.fillna(0.0)

    # assert_df(expected_metadata, metadata)
    # assert_df(expected_metric, metric)
