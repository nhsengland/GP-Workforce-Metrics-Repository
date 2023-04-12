from myproject.datasets.fte.gp_fte_metadata import compute
from nhs_foundry_common.test_utils import assert_df
from pyspark.sql import functions as f, types as T


def test_metadata_creation(spark_session):
    metric_id_prefix = "GP_FTE"
    unit_type = "FTE"

    programme_area = 'Primary Care'
    programme_sub_area = 'Workforce'
    metric_title = 'GP Full Time Equivalent (FTE)'
    metric_name = 'GP_Full_Time_Equivalent_(FTE)'

    metric_description_part1 = 'The sum of full time equivalent (FTE) '
    metric_description_part2 = ' staff working in General Practice'

    total_suffix = '_Total'

    is_sensitive = False
    is_published = True

    # a random string is used to replace null values as the test framework does not have the
    # ability to compare null values
    random_string = 'j02qdc89nhrt7289qj5489qsw7x89e4702hdqjs3482'

    df = spark_session.createDataFrame(
        [
            ['a', 'b', 'c', 'x'],
            ['A', 'B', 'C', 'x'],
            ['1/2', '@3', '4_', 'x']
        ],
        ['staff_group', 'staff_role', 'detailed_staff_role', 'another_random']
    )

    metric_description_1 = metric_description_part1 + 'a' + metric_description_part2
    metric_description_2 = metric_description_part1 + 'a' + metric_description_part2
    metric_description_3 = metric_description_part1 + '1/2' + metric_description_part2
    metric_description_4 = metric_description_part1 + 'total' + metric_description_part2

    expected_df = spark_session.createDataFrame(
        [
            [metric_id_prefix+'_a_b_c', 'a', 'b', 'c', programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_1, unit_type, is_sensitive, is_published],
            [metric_id_prefix+'_A_B_C', 'A', 'B', 'C', programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_2, unit_type, is_sensitive, is_published],
            [metric_id_prefix+'_1/2_@3_4_', '1/2', '@3', '4_', programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_3, unit_type, is_sensitive, is_published],

            [metric_id_prefix+'_a_b'+total_suffix, 'a', 'b', 'Total', programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_1, unit_type, is_sensitive, is_published],
            [metric_id_prefix+'_A_B'+total_suffix, 'A', 'B', 'Total', programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_2, unit_type, is_sensitive, is_published],
            [metric_id_prefix+'_1/2_@3'+total_suffix, '1/2', '@3', 'Total', programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_3, unit_type, is_sensitive, is_published],

            [metric_id_prefix+'_a'+total_suffix, 'a', 'Total', None, programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_1, unit_type, is_sensitive, is_published],
            [metric_id_prefix+'_A'+total_suffix, 'A', 'Total', None, programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_2, unit_type, is_sensitive, is_published],
            [metric_id_prefix+'_1/2'+total_suffix, '1/2', 'Total', None, programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_3, unit_type, is_sensitive, is_published],

            [metric_id_prefix+total_suffix, 'Total', None, None, programme_area, programme_sub_area,
                metric_title, metric_name, metric_description_4, unit_type, is_sensitive, is_published],
        ],
        ['metric_id', 'dimension_1', 'dimension_2', 'dimension_3', 'programme_area', 'programme_sub_area',
         'metric_title', 'metric_name', 'metric_description', 'unit', 'is_sensitive', 'is_published']
    ).withColumn("metric_short_name", f.lit(None).cast(T.StringType()))\
     .withColumn("detailed_programme_area", f.lit(None).cast(T.StringType())) \
     .withColumn("technical_description", f.lit(None).cast(T.StringType())) \
     .withColumn("description_is_visible", f.lit(True)) \
     .withColumn("tab_reference", f.lit(None).cast(T.StringType()))

    # a random string is used to replace null values as the test framework does not have the
    # ability to compare null values
    # Ha's note: have tried fillna with subset of all cols except for metric_short_name, but same error returns
    expected_df = expected_df.fillna(random_string)

    # a random string is used to replace null values as the test framework does not have the
    # ability to compare null values
    output_df = compute(df).fillna(random_string)

    assert_df(output_df, expected_df)
