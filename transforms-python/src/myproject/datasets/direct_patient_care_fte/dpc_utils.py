from pyspark.sql import functions as f, types as t

national_dpc_target = 26000


def join_location(ccg, df, join_by_col, spark_session):
    """
    Returns a dataset with the location mapped to CCG table on Location Ontology via ccg_code

    *args*:
      `df` - the dataframe to operate on
      `ccg` - the ccg table from Location Ontology
    """
    ccg = ccg.select("ccg_code", "current_ccg_code", "nhs_region_id", "stp_code", "country_id")

    unknown_ccg = spark_session.createDataFrame(
        [
            ['Unknown', 'Unknown', 'Unknown', 'Unknown', 'england'],
            ['#N/A', 'Unaligned', 'Unaligned', 'Unaligned', 'england']
        ],
        ["ccg_code", "current_ccg_code", "nhs_region_id", "stp_code", "country_id"]
    )
    ccg = ccg.unionByName(unknown_ccg)

    return df.join(ccg, join_by_col, "left")


def generate_target(df, dim_1):
    """
    Returns a dataset with the `target_value` column, with the value {national_dpc_target} if it meets
    the condition specified by the function `get_target_condition()`

    *args*:
      `df` - the dataframe to operate on
    """

    return df.withColumn('target_value', f.when(get_target_condition(df, dim_1), f.lit(national_dpc_target))
                                          .otherwise(f.lit(None).cast(t.LongType())))


def generate_progress(df, dim_1):
    """
    Returns a dataset with the `progress_towards_target_value` column, with the calculation of
    `baseline + {national_gp_target} - metric_value`, provided it meets the condition specified by the function
    `get_target_condition()`

    *args*:
      `df` - the dataframe to operate on
    """

    return df.withColumn('progress_towards_target_value', f.when(get_target_condition(df, dim_1),
                                                                 df.baseline
                                                                 + f.lit(national_dpc_target)
                                                                 - df.metric_value)
                                                           .otherwise(f.lit(None)))


def get_target_condition(df, dim_1):
    """
    Returns the condition for which to create targets and progress

    *args*:
      `df` - the dataframe to operate on
    """

    return ((df.location_id == 'england')
            & (df.dimension_1 == dim_1)
            & (df.dimension_2 == 'Total')
            & (df.dimension_3.isNull())
            & (df.dimension_4.isNull()))
