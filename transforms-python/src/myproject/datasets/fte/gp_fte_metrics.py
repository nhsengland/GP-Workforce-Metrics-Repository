"""
DEV01
"""
from pyspark.sql import functions as f, types as t
from transforms.api import transform_df, Input, Output, configure
import transforms.verbs.dataframes as D
import myproject.datasets.utils as utils
from myproject.datasets.fte.gp_fte_metadata import dimension_cols, metric_id_prefix
from datetime import datetime, timedelta
from metrics_ontology_shared_code.primary_care_dashboard_utils.pc_utils import get_time_comparison

date_col = "effective_snapshot_date"
loc_dict = {"country_id": "nhs-country",
            "nhs_region_id": "nhs-region",
            "stp_code": "nhs-stp",
            "current_ccg_code": "nhs-ccg"}
source_of_data = "GP Level Census"
baseline_date = '2019-03-31'
national_gp_target = 6000


# for the below @configure, see below URL as `get_time_comparison` function requires the below profile
# to be enabled
# https://ppds.palantirfoundry.co.uk/workspace/documentation/product/transforms/spark3#adding-months-no-longer-sticks-to-the-last-day
@configure(profile=['ALLOW_ADD_MONTHS', 'DRIVER_MEMORY_MEDIUM'])
@transform_df(
    Output("ri.foundry.main.dataset.d03145cc-e290-45f5-8606-9ed4716cb56d",
           checks=utils.get_metrics_dataset_checks("all")),
    gp_census=Input("ri.foundry.main.dataset.c5b24d93-aab2-479a-b216-36f10fcb3a12"),
    ccgs=Input("ri.foundry.main.dataset.a01b55f3-626d-459c-9d19-f65201df8ca5"),
    gp_metadata=Input("ri.foundry.main.dataset.48fb636f-3f26-4c1c-ac77-1b1e9306bb07")
    )
def compute(gp_census, ccgs, gp_metadata, ctx):

    gp = join_locations(gp_census, ccgs, ctx.spark_session)
    gp.persist()
    gp.count()
    # generate the metrics at all three dimensions, all two dimensions and the top level dimension
    without_baseline_sets = []
    without_baseline_sets.append(generate_metrics(gp, dimension_cols, gp_metadata, False))
    without_baseline_sets.append(generate_metrics(gp, dimension_cols[0:2], gp_metadata, True))
    without_baseline_sets.append(generate_metrics(gp, dimension_cols[0:1], gp_metadata, True))
    without_baseline_sets.append(generate_metrics(gp, [], gp_metadata, True))

    without_baseline = D.union_many(*without_baseline_sets, how="wide")
    without_baseline.persist()
    without_baseline.count()
    # add the baseline so that it can be used in calculations
    with_baseline = generate_12m_baseline(generate_baseline(without_baseline))

    # add the metadata so that in the next step, we can get the dimensions to identify only GP totals and
    # not GP breakdowns
    w_baseline_plus_metadata = with_baseline.join(f.broadcast(gp_metadata.select('metric_id', 'dimension_1', 'dimension_2',
                                                                     'dimension_3')),
                                                  ['metric_id'], 'left')

    # create the target value column for the national level only and GP staff group only
    w_baseline_and_target = generate_target(w_baseline_plus_metadata)

    # create the progress towards target value column for the national level only and GP staff group only
    progress_metrics = generate_progress(w_baseline_and_target)

    # create the time comparisons, year and month
    compared_metrics = get_time_comparison(get_time_comparison(progress_metrics, 'month', last_day=True),
                                           'year', last_day=True)

    # create percentage baseline column
    metrics = utils.generate_percentage_baseline(compared_metrics)

    return metrics.drop('dimension_1', 'dimension_2', 'dimension_3').repartition(1)


def perform_aggregation(df, dimensions):
    """
    Returns a dataset with the aggregations calculated based on the provided dimensions.

    *args*:
      `df` - the dataframe to operate on
      `dimensions` - the dimensions to perform the metrics on
    """

    # SPECIFIC METRIC CALCULATION
    metrics_calc = f.sum("fte").cast("double")

    # CUSTOM LOGIC/ FILTER

    return utils.get_metrics(df, date_col, dimensions, metrics_calc, loc_dict)


def generate_metrics(df, dim_cols, df_metadata, totals_dimension=False):
    """
    Returns a dataset with the metrics in the format of the Metrics Ontology,
    which includes key fields such as `metric_id`, `timestamp`, `metric_value`,
    `location_id` and `location_type`.\n

    *args*:\n
      `df` - the dataframe to operate on\n
      `dim_cols` - the dimensions to perform the metrics on\n
      `df_metadata` - the metadata associated with the `df` dataset
    """

    # aggregate the data at three dimensions, two dimensions and single dimension
    aggregated_data = perform_aggregation(df, dim_cols)

    # generate the metrics
    return utils.generate_metric_data(aggregated_data, df_metadata, metric_id_prefix, date_col,
                                      dim_cols, source_of_data, totals_dimension)


def generate_baseline(df):
    """
    Returns a dataset with the `baseline` column, which is the based on the value for
    that `metric_id`, `location_type` and `location_id` on the `{baseline_date}`

    *args*:
      `df` - the dataframe to operate on
    """

    # identify the baseline records
    baseline = df \
        .filter(f"timestamp = '{baseline_date}'") \
        .withColumnRenamed('metric_value', 'baseline') \
        .select('metric_id', 'baseline', 'location_type', 'location_id')

    # join the baseline records to the original dataset
    return df.join(f.broadcast(baseline), ['metric_id', 'location_type', 'location_id'], 'left')


def generate_12m_baseline(df):
    """
    Returns a dataset with the `baseline_12m_sum` column, which is the based on the sum of the
    baseline for the previous 12m based on the `metric_id`, `location_type` and `location_id`
    on the `{baseline_date}`

    *args*:
      `df` - the dataframe to operate on
    """

    twelve_months_pre_baseline_start_date = (datetime.strptime(baseline_date, "%Y-%m-%d").date()
                                             + timedelta(days=-364)).strftime('%Y-%m-%d')

    # identify the baseline records
    baseline_12m = df \
        .filter(f"timestamp between '{twelve_months_pre_baseline_start_date}' and '{baseline_date}'") \
        .groupby('metric_id', 'location_type', 'location_id') \
        .agg(f.sum('metric_value').alias('baseline_12m_sum'))

    # join the baseline records to the original dataset
    return df.join(f.broadcast(baseline_12m), ['metric_id', 'location_type', 'location_id'], 'left')


def generate_target(df):
    """
    Returns a dataset with the `target_value` column, with the value {national_gp_target} if it meets
    the condition specified by the function `get_target_condition()`

    *args*:
      `df` - the dataframe to operate on
    """

    return df.withColumn('target_value', f.when(get_target_condition(df), f.lit(national_gp_target))
                                          .otherwise(f.lit(None).cast(t.LongType())))


def generate_progress(df):
    """
    Returns a dataset with the `progress_towards_target_value` column, with the calculation of
    `baseline + {national_gp_target} - metric_value`, provided it meets the condition specified by the function
    `get_target_condition()`

    *args*:
      `df` - the dataframe to operate on
    """

    return df.withColumn('progress_towards_target_value', f.when(get_target_condition(df),
                                                                 df.baseline
                                                                 + f.lit(national_gp_target)
                                                                 - df.metric_value)
                                                           .otherwise(f.lit(None)))


def get_target_condition(df):
    """
    Returns the condition for which to create targets and progress

    *args*:
      `df` - the dataframe to operate on
    """

    return ((df.location_id == 'england')
            & (df.dimension_1 == 'GP - PRACTICE')
            & (df.dimension_2 == 'Total')
            & (df.dimension_3.isNull()))


def join_locations(gp_census, ccgs, spark_session):
    """
    Returns the dataset with a hierarchy of locations. For a particular CCG, it would
    return it's STP, Region and Country as well as their names. This is so that it can
    be used in roll-ups

    *args*:
      `df` - the dataframe to operate on, which contains the `commissioner_code` field
    """

    ccgs = ccgs.select("ccg_code", "current_ccg_code", "current_ccg_name", "nhs_region_id",
                       "nhs_region_name", "stp_code", "stp_name", "country_id")
    unknown_ccg = spark_session.createDataFrame(
        [
            ['Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'england']
        ],
        ["ccg_code", "current_ccg_code", "current_ccg_name", "nhs_region_id",
         "nhs_region_name", "stp_code", "stp_name", "country_id"]
    )
    ccgs = ccgs.unionByName(unknown_ccg)

    gp_census = gp_census.join(f.broadcast(ccgs), gp_census.commissioner_code == ccgs.ccg_code, "left")\
        .drop('ccg_code')\
        .withColumnRenamed('commissioner_code', 'ccg_code')

    return gp_census
