"""
DEV11
"""
from transforms.api import transform, Input, Output, configure
import myproject.datasets.utils as utils
from pyspark.sql import functions as f
import transforms.verbs.dataframes as D
import myproject.datasets.direct_patient_care_fte.dpc_utils as dpc_utils
from myproject.datasets.direct_patient_care_fte.dpc_nwrs import dimension_cols, date_col, source_of_data, unit
from metrics_ontology_shared_code.primary_care_dashboard_utils.pc_utils import get_time_comparison, join_locations

metric_id_prefix = "DPC_FTE_Collated"
IS_PUBLISHED = True
IS_SENSITIVE = False

loc_dict = {"country_id": "nhs-country",
            "nhs_region_id": "nhs-region",
            "stp_code": "nhs-stp",
            "current_ccg_code": "nhs-ccg",
            "pcn_code": "nhs-pcn"}


# for the below @configure, see below URL as `get_time_comparison` function requires the below profile
# to be enabled
# https://ppds.palantirfoundry.co.uk/workspace/documentation/product/transforms/spark3#adding-months-no-longer-sticks-to-the-last-day
@configure(profile=['ALLOW_ADD_MONTHS', 'DRIVER_MEMORY_MEDIUM'])
@transform(
    output_metrics=Output("ri.foundry.main.dataset.4380414f-3551-4ad4-b17c-63e23cc7c4d0",
                          checks=utils.get_metrics_dataset_checks("all")
                          ),
    output_metadata=Output("ri.foundry.main.dataset.f69c73a9-ad6a-4906-805c-0e511d21bfba",
                           checks=utils.METADATA_DATASET_CHECKS
                           ),
    df_dpc=Input("ri.foundry.main.dataset.bfd0ff62-354f-40d8-811f-034443c500dc"),
    all_locations=Input("ri.foundry.main.dataset.c4625196-f6bc-4840-bf49-b63b7e1390d0"),
    ccg=Input("ri.foundry.main.dataset.a01b55f3-626d-459c-9d19-f65201df8ca5"),
    ref_dates=Input("ri.foundry.main.dataset.36abfcca-6f49-49f5-a71a-6d2fdfa43244"),
    arrs_metrics=Input("ri.foundry.main.dataset.ca8fbbf5-2147-4686-a6c6-8423bfb97f3d")
    )
def compute(output_metrics, output_metadata, df_dpc, all_locations, ccg, ref_dates, arrs_metrics, ctx):

    metric, metadata = perform_calculation(df_dpc.dataframe(), all_locations.dataframe(), ccg.dataframe(),
                                           ref_dates.dataframe(), arrs_metrics.dataframe(), ctx.spark_session)

    output_metadata.write_dataframe(metadata)
    output_metrics.write_dataframe(metric)


def perform_calculation(df_dpc, all_locations, ccg, ref_dates, arrs_metrics, spark_session):
    df_dpc.persist()
    df_dpc.count()

    # Join to Location Ontology
    df_dpc_pcn = join_locations(df_dpc.filter(df_dpc['pcn_code'] != 'Unaligned').drop('ccg_code'),
                                all_locations, 'nhs-pcn', 'pcn_code')
    df_dpc_ccg = dpc_utils.join_location(ccg, df_dpc.filter((df_dpc['pcn_code'] == 'Unaligned')),
                                         "ccg_code", spark_session)
    df_dpc = D.union_many(*[df_dpc_pcn, df_dpc_ccg], how="wide")

    df_dpc = df_dpc.withColumn("dimension_1", f.lit("Direct Patient Care - Collated figure"))
    df_dpc.persist()
    df_dpc.count()

    # generate the metadata at all four dimensions, top three dimensions, top two dimensions & top one dimension
    metadata_sets = [getMetadata(df_dpc, dimension_cols, False),
                     getMetadata(df_dpc, dimension_cols[0:3], True),
                     getMetadata(df_dpc, dimension_cols[0:2], True),
                     getMetadata(df_dpc, dimension_cols[0:1], True)]

    # combine the metadata
    dpc_metadata = D.union_many(*metadata_sets, how="wide")
    dpc_metadata.persist()
    dpc_metadata.count()

    # generate the metrics at all four dimensions, top three dimensions, top two dimensions & top one dimension
    without_baseline_sets = [generate_metrics(df_dpc, dimension_cols, "collated_figure_absolute",
                                              dpc_metadata, totals_dimension=False),
                             generate_metrics(df_dpc, dimension_cols[0:3], "collated_figure_absolute",
                                              dpc_metadata, totals_dimension=True),
                             generate_metrics(df_dpc, dimension_cols[0:2], "collated_figure_absolute",
                                              dpc_metadata, totals_dimension=True),
                             generate_metrics(df_dpc, dimension_cols[0:1], "collated_figure_absolute",
                                              dpc_metadata, totals_dimension=True)]
    without_baseline = D.union_many(*without_baseline_sets, how="wide")

    # generate the baseline value at all four dimensions, top three dimensions, top two dimensions & top one dimmension
    # baseline is available in "2018_19_q4_baseline" col in raw table and it is repeated for each quarter
    # therefore filter to 1 quarter of data only
    df_dpc.persist()
    df_dpc.count()
    df_dpc_baseline = df_dpc.filter(df_dpc.year_and_quarter == "2020/21 Q1")
    baseline_sets = [generate_metrics(df_dpc_baseline, dimension_cols, "baseline_value", dpc_metadata,
                                      totals_dimension=False),
                     generate_metrics(df_dpc_baseline, dimension_cols[0:3], "baseline_value", dpc_metadata,
                                      totals_dimension=True),
                     generate_metrics(df_dpc_baseline, dimension_cols[0:2], "baseline_value", dpc_metadata,
                                      totals_dimension=True),
                     generate_metrics(df_dpc_baseline, dimension_cols[0:1], "baseline_value", dpc_metadata,
                                      totals_dimension=True)]
    baseline = D.union_many(*baseline_sets, how="wide").withColumnRenamed("metric_value", "baseline")
    df_dpc.unpersist()


    # merge baseline data to metric table, then add baseline_date & target_value
    metric_table = without_baseline \
        .join(f.broadcast(baseline.select("location_id", "location_type", "metric_id", "baseline")),
              ["location_id", "location_type", "metric_id"], "left")
    metric_table.persist()
    metric_table.count()
    # join to metadata to use dimensions cols for creating target value at national level and by top dimension
    metric_table = metric_table.join(f.broadcast(dpc_metadata.select('metric_id', 'dimension_1', 'dimension_2',
                                                         'dimension_3', 'dimension_4')), ['metric_id'], 'left')

    # add target and progress to national level and top dimension. Conditions are in dpc_utils.get_taget_condition
    metric_table = dpc_utils.generate_target(metric_table, "Direct Patient Care - Collated figure")

    # create the progress towards target value column for the national level only and GP staff group only
    metric_table = dpc_utils.generate_progress(metric_table, "Direct Patient Care - Collated figure")

    # add the financial year
    metric_w_year = metric_table.join(f.broadcast(ref_dates.select('date', 'fin_year')),
                                      metric_table['timestamp'] == ref_dates['date'])
    metric_table.unpersist()

    # add the financial year and select the relevant columns
    arrs_w_year = arrs_metrics.join(f.broadcast(ref_dates.select('date', 'fin_year')),
                                    arrs_metrics['timestamp'] == ref_dates['date']) \
                              .select('location_id', 'metric_value', 'fin_year') \
                              .withColumnRenamed('metric_value', 'initial_trajectory')

    # join with the ARRS data to obtain the trajectory
    metrics_w_arrs = metric_w_year.join(f.broadcast(arrs_w_year), ['location_id', 'fin_year'], 'left') \
                                  .withColumn('trajectory', f.when(get_target_condition(metric_w_year),
                                                                   arrs_w_year.initial_trajectory)
                                                             .otherwise(None)) \
                                  .drop('initial_trajectory')

    # perform calculation
    metrics = metrics_w_arrs.withColumn('difference_from_trajectory',
                                        f.when(get_target_condition(metrics_w_arrs),
                                               metrics_w_arrs.metric_value
                                               - metrics_w_arrs.baseline
                                               - metrics_w_arrs.trajectory)
                                        .otherwise(f.lit(None))) \
                            .drop('dimension_1', 'dimension_2', 'dimension_3', 'dimension_4', 'fin_year', 'date')
    metrics.persist()
    metrics.count()

    metrics = utils.generate_percentage_baseline(metrics)

    # create the time comparisons, year and quarter
    compared_metrics = get_time_comparison(get_time_comparison(metrics, 'quarter', last_day=True),
                                           'year', last_day=True)

    return compared_metrics.repartition(1), dpc_metadata


def perform_aggregation(df, dimensions, agg_col):
    """
    Returns a dataset with the aggregations calculated based on the provided dimensions.

    *args*:
      `df` - the dataframe to operate on
      `dimensions` - the dimensions to perform the metrics on
    """

    # SPECIFIC METRIC CALCULATION
    metrics_calc = f.sum(agg_col).cast("double")

    # CUSTOM LOGIC/ FILTER
    return utils.get_metrics(df, date_col, dimensions, metrics_calc, loc_dict)


def generate_metrics(df, dim_cols, agg_col, df_metadata, totals_dimension=False):
    """
    Returns a dataset with the metrics in the format of the Metrics Ontology,
    which includes key fields such as `metric_id`, `timestamp`, `metric_value`,
    `location_id` and `location_type`.

    *args*:
      `df` - the dataframe to operate on
      `dim_cols` - the dimensions to perform the metrics on
      `df_metadata` - the metadata associated with the `df` dataset
    """

    # aggregate the data at three dimensions, two dimensions and single dimension
    aggregated_data = perform_aggregation(df, dim_cols, agg_col)

    # generate the metrics
    return utils.generate_metric_data(aggregated_data, df_metadata, metric_id_prefix, date_col,
                                      dim_cols, source_of_data, totals_dimension=totals_dimension)


def getMetadata(gp_df, cols, totals_dimension):
    return utils.generate_metadata(
        df=gp_df,
        dimension_cols=cols,
        metric_id_prefix=metric_id_prefix,
        unit_type=unit,
        programme_area='Primary Care',
        programme_sub_area='Workforce',
        metric_title='Direct Patient Care (DPC) full time equivalent (FTE) collated figures',
        totals_dimension=totals_dimension,
        metric_description="Number of full time equivalent (FTE) Direct Patient Care (DPC) "
                           "roles in General Practice and PCNs from collated figures",
        short_name="Collated",
        is_published=IS_PUBLISHED,
        is_sensitive=IS_SENSITIVE
    )


def get_target_condition(df, include_region=True):
    """
    Returns the condition for which to create targets and progress

    *args*:
      `df` - the dataframe to operate on
    """

    location_types = ['nhs-country']
    if include_region:
        location_types.append('nhs-region')

    return ((df.location_type.isin(location_types))
            & (df.dimension_1 == 'Direct Patient Care - Collated figure')
            & (df.dimension_2 == 'Total')
            & (df.dimension_3.isNull()))
