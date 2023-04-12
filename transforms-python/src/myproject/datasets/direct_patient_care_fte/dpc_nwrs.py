"""
DEV10
"""
from transforms.api import transform, Input, Output, configure
from pyspark.sql import functions as f
import transforms.verbs.dataframes as D
import myproject.datasets.utils as utils
import myproject.datasets.direct_patient_care_fte.dpc_utils as dpc_utils
from metrics_ontology_shared_code.primary_care_dashboard_utils.pc_utils import join_locations

dimension_cols = ["dimension_1", "staff_role", "cleaned_role_name", "arrs"]
date_col = "effective_snapshot_date"
metric_id_prefix = "DPC_FTE_NWRS"
source_of_data = "FTE DPC Staff GP PCN Collated"
unit = "FTE"
loc_dict = {"country_id": "nhs-country",
            "nhs_region_id": "nhs-region",
            "stp_code": "nhs-stp",
            "current_ccg_code": "nhs-ccg",
            "pcn_code": "nhs-pcn"}
IS_PUBLISHED = True
IS_SENSITIVE = False


@transform(
    output_metrics=Output("ri.foundry.main.dataset.a8155408-7f7d-4967-944d-3db60daeb81c",
                          checks=utils.get_metrics_dataset_checks("all")
                          ),
    output_metadata=Output("ri.foundry.main.dataset.66b6e57c-dd68-4695-a022-d8e2982c1f27",
                           checks=utils.METADATA_DATASET_CHECKS
                           ),
    df_dpc=Input("ri.foundry.main.dataset.bfd0ff62-354f-40d8-811f-034443c500dc"),
    all_locations=Input("ri.foundry.main.dataset.c4625196-f6bc-4840-bf49-b63b7e1390d0"),
    ccg=Input("ri.foundry.main.dataset.a01b55f3-626d-459c-9d19-f65201df8ca5")
    )
def compute(output_metrics, output_metadata, df_dpc, all_locations, ccg, ctx):

    metric, metadata = perform_calculation(df_dpc.dataframe(), all_locations.dataframe(), ccg.dataframe(),
                                           ctx.spark_session)

    output_metadata.write_dataframe(metadata)
    output_metrics.write_dataframe(metric)


def perform_calculation(df_dpc, all_locations, ccg, spark_session):

    df_dpc.persist()
    df_dpc.count()
    # Join to Location Ontology
    df_dpc_pcn = join_locations(df_dpc.filter(df_dpc['pcn_code'] != 'Unaligned').drop('ccg_code'),
                                all_locations, 'nhs-pcn', 'pcn_code')
    df_dpc_ccg = dpc_utils.join_location(ccg, df_dpc.filter((df_dpc['pcn_code'] == 'Unaligned')),
                                         "ccg_code", spark_session)
    df_dpc = D.union_many(*[df_dpc_pcn, df_dpc_ccg], how="wide")

    df_dpc = df_dpc.withColumn("dimension_1", f.lit("DPC NWRS"))
    df_dpc.persist()
    df_dpc.count()

    metadata_sets = [getMetadata(df_dpc, dimension_cols, False),
                     getMetadata(df_dpc, dimension_cols[0:3], True),
                     getMetadata(df_dpc, dimension_cols[0:2], True),
                     getMetadata(df_dpc, dimension_cols[0:1], True)]

    # combine the metadata
    dpc_metadata = D.union_many(*metadata_sets, how="wide")
    dpc_metadata.persist()
    dpc_metadata.count()

    # generate the metrics at all four dimensions, top three dimensions, top two dimensions & top one dimension
    without_baseline_sets = [generate_metrics(df_dpc, dimension_cols, "all_nwrs_absolute", dpc_metadata, False),
                             generate_metrics(df_dpc, dimension_cols[0:3], "all_nwrs_absolute", dpc_metadata, True),
                             generate_metrics(df_dpc, dimension_cols[0:2], "all_nwrs_absolute", dpc_metadata, True),
                             generate_metrics(df_dpc, dimension_cols[0:1], "all_nwrs_absolute", dpc_metadata, True)]
    without_baseline = D.union_many(*without_baseline_sets, how="wide")
    df_dpc.persist()
    df_dpc.count()

    # generate the baseline value at all four dimensions, top three dimensions, top two dimensions & top one dimension
    # baseline is available in "baseline_value" col in raw table and it is repeated for each quarter
    # therefore filter to 1 quarter of data only
    df_dpc_baseline = df_dpc.filter(df_dpc.year_and_quarter == "2020/21 Q1")
    df_dpc_baseline.persist()
    df_dpc_baseline.count()
    baseline_sets = [generate_metrics(df_dpc_baseline, dimension_cols, "baseline_value", dpc_metadata, False),
                     generate_metrics(df_dpc_baseline, dimension_cols[0:3], "baseline_value", dpc_metadata, True),
                     generate_metrics(df_dpc_baseline, dimension_cols[0:2], "baseline_value", dpc_metadata, True),
                     generate_metrics(df_dpc_baseline, dimension_cols[0:1], "baseline_value", dpc_metadata, True)]

    # join the baseline datasets together
    baseline = D.union_many(*baseline_sets, how="wide").withColumnRenamed("metric_value", "baseline")
    baseline.persist()
    baseline.count()

    # merge baseline data to metric table, add baseline_date & target_value
    metric_table = without_baseline \
        .join(f.broadcast(baseline.select("location_id", "location_type", "metric_id", "baseline")),
              ["location_id", "location_type", "metric_id"], "left")

    # join to metadata to get dimension cols, which are needed for target_value creation
    metric_table = metric_table.join(f.broadcast(dpc_metadata.select('metric_id', 'dimension_1', 'dimension_2',
                                                         'dimension_3', 'dimension_4')), ['metric_id'], 'left')

    # add target and progress to national level and to top dimension only
    metric_table = dpc_utils.generate_target(metric_table, "DPC NWRS")

    # create the progress towards target value column for the national level only and GP staff group only
    metric_table = dpc_utils.generate_progress(metric_table, "DPC NWRS")

    # drop unnessary columns
    metric_table = metric_table.drop('dimension_1', 'dimension_2', 'dimension_3', 'dimension_4').repartition(1)

    return metric_table, dpc_metadata


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
                                      dim_cols, source_of_data, totals_dimension)


def getMetadata(gp_df, cols, totals_dimension):
    return utils.generate_metadata(
        df=gp_df,
        dimension_cols=cols,
        metric_id_prefix=metric_id_prefix,
        unit_type=unit,
        programme_area='Primary Care',
        programme_sub_area='Workforce',
        metric_title='Direct Patient Care (DPC) full time equivalent (FTE) national '
                     'workforce reporting service (NWRS)',
        totals_dimension=totals_dimension,
        metric_description="Number of full time equivalent (FTE) Direct Patient Care (DPC) "
                           "roles in General Practice and PCNs from national workforce "
                           "reporting service (NWRS)",
        short_name='NWRS',
        is_published=IS_PUBLISHED,
        is_sensitive=IS_SENSITIVE
    )
