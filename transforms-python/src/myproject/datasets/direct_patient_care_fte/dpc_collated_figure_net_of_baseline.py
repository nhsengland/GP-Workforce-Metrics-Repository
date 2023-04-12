from transforms.api import transform, Input, Output
import myproject.datasets.utils as utils
from pyspark.sql import functions as f
import transforms.verbs.dataframes as D
import myproject.datasets.direct_patient_care_fte.dpc_utils as dpc_utils
from myproject.datasets.direct_patient_care_fte.dpc_nwrs import dimension_cols, date_col, source_of_data, unit, loc_dict
from metrics_ontology_shared_code.primary_care_dashboard_utils.pc_utils import join_locations

metric_id_prefix = "DPC_FTE_Collated_Net_of_Baseline"
national_gp_target = 26000
IS_PUBLISHED = True
IS_SENSITIVE = False


@transform(
    output_metrics=Output("ri.foundry.main.dataset.de6166c3-f8ab-4bf1-8f48-1e546f5065ef",
                          checks=utils.get_metrics_dataset_checks("all")
                          ),
    output_metadata=Output("ri.foundry.main.dataset.e44ec721-b125-411a-a15b-b9c557b67343",
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

    df_dpc = df_dpc.withColumn("dimension_1", f.lit("DPC Collated Net of Baseline"))
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
    wo_baseline_sets = [generate_metrics(df_dpc, dimension_cols, dpc_metadata, False),
                        generate_metrics(df_dpc, dimension_cols[0:3], dpc_metadata, True),
                        generate_metrics(df_dpc, dimension_cols[0:2], dpc_metadata, True),
                        generate_metrics(df_dpc, dimension_cols[0:1], dpc_metadata, True)]
    without_baseline = D.union_many(*wo_baseline_sets, how="wide")
    df_dpc.unpersist()

    # join to metadata to use dimensions cols for creating target value at national level and by top dimension
    metric_table = without_baseline.join(dpc_metadata.select('metric_id', 'dimension_1', 'dimension_2',
                                                             'dimension_3', 'dimension_4'), ['metric_id'], 'left')

    # add target and progress to national level and top dimension. Conditions are in dpc_utils.get_taget_condition
    metric_table = dpc_utils.generate_target(metric_table, "DPC Collated Net of Baseline")

    # drop unnessary columns
    metric_table = metric_table.drop('dimension_1', 'dimension_2', 'dimension_3', 'dimension_4').repartition(1)

    return metric_table, dpc_metadata


def perform_aggregation(df, dimensions):
    """
    Returns a dataset with the aggregations calculated based on the provided dimensions.

    *args*:
      `df` - the dataframe to operate on
      `dimensions` - the dimensions to perform the metrics on
    """

    # SPECIFIC METRIC CALCULATION
    metrics_calc = f.sum("collated_figure_net_of_baseline").cast("double")

    # CUSTOM LOGIC/ FILTER
    return utils.get_metrics(df, date_col, dimensions, metrics_calc, loc_dict)


def generate_metrics(df, dim_cols, df_metadata, totals_dimension=False):
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
    aggregated_data = perform_aggregation(df, dim_cols)

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
        metric_title='Direct Patient Care (DPC) full time equivalent (FTE) collated figures '
                     'net of baseline',
        totals_dimension=totals_dimension,
        metric_description="Full time equivalent (FTE) Direct Patient Care (DPC) roles in "
                           "General Practice and PCNs for collated figures net of March "
                           "2019 baseline",
        short_name="Collated Net of Baseline",
        is_published=IS_PUBLISHED,
        is_sensitive=IS_SENSITIVE
    )
