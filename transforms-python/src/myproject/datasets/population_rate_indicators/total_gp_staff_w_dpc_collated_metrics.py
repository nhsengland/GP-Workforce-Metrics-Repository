"""
DEV19
"""
from pyspark.sql import functions as f
from transforms.api import transform, Input, Output, configure
import myproject.datasets.utils as utils
from metrics_ontology_shared_code.primary_care_dashboard_utils.pc_utils import get_time_comparison

IS_PUBLISHED = True
IS_SENSITIVE = False


@configure(profile=['ALLOW_ADD_MONTHS'])
@transform(
    metrics_output=Output("ri.foundry.main.dataset.0ee18c8d-b489-4e0e-b147-295061f30646",
                          checks=utils.get_metrics_dataset_checks("all")),
    metadata_output=Output("ri.foundry.main.dataset.0323e581-5da7-4389-b467-1a2ca4b87276",
                           checks=utils.METADATA_DATASET_CHECKS),
    gp_fte_staff_metrics=Input("ri.foundry.main.dataset.d03145cc-e290-45f5-8606-9ed4716cb56d"),
    dpc_collated_metrics=Input("ri.foundry.main.dataset.eb978b5d-a334-45f9-aa96-3cf8c043b22a")
)
def compute(gp_fte_staff_metrics, dpc_collated_metrics, metrics_output, metadata_output):

    # Create dataframes from inputs
    gp_fte_staff_metrics = gp_fte_staff_metrics.dataframe()
    dpc_collated_metrics = dpc_collated_metrics.dataframe()

    metrics, metadata = compute_metrics_and_metadata(gp_fte_staff_metrics,
                                                     dpc_collated_metrics)

    # Output metrics and metadata
    metrics_output.write_dataframe(metrics)
    metadata_output.write_dataframe(metadata)


def compute_metrics_and_metadata(gp_fte_staff_metrics, dpc_collated_metrics):
    # Filter for appropriate rows and columns
    key_columns = ['location_id', 'location_type', 'timestamp']
    non_dpc_roles = ['GP_FTE_ADMIN/NON-CLINICAL_Total', 'GP_FTE_GP_Total', 'GP_FTE_NURSES_Total']

    dpc_collated_metrics = prepare_source_dataset('dpc_adj', dpc_collated_metrics, key_columns,
                                                  ['GP_FTE_DPC Collated'])

    gp_fte_staff = prepare_source_dataset('gp_fte', gp_fte_staff_metrics, key_columns,
                                          non_dpc_roles)

    # Pivot out the different non-DPC GP roles
    gp_fte_staff = gp_fte_staff.groupBy(key_columns) \
                               .pivot('metric_id_gp_fte').sum('metric_value_gp_fte')

    # Join by location ID, location type and timestamp
    all_roles = gp_fte_staff.join(dpc_collated_metrics, key_columns, 'inner')

    # Perform calculation
    # NOTE: uses python sum not pyspark sum
    cols_to_sum = [*non_dpc_roles, 'metric_value_dpc_adj']
    sum_all_roles = all_roles.withColumn('metric_value', sum([f.col(col_name) for col_name in cols_to_sum]))

    # Add a dimension column
    sum_all_roles = sum_all_roles.withColumn('dimension_1', f.lit('Total (inc. DPC – Collated figure)'))

    # Specify values for both metrics and metadata
    metric_id_prefix = "GP_FTE_w_DPC_Collated"
    dimension_cols = ["dimension_1"]
    # Specify values for metadata
    unit_type = "FTE"
    programme_area = "Primary Care"
    programme_sub_area = "Workforce"
    metric_title = "GP full time equivalent (FTE) with DPC collated"
    metric_name = "GP_FTE_with_DPC_Collated"
    metric_description = "The number of full time equivalent (FTE) staff working in General Practice" + \
                         "with direct patient care collated"
    # Specify values for metrics
    date_col = "timestamp"
    source_of_data = "TBC"

    # Generate metadata
    metadata = utils.generate_metadata(df=sum_all_roles, dimension_cols=dimension_cols,
                                       metric_id_prefix=metric_id_prefix,
                                       unit_type=unit_type, programme_area=programme_area,
                                       programme_sub_area=programme_sub_area, metric_title=metric_title,
                                       metric_name=metric_name, metric_description=metric_description,
                                       is_published=IS_PUBLISHED, is_sensitive=IS_SENSITIVE)

    # Generate metrics
    metrics = utils.generate_metric_data(df=sum_all_roles, metadata=metadata, metric_id_prefix=metric_id_prefix,
                                         date_col=date_col, dimension_cols=dimension_cols,
                                         source_of_data=source_of_data)

    # Add Previous period for Summary Screens
    metrics = get_time_comparison(metrics, 'quarter', last_day=True)

    return metrics, metadata


def prepare_source_dataset(alias, metrics, required_cols, required_metric_ids):
    df = metrics.filter(metrics['metric_id'].isin(required_metric_ids)) \
                        .select(*required_cols,
                                f.col('metric_value').alias('metric_value_'+alias),
                                f.col('metric_id').alias('metric_id_'+alias))
    return df
