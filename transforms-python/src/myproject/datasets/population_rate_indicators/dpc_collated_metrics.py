"""
DEV19A
"""
from pyspark.sql import functions as f
from transforms.api import transform, Input, Output
import myproject.datasets.utils as utils

IS_PUBLISHED = True
IS_SENSITIVE = False


@transform(
    metrics_output=Output("ri.foundry.main.dataset.eb978b5d-a334-45f9-aa96-3cf8c043b22a",
                          checks=utils.get_metrics_dataset_checks("all")),
    metadata_output=Output("ri.foundry.main.dataset.d236153e-8f8a-4388-b667-041693e3d3b1",
                           checks=utils.METADATA_DATASET_CHECKS),
    dpc_staff_nwrs_metrics=Input("ri.foundry.main.dataset.a8155408-7f7d-4967-944d-3db60daeb81c"),
    dpc_staff_uplift_metrics=Input("ri.foundry.main.dataset.2d6d60f9-f26e-433e-8ec4-d79193bf0b1f"),
)
def compute(dpc_staff_nwrs_metrics, dpc_staff_uplift_metrics, metrics_output, metadata_output):

    # Create dataframes from inputs
    dpc_staff_nwrs_metrics = dpc_staff_nwrs_metrics.dataframe()
    dpc_staff_uplift_metrics = dpc_staff_uplift_metrics.dataframe()

    metrics, metadata = compute_metrics_and_metadata(dpc_staff_nwrs_metrics,
                                                     dpc_staff_uplift_metrics,)

    # Output metrics and metadata
    metrics_output.write_dataframe(metrics)
    metadata_output.write_dataframe(metadata)


def compute_metrics_and_metadata(dpc_staff_nwrs_metrics, dpc_staff_uplift_metrics):
    # Filter for appropriate rows and columns
    key_columns = ['location_id', 'location_type', 'timestamp']

    dpc_staff_nwrs = prepare_source_dataset('dpc_nwrs', dpc_staff_nwrs_metrics, key_columns,
                                            ['DPC_FTE_NWRS_DPC NWRS_Total'])

    dpc_staff_uplift = prepare_source_dataset('dpc_uplift', dpc_staff_uplift_metrics, key_columns,
                                              ['DPC_FTE_Uplift_from_Collated_DPC Uplift from Collated_Total'])

    # Join by location ID, location type and timestamp
    dpc_values = dpc_staff_nwrs.join(dpc_staff_uplift, key_columns, 'inner')

    # Perform calculation
    calc = f.col('metric_value_dpc_nwrs') + f.col('metric_value_dpc_uplift')
    dpc_collated = dpc_values.withColumn('metric_value', calc)

    # Add a dimension column
    dpc_collated = dpc_collated.withColumn('dimension_1', f.lit('DPC Collated'))

    # Specify values for both metrics and metadata
    metric_id_prefix = "GP_FTE"
    dimension_cols = ["dimension_1"]
    # Specify values for metadata
    unit_type = "FTE"
    programme_area = "Primary Care"
    programme_sub_area = "Workforce"
    metric_title = "Collated Direct Patient Care (DPC) full time equivalent (FTE)"
    metric_description = "The number of full time equivalent (FTE) DPC staff working in General Practice " + \
                         "and Primary Care Networks (PCNs). Collated refers to the fact that these figures " + \
                         "come from the NHSE/I Workforce team."
    # Specify values for metrics
    date_col = "timestamp"
    source_of_data = "TBC"

    # Generate metadata
    metadata = utils.generate_metadata(df=dpc_collated, dimension_cols=dimension_cols,
                                       metric_id_prefix=metric_id_prefix,
                                       unit_type=unit_type, programme_area=programme_area,
                                       programme_sub_area=programme_sub_area, metric_title=metric_title,
                                       metric_description=metric_description,
                                       is_published=IS_PUBLISHED,
                                       is_sensitive=IS_SENSITIVE)

    # Generate metrics
    metrics = utils.generate_metric_data(df=dpc_collated, metadata=metadata, metric_id_prefix=metric_id_prefix,
                                         date_col=date_col, dimension_cols=dimension_cols,
                                         source_of_data=source_of_data)

    return metrics, metadata


def prepare_source_dataset(alias, metrics, required_cols, required_metric_ids):
    df = metrics.filter(metrics['metric_id'].isin(required_metric_ids)) \
                        .select(*required_cols,
                                f.col('metric_value').alias('metric_value_'+alias),
                                f.col('metric_id').alias('metric_id_'+alias))
    return df
