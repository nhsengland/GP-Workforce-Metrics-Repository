"""
DEV17,18,20
"""
from pyspark.sql import functions as f
from transforms.api import transform, Input, Output, configure
import myproject.datasets.utils as utils
from metrics_ontology_shared_code.primary_care_dashboard_utils.pc_utils import get_time_comparison

IS_PUBLISHED = True
IS_SENSITIVE = False


@configure(profile=['ALLOW_ADD_MONTHS'])
@transform(
    metrics_output=Output("ri.foundry.main.dataset.7ee226f1-8ada-48fe-a40e-2f7ad8db2faa",
                          checks=utils.get_metrics_dataset_checks("all")),
    metadata_output=Output("ri.foundry.main.dataset.114a815f-2cdf-484e-8ea0-d232ada27938",
                           checks=utils.METADATA_DATASET_CHECKS),
    dpc_collated_staff_metrics=Input("ri.foundry.main.dataset.eb978b5d-a334-45f9-aa96-3cf8c043b22a"),
    dpc_collated_staff_metadata=Input("ri.foundry.main.dataset.d236153e-8f8a-4388-b667-041693e3d3b1"),
    gp_fte_staff_metrics=Input("ri.foundry.main.dataset.d03145cc-e290-45f5-8606-9ed4716cb56d"),
    gp_fte_staff_metadata=Input("ri.foundry.main.dataset.48fb636f-3f26-4c1c-ac77-1b1e9306bb07"),
    aggregated_weighted_patient_list_metrics=Input(
        "ri.foundry.main.dataset.bc58902d-305f-47c0-8833-271da6a86b87"),
)
def compute(dpc_collated_staff_metrics, dpc_collated_staff_metadata, gp_fte_staff_metrics, gp_fte_staff_metadata,
            aggregated_weighted_patient_list_metrics, metrics_output, metadata_output):

    # Create dataframes from inputs
    dpc_collated_staff_metrics = dpc_collated_staff_metrics.dataframe()
    dpc_collated_staff_metadata = dpc_collated_staff_metadata.dataframe()
    fte_staff_df = gp_fte_staff_metrics.dataframe()
    fte_staff_metadata_df = gp_fte_staff_metadata.dataframe()
    patient_list_df = aggregated_weighted_patient_list_metrics.dataframe()

    metrics, metadata = compute_metrics_and_metadata(dpc_collated_staff_metrics, dpc_collated_staff_metadata,
                                                     fte_staff_df, fte_staff_metadata_df, patient_list_df)

    # Output metrics and metadata
    metrics_output.write_dataframe(metrics)
    metadata_output.write_dataframe(metadata)


def compute_metrics_and_metadata(dpc_collated_staff_metrics, dpc_collated_staff_metadata, fte_staff_df,
                                 fte_staff_metadata_df, patient_list_df):

    # Filter for appropriate metric_id/dimension
    fte_staff_df = fte_staff_df.filter((fte_staff_df['metric_id'] == 'GP_FTE_GP_Total') |
                                       (fte_staff_df['metric_id'] == 'GP_FTE_NURSES_Total'))

    # Add collated DPC staff
    useful_cols = ["location_id", "location_type", "timestamp", "metric_id", "metric_value"]
    fte_staff_df = fte_staff_df.select(*useful_cols) \
                               .unionByName(dpc_collated_staff_metrics.select(*useful_cols))

    # Select appropriate columns for joining later
    fte_staff_df = fte_staff_df.select('location_id', 'location_type', f.col('timestamp').alias('month_end_timestamp'),
                                       'metric_id', f.col('metric_value').alias('metric_value_fte_staff'))
    patient_list_df = patient_list_df.select('location_id', 'location_type',
                                             f.col('timestamp').alias('month_start_timestamp'),
                                             f.col('metric_value').alias('metric_value_patients_weighted'))

    # Attach the relevant staff group dimension to fte staff data
    combined_metadata = fte_staff_metadata_df.select('metric_id', 'dimension_1') \
                                             .unionByName(dpc_collated_staff_metadata.select('metric_id', 'dimension_1'))\
                                             .distinct()

    fte_staff_df = fte_staff_df.join(combined_metadata, 'metric_id') \
                               .select('*', f.col('dimension_1').alias('staff_group'))

    # Join by location ID and location type
    staff_and_patients = fte_staff_df.join(patient_list_df, ['location_id', 'location_type'], 'inner')

    # Perform calculation
    calc = (f.col('metric_value_fte_staff')/f.col('metric_value_patients_weighted'))*10000
    staff_and_patients_w_ratio = staff_and_patients.withColumn('metric_value', calc)

    # Change the names of staff role categories to be displayed as table headings
    staff_and_patients_w_ratio = staff_and_patients_w_ratio.withColumn('staff_group', f.regexp_replace('staff_group',
                                                                                                       'GP - PRACTICE',
                                                                                                       'GP Doctors')) \
                                                           .withColumn('staff_group', f.regexp_replace('staff_group',
                                                                                                       'NURSES - PRACTICE',
                                                                                                       'Nurses'))

    # Create metrics and metadata (including all required fields)

    # Specify values for both metrics and metadata
    metric_id_prefix = "Rate_per_pop_GP_FTE"
    dimension_cols = ['staff_group']
    # Specify values for metadata
    unit_type = None
    programme_area = "Primary Care"
    programme_sub_area = "Workforce"
    metric_title = "Rate of full time equivalent (FTE) staff working in General Practice per 10K weighted population"
    metric_name = "Rate_of_FTE_GP_staff_per_population"

    gps_description = "Rate of full time equivalent (FTE) doctors working in General Practice " + \
                      "per 10,000 weighted patient population"
    nurses_description = "Rate of full time equivalent (FTE) nurses working in General Practice " + \
                         "per 10,000 weighted patient population"
    dpc_description = "Rate of all collated full time equivalent (FTE) Direct Patient Care (DPC) staff " + \
                      "working in General Practice and Primary Care Networks (PCNs) per 10K weighted patient population. " + \
                      "Collated refers to the fact that these figures come from the NHSE/I Workforce team."
    metric_description = f.when(f.col('dimension_1') == "GP Doctors", gps_description) \
                          .when(f.col('dimension_1') == "Nurses", nurses_description) \
                          .when(f.col('dimension_1') == "DPC Collated", dpc_description)
    # Specify values for metrics
    date_col = "month_end_timestamp"
    source_of_data = "TBC"

    # Generate metadata
    metadata = utils.generate_metadata(df=staff_and_patients_w_ratio,
                                       dimension_cols=dimension_cols,
                                       metric_id_prefix=metric_id_prefix,
                                       unit_type=unit_type,
                                       programme_area=programme_area,
                                       programme_sub_area=programme_sub_area,
                                       metric_title=metric_title,
                                       metric_name=metric_name,
                                       metric_description=metric_description,
                                       is_published=IS_PUBLISHED,
                                       is_sensitive=IS_SENSITIVE)

    # Generate metrics
    metrics = utils.generate_metric_data(df=staff_and_patients_w_ratio,
                                         metadata=metadata,
                                         metric_id_prefix=metric_id_prefix,
                                         date_col=date_col,
                                         dimension_cols=dimension_cols,
                                         source_of_data=source_of_data)

    # Add Previous period for Quarter and Month for Summary Screens
    metrics = get_time_comparison(get_time_comparison(metrics, 'month', last_day=True),
                                  'quarter', last_day=True)

    return metrics, metadata
