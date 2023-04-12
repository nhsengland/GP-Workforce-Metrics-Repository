"""
DEV26A
"""
from pyspark.sql import functions as f
from transforms.api import transform, Input, Output
import myproject.datasets.utils as utils

date_col = "effective_snapshot_date"
IS_PUBLISHED = True
IS_SENSITIVE = False
metric_id_prefix = "GP_Practice_FTE"
source_of_data = "Practice Level Census Data"


@transform(
    output_metrics=Output("ri.foundry.main.dataset.f9c387d2-0286-487f-b5ed-af499db4f4cd",
                          checks=utils.get_metrics_dataset_checks("all")),
    output_metadata=Output("ri.foundry.main.dataset.bb494767-5ea2-43e8-97d9-a6fddb9f63ba",
                           checks=utils.METADATA_DATASET_CHECKS),
    gp_census=Input("ri.foundry.main.dataset.cb8ec018-83b7-41ee-820a-e9e2f2f9fa06"),
    practice=Input("ri.foundry.main.dataset.84845362-efbf-4923-8064-7b439be7271d")
    )
def compute(output_metrics, output_metadata, gp_census, practice):
    metrics, metadata = perform_compute(gp_census.dataframe(), practice.dataframe())
    output_metadata.write_dataframe(metadata)
    output_metrics.write_dataframe(metrics)


def perform_compute(gp_census, practice):
    # Join to Practice data on Location Ontology
    gp_census = gp_census.filter(f.col("effective_snapshot_date") ==
                                 gp_census.select(f.max(f.col("effective_snapshot_date"))).collect()[0][0])
    gp_census = gp_census.join(practice.select("organisation_code"),
                               gp_census.practice_code == practice.organisation_code, "inner")

    # Filter gp census to keep just TOTAL_GP_FTE
    gp_census = gp_census.filter(gp_census.measure == "TOTAL_GP_FTE")\
                         .withColumn("dimension_1", f.lit("GP"))

    # Perform Aggregattion
    dim_cols = ["dimension_1"]
    metric_cal = f.sum("measure_value").cast("double")
    location_dictionary = {"organisation_code": "nhs-gp"}
    gp_df = utils.get_metrics(gp_census, date_col, dim_cols, metric_cal, location_dictionary)

    metadata = utils.generate_metadata(
            df=gp_df,
            dimension_cols=dim_cols,
            metric_id_prefix=metric_id_prefix,
            unit_type='FTE',
            programme_area='Primary Care',
            programme_sub_area='Workforce',
            metric_title='GP Full Time Equivalent (FTE) - Practice level',
            totals_dimension=True,
            metric_description="The number of full time equivalent (FTE) staff working in "
                               "General Practice - Practice level",
            is_published=IS_PUBLISHED,
            is_sensitive=IS_SENSITIVE
        )

    metrics = utils.generate_metric_data(gp_df, metadata, metric_id_prefix, date_col, dim_cols, source_of_data,
                                         totals_dimension=True,
                                         additional_selection_cols=None)

    return metrics, metadata
