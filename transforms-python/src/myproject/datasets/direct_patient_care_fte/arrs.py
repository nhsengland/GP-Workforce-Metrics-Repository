from transforms.api import transform, Input, Output
import myproject.datasets.utils as utils
from pyspark.sql import functions as f

date_col = "financial_year_end_date"
metric_id_prefix = "ARRS_Funding_Trajectory"
source_of_data = "arrs_funding_trajectory_total_weighted_population"
unit = "count"
loc_dict = {"country_id": "nhs-country",
            "nhs_region_id": "nhs-region"}


@transform(
    output_metrics=Output("ri.foundry.main.dataset.ca8fbbf5-2147-4686-a6c6-8423bfb97f3d",
                          checks=utils.get_metrics_dataset_checks("all")
                          ),
    metadata=Input("ri.foundry.main.dataset.1f8bac78-6260-4bed-9a5f-84b3f9bd4f88"),
    arrs=Input("ri.foundry.main.dataset.243c73f6-c63b-4a47-a729-61d4e3b24be0"),
    region=Input("ri.foundry.main.dataset.3743b676-88c3-458c-9699-38abf93aac1a"),
    ref_dates=Input("ri.foundry.main.dataset.36abfcca-6f49-49f5-a71a-6d2fdfa43244")
    )
def compute(output_metrics, metadata, arrs, region, ref_dates):
    arrs, region, ref_dates = arrs.dataframe(), region.dataframe(), ref_dates.dataframe()
    metadata = metadata.dataframe()

    # As raw table contains both regional & country level records, we will keep only regional records & aggregate up
    # then join to region table from Location Ontology
    arrs = arrs \
        .filter(arrs.region_name != "England") \
        .select("region_code", "financial_year", "year_end_trajectory") \
        .join(region.select("nhs_region_id", "national_grouping", "nhs_region_name", "country_id"),
              arrs.region_code == region.national_grouping, "left") \
        .drop("national_grouping")

    # Raw data is given in financial year basis so adding 'financial_year_end_date' column
    arrs = fin_year_to_end_dates(arrs, ref_dates)

    metric_calc = f.sum('year_end_trajectory').cast("double")

    arrs = utils.get_metrics(arrs, date_col, [], metric_calc, loc_dict)

    # As there's only one row for metadata, it was created via a fusion sheet in metadata folder
    # Generate metrics
    metrics = utils.generate_metric_data(arrs, metadata, metric_id_prefix, date_col, [], source_of_data)

    output_metrics.write_dataframe(metrics)


def fin_year_to_end_dates(df, dates_ref):
    dates_ref = dates_ref \
        .select("quarter_end", "fin_quarter_no", "fin_year") \
        .filter(dates_ref.fin_quarter_no == 4) \
        .dropDuplicates() \
        .withColumn("financial_year", f.concat(dates_ref.fin_year.substr(1, 4),
                                               f.lit('_'),
                                               dates_ref.fin_year.substr(5, 2))) \
        .drop("fin_quarter_no")

    return df.join(dates_ref, "financial_year", "left").withColumnRenamed("quarter_end", "financial_year_end_date")
