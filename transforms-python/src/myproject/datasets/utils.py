from transforms.api import Check
from transforms import expectations as E
import pyspark.sql.types as T
from pyspark.sql import functions as f, DataFrame
from functools import reduce


METADATA_DATASET_CHECKS = [
    Check(E.primary_key("metric_id"), "Primary Key"),
    Check(
        E.schema().contains(
            {
                "metric_id": T.StringType(),
                "metric_title": T.StringType(),
                "metric_description": T.StringType(),
                "unit": T.StringType(),
                "metric_name": T.StringType()
            }
        ),
        "Minimum Schema"
    ),
    Check(
        E.all(
            E.col("metric_id").non_null(),
            E.col("metric_title").non_null(),
            E.col("metric_name").non_null(),
        ),
        "No Unexpected Nulls"
    )
]


def get_metrics_dataset_checks(level):
    return [
        Check(E.primary_key("datapoint_id"), "Primary Key {}".format(level)),
        Check(
            E.schema().contains(
                {
                    "metric_id": T.StringType(),
                    "location_id": T.StringType(),
                    "timestamp": T.DateType(),
                    "metric_value": T.DoubleType(),
                    "location_type": T.StringType(),
                    "datapoint_id": T.StringType(),
                    "source": T.StringType(),
                    "unit": T.StringType()
                }
            ),
            "Minimum Schema {}".format(level)
        ),
        Check(
            E.all(
                E.col("metric_id").non_null(),
                E.col("location_id").non_null(),
                E.col("timestamp").non_null(),
                E.col("location_type").non_null(),
                E.col("datapoint_id").non_null(),
                E.col("source").non_null()
            ),
            "No Unexpected Nulls {}".format(level)
        )
    ]


def generate_metadata(df, dimension_cols, metric_id_prefix, unit_type, programme_area, programme_sub_area,
                      metric_title, metric_description, is_sensitive, is_published, metric_name=None,
                      totals_dimension=False, short_name=None, technical_description=None, detailed_programme_area=None,
                      description_is_visible=True, tab_reference=None):
    # Generate map of dimension names e.g. {myMetric: dimension_1, ...}
    dimension_aliases = {dimension_name: f'dimension_{ix+1}' for ix, dimension_name in enumerate(dimension_cols)}

    # Drop all columns from input except the specified dimensions via select
    # Rename dimensions using the specified mapping via alias
    # Drop any duplicate rows via distinct
    dimensions_df = df.select(*[f.col(c).alias(dimension_aliases.get(c, c)) for c in dimension_cols]) \
        .distinct()

    revised_dimensions_df = dimensions_df
    if totals_dimension:
        revised_dimensions_df = dimensions_df.withColumn(f'dimension_{len(dimension_cols)+1}',
                                                         f.lit('Total'))

    if not metric_name:
        metric_name = metric_title.replace(' ', '_')

    # Generate a metric_id, metric_title, metric_name and metric_description column
    metrics_df = (
        revised_dimensions_df
        .withColumn('metric_id', f.concat_ws('_', f.lit(metric_id_prefix), *revised_dimensions_df.columns))
        .withColumn('metric_title', f.lit(metric_title))
        .withColumn('metric_name', f.lit(metric_name))
        .withColumn('metric_short_name', f.lit(short_name).cast(T.StringType()))
        .withColumn('metric_description', f.lit(metric_description))
        .withColumn('technical_description', f.lit(technical_description).cast(T.StringType()))
        .withColumn('is_sensitive', f.lit(is_sensitive))
        .withColumn('is_published', f.lit(is_published))
        .withColumn('description_is_visible', f.lit(description_is_visible))
    )

    # Add additional metadata: programme_area, programme_sub_area and unit
    metadata_df = metrics_df.select(
        '*',
        f.lit(programme_area).alias('programme_area'),
        f.lit(programme_sub_area).alias('programme_sub_area'),
        f.lit(unit_type).cast(f.StringType()).alias('unit'),
        f.lit(detailed_programme_area).cast(T.StringType()).alias('detailed_programme_area'),
        f.lit(tab_reference).cast(T.StringType()).alias('tab_reference')
    )

    return metadata_df


def get_metrics(df, date_col, dim_cols, metric_cal, location_dictionary):
    result_temps = []
    for key in location_dictionary:
        result_temp = (
            df
            .groupBy(key, *dim_cols, date_col)
            .agg(metric_cal.alias("metric_value"))
            .withColumnRenamed(key, "location_id")
            .withColumn("location_type", f.lit(location_dictionary[key]))
        )
        result_temps.append(result_temp)
    return reduce(DataFrame.unionByName, result_temps)


def generate_metric_data(df, metadata, metric_id_prefix, date_col, dimension_cols, source_of_data,
                         totals_dimension=False,
                         additional_selection_cols=None):
    """
    `additional_selection_cols` needs to be a list
    """

    totals = f.lit(None)
    if totals_dimension:
        totals = f.lit('Total')

    # FILL IN METRIC_ID
    metrics_df = df.withColumn(
        "metric_id", f.concat_ws(
            "_",
            f.lit(metric_id_prefix), *dimension_cols, totals
            ))
    # JOIN BACK TO METADATA
    metrics_df = metrics_df.join(metadata.select("metric_id", "unit"), "metric_id", 'left')
    # ADD COLUMNS FOR SCHEMA CHECK
    metrics_df = metrics_df.withColumn(
            "source", f.lit(source_of_data)
        ).withColumnRenamed(
            date_col, "timestamp"
        ).withColumn(
            "datapoint_id", f.concat_ws(
                "_",
                f.date_format(f.col("timestamp"), "yyyy-MM-dd"),
                f.col("metric_id"),
                f.col("location_type"),
                f.col("location_id"),
                f.col("source")
                ))

    # ADD A PLACEHOLDER ROLLING 12 MONTH TOTAL
    # don't do the calculation since it will not always be relevant
    if 'metric_value_12monthrolling' not in metrics_df.columns:
        metrics_df = metrics_df.withColumn(
            "metric_value_12monthrolling", f.lit(None).cast(T.DoubleType())
        )

    cols_for_selection = [
        "datapoint_id",
        "location_id",
        "location_type",
        "timestamp",
        "metric_id",
        "metric_value",
        "metric_value_12monthrolling",
        "unit",
        "source"]

    if additional_selection_cols:
        cols_for_selection.append(*additional_selection_cols)

    # SELECT COLS FOR FINAL DF
    return metrics_df.select(*cols_for_selection)


def fin_quarter_to_end_dates(df, dates_ref):
    dates_ref = dates_ref \
        .select("quarter_end", "fin_quarter_no", "fin_year") \
        .dropDuplicates() \
        .withColumn("year_and_quarter", f.concat(dates_ref.fin_year.substr(1, 4),
                                                 f.lit('/'),
                                                 dates_ref.fin_year.substr(5, 2),
                                                 f.lit(' Q'),
                                                 dates_ref.fin_quarter_no)) \
        .drop("fin_quarter_no", "fin_year")

    return df.join(dates_ref, "year_and_quarter", "left").withColumnRenamed("quarter_end", "quarter_end_date")


def generate_percentage_baseline(df):
    """
    Returns a dataset with the `pct_baseline` column, which is (`metric_value`-`baseline`)/`baseline`

    *args*:
      `df` - the dataframe to operate on
    """
    # join the baseline records to the original dataset
    return df.withColumn("percentage_baseline", (f.col("metric_value")-f.col("baseline"))/f.col("baseline"))
