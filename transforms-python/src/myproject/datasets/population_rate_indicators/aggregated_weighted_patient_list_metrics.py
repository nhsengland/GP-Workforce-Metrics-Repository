from pyspark.sql import functions as f
from transforms.api import transform_df, Input, Output
import myproject.datasets.utils as utils


@transform_df(
    Output("ri.foundry.main.dataset.bc58902d-305f-47c0-8833-271da6a86b87",
           checks=utils.get_metrics_dataset_checks("all")),
    metadata_df=Input("ri.foundry.main.dataset.b25e529c-0940-4981-be8b-548133ec1d12",
                      checks=utils.METADATA_DATASET_CHECKS),
    weighted_regs_by_gp=Input("ri.foundry.main.dataset.af296d5e-9c0d-4854-b58c-f3bbf8cbfd28"),
    icb_weighted_pop=Input("ri.foundry.main.dataset.a068fbd5-4dac-4592-bcfa-1f798233d5d0"),
    all_locations_df=Input("ri.foundry.main.dataset.c4625196-f6bc-4840-bf49-b63b7e1390d0")
)
def compute(icb_weighted_pop, weighted_regs_by_gp, all_locations_df, metadata_df):

    # Get max effective snapshot to align with ICB, get metric for Overall GP practice weighted population
    weighted_regs_by_gp = weighted_regs_by_gp.filter(f.col("effective_snapshot_date") \
                                     == weighted_regs_by_gp.select(f.max(f.col("effective_snapshot_date"))).collect()[0][0]) \
                                     .where(f.col('metric_name') == 'Overall GP practice weighted population')

    # filter by Overall ICB  weighted population metric and remove region/country granularity
    icb_weighted_pop = icb_weighted_pop.filter(f.col('metric') == 'Overall ICB  weighted population')\
                                       .where(f.col('region_code') != f.col('icb_code'))

    # Map the GP and icb entries to other location granularities
    weighted_regs_by_gp_w_locations = join_locations(weighted_regs_by_gp, all_locations_df)
    icb_weighted_pop_w_locations = join_locations(icb_weighted_pop, all_locations_df)

    # Perform the aggregation of the original dataset at the specified location granularities
    date_col = 'effective_snapshot_date'
    dim_cols = []
    metric_calc = f.sum('metric_value').cast("Double")

    # In the loc_dict, key is the column name for the relevant granularity represented by the value
    loc_dict_regs_gp = {
                "current_ccg_code": "nhs-ccg",
                "pcn_code": "nhs-pcn",
                "gp_practice_code": "nhs-gp"}

    loc_dict_icb_pop = {"country_id": "nhs-country",
                "nhs_region_id": "nhs-region",
                "stp_code": "nhs-stp"}

    # get respective metrics
    aggregated_weighted_regs_by_gp = utils.get_metrics(weighted_regs_by_gp_w_locations, date_col, dim_cols,
                                                         metric_calc, loc_dict_regs_gp)
    aggregated_icb_weighted_pop_w_locations = utils.get_metrics(icb_weighted_pop_w_locations, date_col, dim_cols,
                                                         metric_calc, loc_dict_icb_pop)

    # Specify names needed for metric creation
    metric_id_prefix = "Weighted_Patient_Population"
    date_col = "effective_snapshot_date"
    source_of_data = "Weighted Patient List"

    # Aggregate ICB and Weighted Regs by GP
    aggregated_gp_and_icb = aggregated_weighted_regs_by_gp.unionByName(aggregated_icb_weighted_pop_w_locations)

    # Generate metrics
    metrics = utils.generate_metric_data(aggregated_gp_and_icb, metadata_df, metric_id_prefix,
                                         date_col, dim_cols, source_of_data)

    return metrics


def join_locations(df, all_locations):
    """
        Returns the input data set supplemented with location codes.
        Maps from GP practice to CCG, STP, Regional and National.

        *args*:
          `df` - the dataframe to operate on
          `all_locations` - the dataframe containing the location code mappings
    """

    if('gp_practice_code' in df.columns):
        # Keep the columns of the relevant location types
        locations = all_locations.filter(f.col('object_id') == 'nhs-gp')\
                                 .select('object_id', 'organisation_code', 'pcn_code', 'ccg_code')\

        # Add the current CCG codes so any entries referring to old ccg's can still be used
        current_ccgs = all_locations.filter(all_locations['object_id'] == 'nhs-ccg') \
                                    .select('object_id', 'ccg_code', 'current_ccg_code') \
                                    .drop('object_id')
        locations = locations.join(current_ccgs, 'ccg_code', 'left')

        # Join on locations data
        df = df.join(locations, df['gp_practice_code'] == locations['organisation_code'], "left") \
            .drop('gp_practice_code')\
            .withColumnRenamed('organisation_code', 'gp_practice_code')\
            .distinct()

        # Handle null locations
        df = df.fillna('Unknown', subset=['gp_practice_code', 'pcn_code', 'ccg_code', 'current_ccg_code'])

    elif('icb_code' in df.columns):
        # Map the input dataframes locations to the other location types
        locations = all_locations.select('stp_code', 'object_id', 'nhs_region_id', 'country_id') \
                                 .filter(f.col('object_id') == 'nhs-stp')

        # join icb_code column on stp_code
        df = df.join(locations, (df['icb_code'] == locations['stp_code']), "left").drop('icb_code')

        # Handle null locations
        df = df.fillna('Unknown', subset=['stp_code', 'nhs_region_id']) \
            .fillna({'country_id': "england"})

    else:
        raise ValueError("Unexpected input data column names")
        # raise exception for column name mismatch

    return df
