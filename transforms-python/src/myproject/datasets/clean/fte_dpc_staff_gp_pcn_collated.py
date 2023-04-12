from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F, types as T
import myproject.datasets.utils as utils
import transforms.verbs.dataframes as D


@transform_df(
    Output("ri.foundry.main.dataset.bfd0ff62-354f-40d8-811f-034443c500dc"),
    fte_dpc=Input("ri.foundry.main.dataset.b1eb8321-d3ac-4065-bb16-260dee8a9dc1"),
    fte_dpc_ukhf=Input("ri.foundry.main.dataset.d02069f4-5bbf-49bd-908a-b3a98b1fed76"),
    ref_dates=Input("ri.foundry.main.dataset.36abfcca-6f49-49f5-a71a-6d2fdfa43244"),
    role_mapping=Input("ri.foundry.main.dataset.418ce0e1-c49f-445f-bc74-87b79fab7bfe")
)
def compute(fte_dpc, fte_dpc_ukhf, role_mapping, ref_dates):
    # merge the new feed of dpc data: fte_dpc_ukhf into fte_dpc
    mutual_cols = [col for col in fte_dpc.columns if col in fte_dpc_ukhf.columns]
    fte_dpc = fte_dpc.select(*mutual_cols, "2018_19_q4_baseline", "role_name", "year_and_quarter")
    fte_dpc = utils.fin_quarter_to_end_dates(fte_dpc, ref_dates)
    # Within this dataset, the data after 30/09/2021 has been revised and published by UKHF. Hence combining the two datasets below.
    fte_dpc = fte_dpc.filter("quarter_end_date < date'2021-09-30'") \
                     .withColumnRenamed("quarter_end_date", "effective_snapshot_date") \
                     .withColumnRenamed("2018_19_q4_baseline", "baseline_value") \
                     .withColumn("baseline_date", F.lit("2018/19_Q4"))

    fte_dpc_ukhf = fte_dpc_ukhf.withColumnRenamed("staff_role", "role_name")

    num_cols = ["all_nwrs_absolute", "all_nwrs_net_of_baseline", "collated_figure_absolute", "collated_figure_net_of_baseline", "baseline_value"]
    for col in num_cols: # noqa
        fte_dpc = fte_dpc.withColumn(col, F.col(col).cast(T.DoubleType()))
        fte_dpc_ukhf = fte_dpc_ukhf.withColumn(col, F.col(col).cast(T.DoubleType()))

    fte_dpc = D.union_many(fte_dpc, fte_dpc_ukhf, how='wide')

    # adjust new arrs values to old values (NWRS-only to Non-ARRS, NWRS / ARRS claims portal to ARRS)
    fte_dpc = adjust_new_arrs_values(fte_dpc)

    # Create a new column cleaned_role_name and keep cleaned role_names
    fte_dpc = clean_role_name(fte_dpc)

    # Get staff_role
    fte_dpc = get_staff_role(fte_dpc, role_mapping)

    # Renaming Proposed_staff_grouping column to staff_role
    fte_dpc = fte_dpc.withColumnRenamed('Proposed_staff_grouping', 'staff_role')\
                     .withColumn('arrs', F.when(F.col("role_name") == "Physiotherapists", 'Non-ARRS')
                                          .otherwise(F.col("arrs"))
                                 )

    return fte_dpc


def clean_role_name(fte_dpc):
    # Create cleaned_role_name column and change all values to UPPER case
    # Trim 's' from the end of the role_name wherever applicable apart from the column mentioned
    # in the list role_name_vals_not_to_change
    # Change 'Other Direct Patient Care' to 'OTHER'

    # These role_names do not change
    role_name_vals_not_to_change = ['Health and Wellbeing Coaches', 'High Intensity Therapists']

    return fte_dpc.withColumn(
                                            "cleaned_role_name",
                                            F.when(
                                                F.col("role_name").isin(role_name_vals_not_to_change),
                                                F.upper(F.col("role_name"))
                                            ).when(
                                                F.col("role_name") == 'Other Direct Patient Care',
                                                'OTHER'
                                            ).otherwise(
                                                F.upper(F.expr("rtrim('s', rtrim(role_name))"))
                                            )
                                    )


def adjust_new_arrs_values(fte_dpc):
    return fte_dpc.withColumn("arrs", F.when(F.col("arrs") == "NWRS-only",
                                             "Non-ARRS")
                                       .when(F.col("arrs") == "NWRS / ARRS claims portal",
                                             "ARRS")
                                       .otherwise(F.col("arrs"))
                              )


def get_staff_role(fte_dpc, role_mapping):
    # Do left join with role_mapping based on cleaned_role_name and detailed_staff_role to get staff_role
    # Insert 'Unknown' for staff_role wherever it does not match

    # Dealing with dietician and dieticians
    fte_dpc = fte_dpc.withColumn("role_name", F.when(F.col("role_name") == "Dietician", "Dieticians")
                                               .otherwise(F.col("role_name")))

    cleaned_role_name_to_be_filtered = ['MENTAL HEALTH']
    role_mapping = role_mapping.withColumn("Proposed_staff_grouping", F.upper("Proposed_staff_grouping"))\
                               .withColumn("Proposed_staff_grouping", F.when(F.col("Proposed_staff_grouping") == "PERSONALISED CARE ROLES",
                                                                             "PERSONALISED CARE")
                                                                       .otherwise(F.col("Proposed_staff_grouping"))
                                           )

    # Updating Pharmacy -> Pharmacy Roles Mapping in the Proposed_staff_grouping
    role_mapping = role_mapping.withColumn("Proposed_staff_grouping", F.when(F.col("Proposed_staff_grouping") == "PHARMACY", "PHARMACY ROLES")
                                                                       .otherwise(F.col("Proposed_staff_grouping")))

    return fte_dpc.join(
                                    role_mapping.select('STAFF_ROLE', 'Proposed_staff_grouping').distinct(),
                                    fte_dpc.role_name == role_mapping.STAFF_ROLE,
                                    'left'
                                ).drop('STAFF_ROLE')\
                                 .fillna({'Proposed_staff_grouping': 'UNCATEGORISED'})\
                                 .filter(~(F.col('Proposed_staff_grouping').isin(cleaned_role_name_to_be_filtered)))
