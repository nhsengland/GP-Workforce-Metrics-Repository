from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as f
import transforms.verbs.dataframes as D
import myproject.datasets.utils as utils

dimension_cols = [
    'staff_group',
    'staff_role',
    'detailed_staff_role']
metric_id_prefix = "GP_FTE"
source_of_data = "GP Level Census"
IS_PUBLISHED = True
IS_SENSITIVE = False


@transform_df(
    Output("ri.foundry.main.dataset.48fb636f-3f26-4c1c-ac77-1b1e9306bb07", checks=utils.METADATA_DATASET_CHECKS),
    gp_df=Input("ri.foundry.main.dataset.c5b24d93-aab2-479a-b216-36f10fcb3a12"),
)
def compute(gp_df):

    metadata_sets = []
    metadata_sets.append(getMetadata(gp_df, dimension_cols, False))
    metadata_sets.append(getMetadata(gp_df, dimension_cols[0:2], True))
    metadata_sets.append(getMetadata(gp_df, dimension_cols[0:1], True))
    metadata_sets.append(getMetadata(gp_df, [], True))

    combined_metadata = D.union_many(*metadata_sets, how="wide")

    # Adding Practice only suffix
    combined_metadata = combined_metadata.withColumn('dimension_1',
                                                     f.when((combined_metadata['dimension_1'] == 'DIRECT PATIENT CARE'),
                                                            f.lit('DIRECT PATIENT CARE - PRACTICE'))
                                                     .when((combined_metadata['dimension_1'] == 'ADMIN/NON-CLINICAL'),
                                                           f.lit('ADMIN/NON-CLINICAL - PRACTICE'))
                                                     .when((combined_metadata['dimension_1'] == 'GP'),
                                                           f.lit('GP - PRACTICE'))
                                                     .when((combined_metadata['dimension_1'] == 'NURSES'),
                                                           f.lit('NURSES - PRACTICE'))
                                                     .otherwise(combined_metadata['dimension_1']))

    return combined_metadata.withColumn('metric_short_name',
                                        f.when((combined_metadata['dimension_1'] == 'GP - PRACTICE')
                                               & (combined_metadata['dimension_2'] == 'Total'),
                                               f.lit('GP Doctors'))
                                         .when((combined_metadata['dimension_1'] == 'NURSES - PRACTICE')
                                               & (combined_metadata['dimension_2'] == 'Total'),
                                               f.lit('Nurses'))
                                         .otherwise(f.lit(None)))


def getMetadata(gp_df, cols, totals_dimension):
    metric_description = f.concat(f.lit('The sum of full time equivalent (FTE) '),
                                  f.lower(f.col('dimension_1')),
                                  f.lit(' staff working in General Practice'))
    return utils.generate_metadata(df=gp_df,
                                   dimension_cols=cols,
                                   metric_id_prefix=metric_id_prefix,
                                   unit_type="FTE",
                                   programme_area='Primary Care',
                                   programme_sub_area='Workforce',
                                   metric_name='GP_Full_Time_Equivalent_(FTE)',
                                   metric_title='GP Full Time Equivalent (FTE)',
                                   totals_dimension=totals_dimension,
                                   metric_description=metric_description,
                                   is_sensitive=IS_SENSITIVE,
                                   is_published=IS_PUBLISHED)
