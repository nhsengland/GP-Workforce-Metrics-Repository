from transforms.api import transform_df, Output
import myproject.datasets.utils as utils
from metrics_ontology_shared_code.primary_care_dashboard_utils.pc_utils import create_initial_metadata_df


metadata_rows = [
    [
        'Weighted_Patient_Population',
        'Aggregated General Practice Weighted Patient Population',
        'Aggregated_General_Practice_Weighted_Patient_Population',
        'Published Annex J ICB and General Practice Weighted Population figures',
        'Primary Care',
        'Workforce',
        'count',
        True,
        False,
    ]
]


@transform_df(
    Output("ri.foundry.main.dataset.b25e529c-0940-4981-be8b-548133ec1d12", checks=utils.METADATA_DATASET_CHECKS),
)
def compute(ctx):
    return create_initial_metadata_df(ctx.spark_session, metadata_rows)
