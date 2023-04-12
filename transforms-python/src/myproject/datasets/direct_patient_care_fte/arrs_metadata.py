from transforms.api import transform_df, Output
import myproject.datasets.utils as utils
from metrics_ontology_shared_code.primary_care_dashboard_utils.pc_utils import create_initial_metadata_df

metadata_rows = [
    [
        'ARRS',
        'Additional Roles Reimbursement Scheme (ARRS) funding trajectory',
        'Additional_Roles_Reimbursement_Scheme_(ARRS)_funding_trajectory',
        'Unpublished PCSE General Practice Weighted Population figures',
        'Primary Care',
        'Workforce',
        'count',
        True,
        False,
        'ARRS',
        'ARRS',
    ]
]


@transform_df(
    Output("ri.foundry.main.dataset.1f8bac78-6260-4bed-9a5f-84b3f9bd4f88",  checks=utils.METADATA_DATASET_CHECKS)
)
def compute(ctx):
    return create_initial_metadata_df(ctx.spark_session, metadata_rows, ['dimension_4', 'metric_short_name'])
