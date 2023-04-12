from transforms.api import transform, Input, Output
import transforms.verbs.dataframes as D
import myproject.datasets.utils as utils


@transform(
    metrics_output=Output("ri.foundry.main.dataset.ca2e348b-7b80-44e6-9561-e9debc780782",
                          checks=utils.get_metrics_dataset_checks("all")),
    metadata_output=Output("ri.foundry.main.dataset.da08b2ad-6c83-4def-8a66-99fba6375e1f",
                           checks=utils.METADATA_DATASET_CHECKS),
    patient_list_metrics=Input("ri.foundry.main.dataset.bc58902d-305f-47c0-8833-271da6a86b87"),
    patient_list_metadata=Input("ri.foundry.main.dataset.b25e529c-0940-4981-be8b-548133ec1d12"),
    staff_to_patient_rate_metrics=Input("ri.foundry.main.dataset.7ee226f1-8ada-48fe-a40e-2f7ad8db2faa"),
    staff_to_patient_rate_metadata=Input("ri.foundry.main.dataset.114a815f-2cdf-484e-8ea0-d232ada27938"),
    total_gp_staff_w_dpc_collated_metrics=Input("ri.foundry.main.dataset.0ee18c8d-b489-4e0e-b147-295061f30646"),
    total_gp_staff_w_dpc_collated_metadata=Input("ri.foundry.main.dataset.0323e581-5da7-4389-b467-1a2ca4b87276"),
    dpc_collated_metrics=Input("ri.foundry.main.dataset.eb978b5d-a334-45f9-aa96-3cf8c043b22a"),
    dpc_collated_metadata=Input("ri.foundry.main.dataset.d236153e-8f8a-4388-b667-041693e3d3b1")
)
def compute(
            metrics_output, metadata_output,
            patient_list_metrics, patient_list_metadata,
            staff_to_patient_rate_metrics, staff_to_patient_rate_metadata,
            total_gp_staff_w_dpc_collated_metrics, total_gp_staff_w_dpc_collated_metadata,
            dpc_collated_metrics, dpc_collated_metadata):

    all_metric_dfs = [patient_list_metrics.dataframe(),
                      staff_to_patient_rate_metrics.dataframe(),
                      total_gp_staff_w_dpc_collated_metrics.dataframe(),
                      dpc_collated_metrics.dataframe()]

    all_metadata_dfs = [patient_list_metadata.dataframe(),
                        staff_to_patient_rate_metadata.dataframe(),
                        total_gp_staff_w_dpc_collated_metadata.dataframe(),
                        dpc_collated_metadata.dataframe()]

    # Combine all the metrics together
    metrics_output.write_dataframe(D.union_many(
        *all_metric_dfs,
        how="wide"
    ))

    # Combine all the metadata together
    metadata_output.write_dataframe(D.union_many(
        *all_metadata_dfs,
        how="wide"
    ))
