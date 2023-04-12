from transforms.api import transform, Input, Output
import transforms.verbs.dataframes as D
import myproject.datasets.utils as utils


@transform(
    output_metrics=Output("ri.foundry.main.dataset.95a5b9e5-61d7-4557-976d-a83ea9434c81",
                          checks=utils.get_metrics_dataset_checks("all")),
    output_metadata=Output("ri.foundry.main.dataset.70529c1d-1c5b-4a22-b263-cd8137e3572b",
                           checks=utils.METADATA_DATASET_CHECKS),

    dpc_nwrs_metrics=Input("ri.foundry.main.dataset.a8155408-7f7d-4967-944d-3db60daeb81c"),
    dpc_nwrs_metadata=Input("ri.foundry.main.dataset.66b6e57c-dd68-4695-a022-d8e2982c1f27"),

    dpc_collated_metrics=Input("ri.foundry.main.dataset.4380414f-3551-4ad4-b17c-63e23cc7c4d0"),
    dpc_collated_metadata=Input("ri.foundry.main.dataset.f69c73a9-ad6a-4906-805c-0e511d21bfba"),

    dpc_uplift_metrics=Input("ri.foundry.main.dataset.2d6d60f9-f26e-433e-8ec4-d79193bf0b1f"),
    dpc_uplift_metadata=Input("ri.foundry.main.dataset.5185ea32-eba5-4d78-a131-8ae89b7de6ed"),

    dpc_nwrs_net_of_baseline_metrics=Input("ri.foundry.main.dataset.d6714d3d-952c-4ff4-90be-3caf7967aeff"),
    dpc_nwrs_net_of_baseline_metadata=Input("ri.foundry.main.dataset.918c6de9-88fb-4a26-990a-4de3402be5fe"),

    dpc_collated_net_of_baseline_metrics=Input("ri.foundry.main.dataset.de6166c3-f8ab-4bf1-8f48-1e546f5065ef"),
    dpc_collated_net_of_baseline_metadata=Input("ri.foundry.main.dataset.e44ec721-b125-411a-a15b-b9c557b67343")

)
def compute(output_metrics, output_metadata,
            dpc_nwrs_metrics, dpc_nwrs_metadata,
            dpc_collated_metrics, dpc_collated_metadata,
            dpc_uplift_metrics, dpc_uplift_metadata,
            dpc_nwrs_net_of_baseline_metrics, dpc_nwrs_net_of_baseline_metadata,
            dpc_collated_net_of_baseline_metrics, dpc_collated_net_of_baseline_metadata):

    # combine all the metrics datasets together
    combined_metrics = D.union_many(
                    dpc_nwrs_metrics.dataframe(),
                    dpc_collated_metrics.dataframe(),
                    dpc_uplift_metrics.dataframe(),
                    dpc_nwrs_net_of_baseline_metrics.dataframe(),
                    dpc_collated_net_of_baseline_metrics.dataframe(),
                    how='wide')

    # combine all the metadata datasets together
    combined_metadata = D.union_many(
                    dpc_nwrs_metadata.dataframe(),
                    dpc_collated_metadata.dataframe(),
                    dpc_uplift_metadata.dataframe(),
                    dpc_nwrs_net_of_baseline_metadata.dataframe(),
                    dpc_collated_net_of_baseline_metadata.dataframe(),
                    how='wide')

    output_metrics.write_dataframe(combined_metrics)
    output_metadata.write_dataframe(combined_metadata)
