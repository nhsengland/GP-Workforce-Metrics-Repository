# Primary Care Development Documentation


# Business Analysis
## Metrics Definitions
The document containing the metrics definitions can be found [here](https://nhsengland.sharepoint.com/:x:/r/sites/PrimaryCareDataforImprovement-MVP/_layouts/15/Doc.aspx?sourcedoc=%7BE16DEE9A-4BA9-495B-AD1C-C1ED72500589%7D&file=Automated%20Platform%20for%20Primary%20Care%20Data%20MVP%20Initial%20Indicators%20and%20Data%20Sources%20V2.xlsx&action=default&mobileredirect=true). The access to this is part of the Primary Care Datasets - MVP team on Teams.


# Data Sources
The Primary Care application uses a range of data sources to calculate metrics. Some of these data sources are loaded into Foundry through an automated process whereas some are in a workaround list, where they will be automated in a later phase.
## Automated Feeds
All the below datasets are triggered to refresh from source systems between 4:00am and 4:15am. Most of these jobs should take around 10-15 minutes to run, with the exception of Patients Registered at GP Practice by LSOA, which takes around 45-50 minutes.
- [IMD Patient Weighted Data](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.972f9fdc-d7c8-4299-95d6-b31be8d157f0/master), sourced from NCDR (through Stuart Knight), refreshed on an ad-hoc basis
- [ukhf - nhs_workforce - practice_level_census_data](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.cb8ec018-83b7-41ee-820a-e9e2f2f9fa06/master), sourced from UKHF via UDAL, refreshed monthly, currently has an issue such that the historic data is in [this table](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.723c3482-b018-432f-88ce-944fb7cd3d97/master) and the latest is in [another](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.5c63c0eb-cb85-46af-90b2-4b56d020d884/master)
- [GP Level Census Data](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.0b938ee2-1f10-4d46-8925-d3fc9afb2259/master), at a CCG level, sourced from UKHF, refreshed monthly, slightly similar to the above dataset, but this one has further breakdowns in terms of granularity. However, the expectation was initially to move to the above practice level data to make the sources consistent, but the above dataset does not get broken down in the same way.
- [Patients Registered at GP Practice by LSOA](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.01b85ceb-0ec4-4efd-a3d8-3450e4078899/master), sourced from NHSD, refreshed quarterly 
- [Demographic Domains of Deprivation by LSOA](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.f4c95d27-45e4-48f9-b44e-8fc083a6d2c1/master), sourced from NHSD, refreshed very infrequently (years between each refresh)
- [ukhf - appts_in_general_practice - appts_gp_daily](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.576dd69b-ff02-4278-ab29-fb22fcf398d5/master) (aka GPAD data), sourced from UKHF via UDAL, refreshed monthly 
- [Direct Patient Care Collated](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.cf28fbd9-9d31-4353-8130-ff74ab33aefa/master), sourced from UKHF, refreshed quarterly
- [Patients Registered at GP Practice Single Age](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.ba1b5672-df14-4d91-8756-557471c53136/master), sourced from UKHF, refreshed monthly
- [ukhf - appts_in_general_practice - nims_covid_vaccine_counts](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.d99a0fe2-ab8b-44fd-b6a4-2a30056ecd62/master), sourced from UKHF via UDAL, refreshed monthly
- [ukhf - appts_in_general_practice - appts_gp_coverage](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.c4275ea1-46de-4554-8292-b14b0d3ef5ee/master), sourced from UKHF via UDAL, refreshed monthly
- [tbl_WeightedListSizes_v2](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.9168c0de-b6e0-41cf-9a3c-72eabeeaf544/master), sourced from NCDR (through Stuart Knight) - replaced by Annex J
- Annex J ([ukhf - comm_allocations - icb_weighted_population ](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.a068fbd5-4dac-4592-bcfa-1f798233d5d0/master)& [ukhf - comm_allocations - weighted_regs_by_gp_practice](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.af296d5e-9c0d-4854-b58c-f3bbf8cbfd28/master)), NHSE Allocations, refreshed annually
- [tbl_Ref_PCAT_demography_imd_scores_ranks_all_levels](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.972f9fdc-d7c8-4299-95d6-b31be8d157f0/master) 
- [MYS Submission Data](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.e7f279bd-8f9f-49a4-ad79-c8fadb36e629/master), sourced from Community Pharmacy Blood Pressure Check Service (BPCS), refreshed monthly
- [BSA Reimbursement Data](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.4589fc5e-49a4-43ef-9383-41601981076e/master), sourced from , Community pharmacy Discharge Medicines Service (DMS), refreshed monthly
- [Secondary Uses Service (SUS) Admitted Patient Care (APC) Dataset](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.7ef155e6-28ff-4eba-a9fb-53aa14a0c02c/master), sourced from NHSD, refreshed monthly
- [FP17 Form Submissions](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.c27ce61e-68ea-47be-b379-1d7eedb3e1fb/master), sourced from BSA
- [ukhf - pomi - underlying_data](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.98d7ed2d-a6ce-4735-becc-29aaf2c479ee/master), sourced from UKHF via UDAL
- [ukhf - appts_in_general_practice - prac_level_crosstab](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.e8f786b9-5f23-4b31-82d4-5163665fb0d9/master) (practice gp appt data), sourced from UKHF via UDAL, refreshed monthly(?)
## Manual Feeds
- [Direct Patient Care Collated (historic)](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.c5e2a815-aa71-4dd4-b1ab-e9c01857bb7d/master), no longer refreshed as it has now been replaced by an automated feed. However, this dataset is required as it contains historic data
- IMD by Health Organisation ([CCG](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.23e03c6f-3512-4f71-9690-b3652ca867f8/master), [STP](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.5e437f94-753e-4ac1-b3fe-c0a2eef1743a/master), [region](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.ce47391a-9e8c-47d0-a75e-0930828d5849/master)) (deprecated & superseded by automated feed)
- [Weighted Patient List](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.c6a055bb-668b-43bb-8ae9-aa6d1248c596/master) (deprecated & superseded by Annex J)
- Seasonal flu vaccine uptake in GP patients, sourced from UKHSA, refreshed annually
    - [seasonal_flu_vaccinations_2018_19 ](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.3f78cbaa-c723-45d8-9f82-324d0c39746e/master)
    - [seasonal_flu_vaccinations_2019_20 ](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.376b4ddd-2fea-487e-9f8b-8f7df6feb56c/master)
    - [seasonal_flu_vaccinations_2020_21 ](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.2eb7fc32-be63-474c-afd9-328f8f35dc18/master)
    - [seasonal_flu_vaccinations_2021_22 ](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.292de9b1-189e-46b3-8275-befd2f4e3817/master)
## Unknown
- [ARRS Funding Trajectories](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.5015448d-c383-4a64-9d37-2378b364890e/master), not expected to be updated as it looks like a one-off dataset



# Gotchas
Below are things to take note of within the repos.
1. The code in the Metrics Ontology Shared Repo is tagged and published. However, the individual repositories (e.g. Workforce Repository, Access Repository...etc) may be using an earlier version (and not the latest code!). Therefore, do not assume that the individual repo is using a function as per the latest code in the Metrics Ontology Shared Repo. You can check the version in the `transforms-python/conda_recipe/meta.yaml` file and from there, go to the Metrics Ontology Shared Repo --> Branches --> Tags and select the hyperlink to the relevant tag. From there, hit the 'view code' button to display the code associated with that version
1. The GP by IMD (DEV26) uses 'Aggregated' rather than 'Total' within dimension 1/2/3 as there was initially an activity to refactor the other metrics to use 'Aggregated', but had to be aborted due to complexity and being too close to go-live
1. Some roll-ups, such as DEV05 (Estimated General Practice Appointments) perform a roll up from CCG to STP to region. However, they have a separate roll up for national where it includes there entire dataset. This is in case any data 'falls through the cracks' with rolling up to national and does not make it to the national level
1. The Primary Care Dashboard Metrics Union table has a filter for data from Jan-2019 onwards



# Core Ontology
From the Core Ontology, we use two key sets of data:
1. Location related data ([all_locations](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.c4625196-f6bc-4840-bf49-b63b7e1390d0/master), [ccgs](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.a01b55f3-626d-459c-9d19-f65201df8ca5/master) and [gps](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.84845362-efbf-4923-8064-7b439be7271d/master)) to provide the hierarchy to perform roll ups from a CCG to an STP to a region to the national level. 
    1. As of Nov '22 this was updated to point upstream to Places Ontology which reflects the new (effective July '22)boundary changes and hierarchy of NHS bodies (ICS -> ICB & CCG -> Sub-ICB Location)
1. Date related data ([ref_dates](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.36abfcca-6f49-49f5-a71a-6d2fdfa43244/master), [working_days](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.23f9ce15-c086-4724-b06b-911659013aa9/master)) to determine the number of working days in a month and the start/end dates of months/quarters/financial-years
    1. ref_dates is mainly used in DPC as that dataset is based on quarters and we need to identify quarter start/end dates
    1. working_days is mainly used in Appointments, as we need to use it for the 'Workday Adjusted' calculations



# Scheduling
Build scheduling for Primary Care consists of decoupled schedules with a daily dashboard refresh. The daily schedule is set by running the final downstream datasets which refresh the objects on the dashboard at 5:00am everyday; the schedule for the datasets take approximately 45 minutes to run and subsequently, a phonograph sync (for the object refresh) takes place and usually runs in approximately 5 minutes. Therefore, the end-to-end duration to load into the front-end is just around an hour.  
One of the reasons for the schedule running daily is so that any Core Ontology changes can be reflected in Primary Care on a daily basis. The data that is landed into Foundry is typically scheduled to run around 4am and those jobs typically take less than 15 minutes. Therefore, there is a sufficient buffer between the landing data and the schedule.
In Phase 3, we have updated our scheduling approach to decouple builds to save unnecessary compute, previously the entire [PCDID schedule](https://ppds.palantirfoundry.co.uk/workspace/data-integration/monocle/schedule/ri.scheduler.main.schedule.88e6d49d-ce28-4735-a5e6-dfe136f13efa) ran daily.  Schedules are triggered by a single table or multiple tables and then setting all precedessor tables in scope for the build  as 'targets' in order to invoke them to run. The schedule runs under 'Build Scope=Projects' rather than under a named 'user' and this approach should be maintained (we would not want user mode as a user's permissions may change over time).
- [Cleaning schedules](https://ppds.palantirfoundry.co.uk/workspace/data-integration/scheduler/find?v=1&uid=98fb3fcb-016b-4f94-8afb-ad0862a85e44&uid=bdc59ce2-33dc-4ff6-adb0-859093cb4f18&s=NAME&dir=DESCENDING&q=PCDID) - These schedules are for intermediary datasets which are used as development inputs, where a developer has done some cleaning on the raw data. These schedules are triggered by an update to the source (raw) data. All cleaning schedules are  suffixed with the word 'clean'.
- [Downstream schedules](https://ppds.palantirfoundry.co.uk/workspace/data-integration/scheduler/find?v=1&uid=98fb3fcb-016b-4f94-8afb-ad0862a85e44&uid=bdc59ce2-33dc-4ff6-adb0-859093cb4f18&s=NAME&dir=DESCENDING&q=PCDID) -	These schedules use the cleaned datasets which are on the cleaning schedules as a trigger. Therefore whenever there is new clean data available for a specific dataset, this can trigger one or more downstream schedules to run. This is because one metric dataset can have multiple data inputs which update independently, this will ensure that when the latest available data is available that a downstream refresh will occur. All downstream schedules are prefixed with 'PCDID: Downstream'.
- [Primary Care Dashboard Build](https://ppds.palantirfoundry.co.uk/workspace/data-integration/monocle/schedule/ri.scheduler.main.schedule.e507d41c-0bb2-480e-8b5e-d67b3144f2d0) - This is the daily 5am build mentioned above.
- [Weekly build on Sunday](https://ppds.palantirfoundry.co.uk/workspace/data-integration/monocle/schedule/ri.scheduler.main.schedule.88e6d49d-ce28-4735-a5e6-dfe136f13efa), which builds the entire PCDID scope of data. This is to ensure that every week, all data is refreshed even if it has not been triggered to build by other schedules.



# Access Model
The access model defines the levels of access users have to the data generated/processed by this Primary Care project. It can be found on the Microsoft Teams site [here](https://nhsengland.sharepoint.com/sites/PrimaryCareDataforImprovement-MVP/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FPrimaryCareDataforImprovement%2DMVP%2FShared%20Documents%2FPrimary%20Care%20Data%20and%20Insights%20Dashboard%2FMVP%2FGovernance%2FPCD%20MVP%20Access%20Proposal%20%2D%20NHS%20Data%20Platform%20%28Foundry%29%20%2D%20v3%20%7BFINAL%7D%2Epdf&viewid=39e87b52%2D30d3%2D499e%2Db1d2%2Db03fe692fd3c&parent=%2Fsites%2FPrimaryCareDataforImprovement%2DMVP%2FShared%20Documents%2FPrimary%20Care%20Data%20and%20Insights%20Dashboard%2FMVP%2FGovernance).
When a user is granted access to the Primary Care purpose, they are required to specify the scope they require (i.e. the region/STP/ICS they belong to). The access model will then only show the data applicable to that scope.
Developers have access to all the data.
There are three types of markings in the Access Model and are summarised below:
1. Unrestricted - anyone has access to this data
1. Restricted - only certain users have access to this data based on the role model. This is mainly based on the user's location and the location that the row of data relates to
1. Sensitive - this is for case-by-case datasets where access is limited to named people
The access model is developed in the Primary Care Dashboards project where all the datasets that feed into are then given an associated marking. It happens as per the below process
1. We generate the access based on the locations in the [`pc_restricted_access_model`](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.78fbfb35-c36e-4fd9-9f38-0501f460a5d4/master) dataset. This is for the 'Restricted' marking as per the above markings
1. Access policy gets applied to the `policy_column` based on:
    1. Where `is_published=false`, the row gets joined to the above table based on the location to provide it with it's access policy
    1. Where `is_published=true`, the row gets given unrestricted access by granting it to all users
1. A restricted view is applied based on the `policy_column`
1. The restricted view feeds the Ontology object, so that users can only see data on the front-end that is applicable to them



# Key Design Decisions
During the development of the Primary Care dashboards, a number of design decisions were taken and the key ones are as below:
## Locations Data
Some of the data that is landed Foundry has the locations mapped all the way to the region. For instance, it would contain the PCN, CCG, STP and Region. However, the objective is to always map the location from the lowest granularity (PCN in this example) and use the Locations Ontology. This is mainly so that the data is always provided based on the latest hierarchy.
However, we should also note that as per [DevOps 95555](https://nhsidev.visualstudio.com/NHS%20Data%20Platform%20Team/_workitems/edit/95555), some locations are omitted as they are not General Practices. Additionally, when mapping a Practice, it should also be noted that some Practices may not have a PCN as they are not part of one.
## 30 Months of Data
There is a requirement that only 30 months of data is used in any calculations and that we show only 19 months (as the first 12 months of data is omitted in order to calculate the 12 month rolling figure). However, this hasn't been strictly adhered and the compromise is that we've displayed data from January 2019, because the baseline date (Mar-2019) is beyond the 30 months.
## Appointments and Vaccinations
The appointments and vaccinations data are from separate sources. There can be situations where one dataset is landed but not the other. As such, the front-end visualisation for 'including vaccinations' becomes incorrect as it then shows incomplete totals.
There are currently conversations in progress to define this requirement further.
## Calculation of Rate Metrics
Wherever rate metrics are calculated, developers should include _numerator _and _denominator fields _in the output metric dataset for downstream analysis and ease for QA.
These should be passed in the `additional_selection_cols `argument when calling the `generate_metric_data `standard function.



# Metadata Columns
### Metric Name
The metric name will be a constant name for the metric, which will not include any dimension information (e.g. `GP_Full_Time_Equivalent_(FTE)` )
### Metric Title
The metric title is a human readable/friendly version of the metric name (e.g. `GP Full Time Equivalent (FTE)` )
### Metric ID
 The metric ID is a derived field, which is composed of the metric name and dimensions (e.g. `GP_Full_Time_Equivalent_(FTE)_GP_Salaried GP_Other Salaried` whereby GP, Salaried GP and Other Salaried are the dimensions in this example)



# Development Process
[Foundry Development Best Practices Documentation](https://ppds.palantirfoundry.co.uk/workspace/report/ri.report.main.report.e8527f84-9002-4356-9b4b-77bb00391194)
In relation to the development process, below shall be the steps:
1. Development of dataset (developer)
1. Development of unit tests (pytests) (developer)
1. Peer review of code (peer reviewer)
1. Creation of contours for testing (peer reviewer)
1. Initial review by a Tech Lead
1. QA Testing
1. Final review by a Tech Lead (only required if changes were made following the QA process)
1. Build of all subsequent jobs by developer (in dev branch)
1. Perform a sanity-check/spot-check on the resulting metrics by comparing it to [PCOR](https://nhsengland.sharepoint.com/:b:/r/sites/PrimaryCareDataforImprovement-MVP/Shared%20Documents/MVP/Project%20Documentation/Primary%20Care%20Overview%20Report%20PCOR%20v1.8%2024-11-2021.pdf?csf=1&web=1&e=xq0OXN)
1. Raising a PR by developer
1. Merge by a Tech Lead and build on master
1. Add output datasets (metrics and metadata) into PC Dashboard purpose (for GP Access, GP Workforce, this is done by adding output datasets to Project Catalog ). Check if these datasets appear in PC Dashboard references list
1. Add newly developed metrics into the  [pc_dashboard_metrics_union ](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.215380f4-d0d0-4774-b566-13e61fb0c38f/master) dataset 
    1. Add the datasets (metrics and metadata) as inputs to `pc_dashboard_metrics_union.py` (careful to name the arguments appropriately)
    1. Add the metric_title's into `metrics.py` to include them post filtering
1. Add the newly developed metadata to the [unified_metric_metadata ](https://ppds.palantirfoundry.co.uk/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.8094db87-e826-4c92-9937-cad87e71b1e9/master)dataset
    1. NOTE: name needs to be added in three places in the `unified_metadata.py` file
1. Building of all subsequent jobs by developer (in master branch)
    1. Validate success of Ontology objects
    1. Communicate the merge and associated changes to wider team
1. Add health checks for new datasets
1. Scheduling - add to Primary Care Dashboard Metrics Union dataset's schedule ([here](https://ppds.palantirfoundry.co.uk/workspace/data-integration/monocle/schedule/ri.scheduler.main.schedule.88e6d49d-ce28-4735-a5e6-dfe136f13efa)) and set it as a target
We will also endeavour to demonstrate the data to the BA team.



# Scope of Pytests
1. Happy path scenario (including different combinations of different dimensions)
1. Testing of table joins (how does the data fare with data missing in the 'right' table)
1. Dates and edge dates
1. Break-ability/Errors
1. Any functions in `utils.py` should be tested in the `test_utils.py` class
    1. If there is a specific test-case for a `utils.py` function, then add it to the class's testing, otherwise do not add a test case
1. Bugs - anything reported as a bug should have a test case as that would then be an official requirement



# Dev Contour Standard
Exceptions are allowed but have to be explained
1. Metric value:
    1. Building logic
        1. Look at the requirements from Excel <add link>
        1. reperform calculation of the metric in Contour based on the requirements above
        1. Match the Contour output to the output of the code
    1. Check the dimensions and the permutations are needed i.e. all dims, top n dims
    1. Check we start with the lowest geography granularity and that we roll up to the national level.
        1. Records with the lowest geo level values 'Unknown'/ 'Unaligned' will still be recorded under 'england'
    1. Timestamp for aggregation:
        1. See the 'reporting period' column in Excel for raw format. If the metric value is derived from other metric values, the timestamp will be dependent on these
        1. See Wireframe if additional transformation is needed. E.g from financial quarters to dates, from months to quarter or year, etc
1. Other columns such as baseline, target, progress, 12m_rolling,…
    1. Repeat process in step 1)
1. Table shape - Expected number of cols & rows
    1. Can be worked out from input table(s) and requirements from Excel



# Data Asset Creation
Whenever there is a requirement to develop a set of metrics in a new__*__ Programme Area. You will first have to create a _Data Asset _for this area within Metrics Ontology. The steps required to do so are as follows:
__*Doesn't already exist the Programme Areas listed under __[__Projects __](https://ppds.palantirfoundry.co.uk/workspace/report/ri.report.main.report.416ddd4b-8472-4ee4-988a-201b2d9e764e/edit#ri.report.main.widget.916b9e72-6d45-4228-9786-12cb76c888e5)__section.__
1. Navigate to Purpose Management App > click on Metrics Ontology
1. Click on Manage Purpose > Create Derived Data Asset [object Object]
1. Derived from Subset of Approved Assets > fill in form 
    1. Derived Data Asset Name: _<Programme Area> Metrics  _
    1. Upstream Data Assets: Data Assets which contain the input data (e.g. public reference data, primary care core datasets) & Shared Code Assets (nhs-foundry-common) & Locations Ontology & Place Ontology datasets
    1. Keep default
    1. Description: _<Programme Area> metrics for Primary Care_
    1. [object Object]
1. Purpose Management App > Primary Carte Data & Insights Dashboard Purpose > Add Data Asset > search for <programme area> metrics > Request[object Object]
1. Add justification > Keep default Permissions & add developers can edit[object Object]

__Note: If you are developing new metrics in an existing data asset which doesn't contain the source data as a reference (project which contains source data isn't linked to metrics project then reach out to prasanta.panja@england.nhs.uk to get the projects linked__
## Exposing Data Between Projects
In order to be able to reference data from one project in another project which is linked, the dataset must be added to the _Project Catalog:_
[object Object]




# Quiver Development
## Quiver Structure
- __Canvas View:__ Best used for setting up your canvas structure with Containers to hold your charts/filters/text cards which are shown with their ordering in the _Analysis Content_ pane:
			[object Object]
    - _Canvas _contains your entire analysis
    - _List containers _allow you to stack elements on the canvas horizontally or vertically 
        - _TIP: _When setting up items within containers, be sure to configure the dimensions setting to optimise space usage
								[object Object]
    - _TIP_: any non visual analysis content and chart elements that are not intended to be included in the canvas should be removed from the canvas (not just hidden in canvas view):
								[object Object]
    - _TIP:_ Naming conventions for analysis content should follow the following structure
        - Tab <number>: <short description of  context> _<content type>_
        - e.g. Tab 1: Staff Group _Filter_
        - e.g. Tab 5: Date Filtered _Workforce Object Set_
    - _TIP:_ The size of the canvas can be adjusted by hovering over the lower right corner of the canvas and dragging to size
- __Graph View: __Best used for mapping the flow of your analysis & keeping track of the filtered object sets:
			[object Object]
    - Each tab in your Quiver Canvas should have its own branch of analysis as shown in the above image with each of the blue/purple/green/red/navy coloured boxes representing the analysis for a different tabs
    - Graph view can be best compared to the canvas view as 'top down' view- the top of the graph is the top of the canvas with filters lower in the canvas related to filters lower in the analysis branch
        - Upper Quiver filters (see CCG filter in yellow box on above image) should be the at the top of your analysis to that it acts on act subsequent branches
    - _TIP: _Keep each individual filter feeding a separate filtered object set- multiple filters__ __being used in parallel to feed the same filtered object set should be avoided. This allows you to easily validate the number of returned objects as a result of a filter on an object set with what is expected from independent analysis.
    - Analysis steps:
        1. __metric_title __should be used to initially filter the relevant metrics from initial object set (PC_dashboard_metrics) into the Quiver analysis
        1. At the point of splitting your analysis into separate branches, the object set should be filtered to the specific metric_title objects before any dimension specific filtering
        1. Where __dimension-aggregated__ metrics are involved, be mindful to treat filtering aggregates vs non aggregated metrics out of your analysis with caution to avoid double counting metrics across lower dimensions
        1. Where __geographically-aggregated __metrics__ __(e.g. metric for geographical hierarchies of CCG > ICS > Regions > National) are involved, be mindful to filter to the min (or min + 1 if two levels of the hierarchy and so on) for the __geography_numeric_representation __field to avoid double counting across 'higher' geographical hierarchy levels (higher referring to dimension with lower geography_numeric_representation). __NOTE: this filter should be passed after all location name specific filters have been applied in the flow of the analysis.__
## Data Egress
- To enable users to egress data ___Card Actions ___must be enabled in the Options settings of your Quiver Analysis
[object Object]
## Adding Your Quiver Analysis to Workshop Application
- Development of your _Quiver Analysis_ (naming convention <sub-topic> Quiver Analysis) should be contained within the development project
    - Primary Care Dashboard > quiver > dev
- Publishing your Quiver Analysis to a _Template_ should be contained within the Application project
    - Primary Care Applications > quiver
- The RID of the _Quiver Template_ should be pasted into the relevant field of a _Quiver Object Canvas Template Widget _ in Workshop
- If there are universal filters in the Workshop Application, the Object Set that considers the outcome of user selection (module_PCMetric_selectedSystem considers the output of the Region and ICS/STP filters) should be used in the _Object Set to Apply on the Template _field
			[object Object]
- NOTE: If you have based your quiver analysis from an existing analysis Template by duplicating the analysis you must remember to change the name of the Template when publishing, otherwise the RID will be the same as the old analysis.
## Quiver Performance Considerations
Primary Care Metric Objects are passed from the workshop layer to the quiver layer. In the event that timeseries are needed,__ the search around from Primary Care Metric Objects to Primary Care Timeseries Metric Objects should be done as late as possible__ in the quiver flow to avoid expensive aggregations over superfluous objects. This means going from metric to timeseries metric after all location/dimension filtering, as all locations and dimensions are captured in the Metrics objects. Any date filtering needs to be done after the search around to the timeseries objects.
In global settings, toggle Load Settings to "Visible items only". 
	[object Object]



# Quiver Design Guide
## Regions
In charts where Regions are shown use the following colour guidance ( from [▶ ★ Styleguide-Prototype - Tableau Style-guide (figma.com)](https://www.figma.com/proto/uMM9gCPb67QLm51po4P3iy/Tableau-Style-guide?node-id=1214%3A13571&scaling=min-zoom&page-id=1141%3A33429&starting-point-node-id=1214%3A13571)) to differentiate between them:
- North East = #005EB8
- North West = #41B6E6
- East of England = #330072
- Midlands = #AE2573
- London = #78BE20
- South West = #FAE100
- South East = #ED8B00
- England = #212B32 (NHS Black)
[object Object]
## ICS's & CCG's
[TO BE UPDATED]
For ICS's and CCG's falling under a parent region, use gradients of the region colour to differentiate:
[object Object]
## Heirarchies
For differentiating between categories in any other fields within charts, follow the guidance in [▶ 1. Design guide V1.0 (Current) - Foundry Design For Developers V1.0 (figma.com)](https://www.figma.com/proto/Znb3hwn8subkZCH3GUASY8/Foundry-Design-For-Developers-V1.0?page-id=1723%3A86656&node-id=2012%3A61174&viewport=241%2C48%2C0.13&scaling=contain&starting-point-node-id=2012%3A61174) and use chart colours in the following sequence (either by alphabetical hierarchy or other based on domain knowledge):
[object Object]
1. NHS Blue = #005EB8
1. NHS Light Blue = #41B6E6
1. NHS Grey 1 = #4C6272
1. NHS Warm Yellow = #FFB81C
1. NHS Pink = #AE2573
1. NHS Aqua Green = #00A499
1. NHS Bright Pink = #E317AA
1. NHS Green = #007F3B
1. NHS Brown = #9A6324
1. NHS Light Green = #78BE20
1. NHS Tangerine = #ED4F00
1. NHS Light Purple = #880FB8
## RAG
To indicate when targets have/haven't been met use the following RAG colours:
- NHS Error Red = #D5281B
- NHS Green = #007F3B
