from myproject.datasets.clean.fte_dpc_staff_gp_pcn_collated import clean_role_name, get_staff_role
from nhs_foundry_common.test_utils import assert_df


def test_clean_role_name(spark_session):

    fte_gp_pcn_collated = spark_session.createDataFrame(
        [
            ['1', '1', '1 1', 'ARRS', 'U1', 'ccg_1', 'S1', 'R1', 'Care Coordinators'],
            ['2', '2', '2 2', 'Non-ARRS', 'U2', 'ccg_2', 'S1', 'R1', 'Apprentice - Others'],
            ['3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Other Direct Patient Care'],
            ['3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Mental Health Practitioner Band 5'],
            ['3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Health and Wellbeing Coaches'],
            ['3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'High Intensity Therapists']
        ],
        ['year', 'quarter', 'year_and_quarter', 'arrs', 'pcn_code', 'ccg_code',
         'ics_stp_code', 'region_code', 'role_name']
    )

    expected_df = spark_session.createDataFrame(
        [
            ['CARE COORDINATOR', '1', '1', '1 1', 'ARRS', 'U1', 'ccg_1', 'S1', 'R1', 'Care Coordinators'],
            ['APPRENTICE - OTHER', '2', '2', '2 2', 'Non-ARRS', 'U2', 'ccg_2', 'S1', 'R1', 'Apprentice - Others'],
            ['OTHER', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Other Direct Patient Care'],
            ['MENTAL HEALTH PRACTITIONER BAND 5', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Mental Health Practitioner Band 5'],
            ['HEALTH AND WELLBEING COACHES', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Health and Wellbeing Coaches'],
            ['HIGH INTENSITY THERAPISTS', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'High Intensity Therapists']
        ],
        ['cleaned_role_name', 'year', 'quarter', 'year_and_quarter', 'arrs', 'pcn_code', 'ccg_code',
         'ics_stp_code', 'region_code', 'role_name']
    )

    cleaned_df = clean_role_name(fte_gp_pcn_collated)
    assert_df(cleaned_df, expected_df)


def test_get_staff_role(spark_session):

    fte_gp_pcn_collated = spark_session.createDataFrame(
        [
            ['CARE COORDINATOR', '1', '1', '1 1', 'ARRS', 'U1', 'ccg_1', 'S1', 'R1', 'Care Coordinators'],
            ['APPRENTICE - OTHER', '2', '2', '2 2', 'Non-ARRS', 'U2', 'ccg_2', 'S1', 'R1', 'Apprentice - Others'],
            ['OTHER', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Other Direct Patient Care'],
            ['MENTAL HEALTH PRACTITIONER BAND 5', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Mental Health Practitioner Band 5'],
            ['HEALTH AND WELLBEING COACHES', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Health and Wellbeing Coaches'],
            ['HIGH INTENSITY THERAPISTS', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'High Intensity Therapists']
        ],
        ['cleaned_role_name', 'year', 'quarter', 'year_and_quarter', 'arrs', 'pcn_code', 'ccg_code',
         'ics_stp_code', 'region_code', 'role_name']
    )

    df = spark_session.createDataFrame(
        [
            ['Apprentice - Others', 'APPRENTICES'],
            ['GP IN TRAINING GRADE ST3/4', 'GPS IN TRAINING GRADES'],
            ['Other Direct Patient Care', 'OTHER DIRECT PATIENT CARE'],
            ['OTHER', 'OTHER ADMIN/NON-CLINICAL'],
            ['OTHER', 'OTHER NURSES']
        ],
        ['STAFF_ROLE', 'Proposed_staff_grouping']
    )

    expected_df = spark_session.createDataFrame(
        [
            ['UNCATEGORISED', 'CARE COORDINATOR', '1', '1', '1 1', 'ARRS', 'U1', 'ccg_1', 'S1', 'R1', 'Care Coordinators'],
            ['APPRENTICES', 'APPRENTICE - OTHER', '2', '2', '2 2', 'Non-ARRS', 'U2', 'ccg_2', 'S1', 'R1', 'Apprentice - Others'],
            ['OTHER DIRECT PATIENT CARE', 'OTHER', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Other Direct Patient Care'],
            ['UNCATEGORISED', 'MENTAL HEALTH PRACTITIONER BAND 5', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Mental Health Practitioner Band 5'],
            ['UNCATEGORISED', 'HEALTH AND WELLBEING COACHES', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'Health and Wellbeing Coaches'],
            ['UNCATEGORISED', 'HIGH INTENSITY THERAPISTS', '3', '3', '3 3', 'ARRS', 'U3', 'ccg_3', 'S2', 'R2', 'High Intensity Therapists']
        ],
        ['Proposed_staff_grouping', 'cleaned_role_name', 'year', 'quarter', 'year_and_quarter', 'arrs', 'pcn_code', 'ccg_code',
         'ics_stp_code', 'region_code', 'role_name']
    )

    joined_df = get_staff_role(fte_gp_pcn_collated, df)
    assert_df(joined_df, expected_df)
