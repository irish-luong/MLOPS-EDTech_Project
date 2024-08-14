DB_PATH = "/Users/geotech/Documents/max/upgrad/mlops_assignment/mlops_edtech_project/dags/Lead_scoring_data_pipeline"

DB_FILE_NAME = "lead_scoring_data_cleaning.db"

DATA_DIRECTORY = "/Users/geotech/Documents/max/upgrad/mlops_assignment/mlops_edtech_project/dags/Lead_scoring_data_pipeline/data"
INTERACTION_MAPPING = "/Users/geotech/Documents/max/upgrad/mlops_assignment/mlops_edtech_project/dags/Lead_scoring_data_pipeline/mapping/interaction_mapping.csv"
INDEX_COLUMNS = [
    "created_date",
    "city_tier",
    "first_platform_c",
    "first_utm_medium_c",
    "first_utm_source_c",
    "total_leads_droppped",
    "referred_lead",
    "app_complete_flag",
]

FILE_LEADSCORING = "leadscoring.csv"
TABLE_MODEL_INPUT = "model_input"
TABLE_LOADED_DATA = 'loaded_data'
TABLE_CITY_TIER_MAPPED = 'city_tier_mapped'
TABLE_CATEGORICAL_VARIABLES_MAPPED  = 'categorical_variables_mapped'

