DB_PATH = "/Users/geotech/Documents/max/upgrad/mlops_assignment/mlops_edtech_project/unit_testing"

DB_FILE_NAME = 'utils_output.db'
UNIT_TEST_DB_FILE_NAME = 'unit_test_cases.db' 
DATA_DIRECTORY = '/Users/geotech/Documents/max/upgrad/mlops_assignment/mlops_edtech_project/dags/Lead_scoring_data_pipeline/data'
INTERACTION_MAPPING = '/Users/geotech/Documents/max/upgrad/mlops_assignment/mlops_edtech_project/dags/Lead_scoring_data_pipeline/mapping/interaction_mapping.csv'
INDEX_COLUMNS =  ['created_date', 'city_tier', 'first_platform_c', 'first_utm_medium_c',
       'first_utm_source_c', 'total_leads_droppped', 'referred_lead',
       'app_complete_flag']  
LEADSCORING_CSV_PATH = f"{DATA_DIRECTORY}leadscoring.csv"

FILE_LEADSCORING = "leadscoring.csv"
TABLE_MODEL_INPUT = "model_input"
TABLE_LOADED_DATA = 'loaded_data'
TABLE_CITY_TIER_MAPPED = 'city_tier_mapped'
TABLE_CATEGORICAL_VARIABLES_MAPPED  = 'categorical_variables_mapped'