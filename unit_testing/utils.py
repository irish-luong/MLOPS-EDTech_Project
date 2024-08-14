##############################################################################
# Import necessary modules and files
##############################################################################


import os
import sqlite3
import pandas as pd
from sqlite3 import Error

import constants
import city_tier_mapping
from significant_categorical_level import *

###############################################################################
# Define the function to build database
###############################################################################

def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    db_path = os.path.join(constants.DB_PATH, constants.DB_FILE_NAME)
    print("SQLite path", db_path)
    if os.path.isfile(db_path):
        print("DB Already Exsist")
        return "DB Exsist"
    else:
        print("Creating Database")
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            print("New DB Created")
            return "DB created"
        except Error as e:
            print(e)
            return "Error creating DB " + db_path
        # closing the connection once the database is created
        finally:
            if conn:
                conn.close()

###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db() -> pd.DataFrame:
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'total_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''

    leadscoring_file_path = os.path.join(constants.DATA_DIRECTORY, constants.FILE_LEADSCORING)
    print("Read data from ", leadscoring_file_path)

    db_path = os.path.join(constants.DB_PATH, constants.DB_FILE_NAME)
    print("Write data into ", db_path)
    try:

        # Open connection to SQLite
        conn = sqlite3.connect(db_path)

        # Load data from CSV file
        df = pd.read_csv(leadscoring_file_path, index_col=[0])

        print("Processing total_leads_droppped and referred_lead columns")
        df["total_leads_droppped"] = df["total_leads_droppped"].fillna(0)
        df["referred_lead"] = df["referred_lead"].fillna(0)

        print("Storing processed df to loaded_data table")
        df.to_sql(name=constants.TABLE_LOADED_DATA, con=conn, if_exists="replace", index=False)
        print(df)
    except Exception as e:
        print(f"Exception thrown in load_data_into_db : {e}")
        raise e
    finally:
        if conn:
            conn.close()


###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    db_path = os.path.join(constants.DB_PATH, constants.DB_FILE_NAME)
    print("Read data from ", constants.DB_PATH)
    try:

        # Open connection to SQLite
        conn = sqlite3.connect(db_path)

        print("Loading loaded_data table")
        df = pd.read_sql(f"select * from {constants.TABLE_LOADED_DATA}", conn)

        print("Mapping city_mapped to tiers")
        df["city_tier"] = df["city_mapped"].map(city_tier_mapping.city_tier_mapping)
        df["city_tier"] = df["city_tier"].fillna(3.0)

        # we do not need city_mapped later
        df = df.drop(["city_mapped"], axis=1)

        print("Storing mapped df to table city_tier_mapped")
        df.to_sql(name=constants.TABLE_CITY_TIER_MAPPED, con=conn, if_exists="replace", index=False)
        print(df)
    except Exception as e:
        print(f"Exception thrown in load_data_into_db : {e}")
        raise e
    finally:
        if conn:
            conn.close()

###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    db_path = os.path.join(constants.DB_PATH, constants.DB_FILE_NAME)
    print("Read data from ", constants.DB_PATH)
    try:

        # Open connection to SQLite
        conn = sqlite3.connect(db_path)

        df = pd.read_sql(f"select * from {constants.TABLE_CITY_TIER_MAPPED}", conn)
        print("Reading of city_tier_mapped data completed")

        # all the levels below 90 percentage are assgined to a single level called others
        new_df = df[
            ~df["first_platform_c"].isin(list_platform)
        ]  # get rows for levels which are not present in list_platform
        new_df["first_platform_c"] = "others"  # replace the value of these levels to others
        old_df = df[
            df["first_platform_c"].isin(list_platform)
        ]  # get rows for levels which are present in list_platform
        df = pd.concat(
            [new_df, old_df]
        )  # concatenate new_df and old_df to get the final dataframe

        # all the levels below 90 percentage are assgined to a single level called others
        new_df = df[
            ~df["first_utm_medium_c"].isin(list_medium)
        ]  # get rows for levels which are not present in list_medium
        new_df[
            "first_utm_medium_c"
        ] = "others"  # replace the value of these levels to others
        old_df = df[
            df["first_utm_medium_c"].isin(list_medium)
        ]  # get rows for levels which are present in list_medium
        df = pd.concat(
            [new_df, old_df]
        )  # concatenate new_df and old_df to get the final dataframe

        # all the levels below 90 percentage are assgined to a single level called others
        new_df = df[
            ~df["first_utm_source_c"].isin(list_source)
        ]  # get rows for levels which are not present in list_source
        new_df[
            "first_utm_source_c"
        ] = "others"  # replace the value of these levels to others
        old_df = df[
            df["first_utm_source_c"].isin(list_source)
        ]  # get rows for levels which are present in list_source
        df = pd.concat(
            [new_df, old_df]
        )  # concatenate new_df and old_df to get the final dataframe

        df = df.drop_duplicates()
        print("Storing mapped df to table categorical_variables_mapped")
        df.to_sql(
            name=constants.TABLE_CATEGORICAL_VARIABLES_MAPPED,
            con=conn,
            if_exists="replace",
            index=False,
        )
        print(df)
    except Exception as e:
        print(f"Exception thrown in load_data_into_db : {e}")
        raise e
    finally:
        if conn:
            conn.close()


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    db_path = os.path.join(constants.DB_PATH, constants.DB_FILE_NAME)
    print("Read data from ", constants.DB_PATH)
    try:

        # Open connection to SQLite
        conn = sqlite3.connect(db_path)

        df = pd.read_sql(f"select * from {constants.TABLE_CATEGORICAL_VARIABLES_MAPPED}", conn)
        print("categorical_variables_mapped table loaded")

        # reading interaction mapping file
        df_event_mapping = pd.read_csv(constants.INTERACTION_MAPPING, index_col=[0])

        # unpivot the interaction columns
        id_vars = constants.INDEX_COLUMNS
        if "app_complete_flag" not in df.columns:
            id_vars.remove("app_complete_flag")

        df_unpivot = pd.melt(
            df,
            id_vars=id_vars,
            var_name="interaction_type",
            value_name="interaction_value",
        )

        # handling null value
        df_unpivot["interaction_value"] = df_unpivot["interaction_value"].fillna(0)

        # map interaction type column with the mapping file to get interaction mapping
        df = pd.merge(df_unpivot, df_event_mapping, on="interaction_type", how="left")

        # dropping the interaction type column as it is not needed
        df = df.drop(["interaction_type"], axis=1)

        # pivot interaction mapping column values
        df_pivot = df.pivot_table(
            values="interaction_value",
            index=id_vars,
            columns="interaction_mapping",
            aggfunc="sum",
        )
        df_pivot = df_pivot.reset_index()

        df_pivot.to_sql(
            name="interactions_mapped", con=conn, if_exists="replace", index=False
        )

        # Selecting a subset of columns for model traning part, excluding created_date
        trimmed_dataset = df_pivot[constants.INDEX_COLUMNS[1:]]

        print("Storing trimmed df to table model_input")
        trimmed_dataset.to_sql(
            name=constants.TABLE_MODEL_INPUT, con=conn, if_exists="replace", index=False
        )


        df = pd.read_sql("select * from model_input", conn)

        print(trimmed_dataset)

        print("Xxxx")

        print(df)

    except Exception as e:
        print(f"Exception thrown in load_data_into_db : {e}")
        raise e
    finally:
        if conn:
            conn.close()
   