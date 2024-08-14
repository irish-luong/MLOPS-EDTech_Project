'''
filename: utils.py
functions: encode_features, get_train_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import os
import contextlib
import numpy as np
import pandas as pd
from datetime import date


import sqlite3

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    confusion_matrix,
)
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from Lead_scoring_training_pipeline import constants


###############################################################################
# Define the function to encode features
# ##############################################################################

def encode_features():
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline from the pre-requisite module for this.
    '''
    cnx = sqlite3.connect(os.path.join(constants.DB_PATH, constants.DB_FILE_NAME))
    print("Loading model_input table")
    df = pd.read_sql(f"select * from {constants.TABLE_MODEL_INPUT}", cnx)

    print("One hot encoding features")
    # Implement these steps to prevent dimension mismatch during inference
    encoded_df = pd.DataFrame(columns=constants.ONE_HOT_ENCODED_FEATURES)  # from constants.py
    placeholder_df = pd.DataFrame()

    # One-Hot Encoding using get_dummies for the specified categorical features
    for f in constants.FEATURES_TO_ENCODE:
        if f in df.columns:
            encoded = pd.get_dummies(df[f])
            encoded = encoded.add_prefix(f + "_")
            placeholder_df = pd.concat([placeholder_df, encoded], axis=1)
        else:
            print(f + ",Feature not found")
            # return df

    # Implement these steps to prevent dimension mismatch during inference
    for feature in encoded_df.columns:
        if feature in df.columns:
            encoded_df[feature] = df[feature]
        if feature in placeholder_df.columns:
            encoded_df[feature] = placeholder_df[feature]

    encoded_df.fillna(0, inplace=True)
    target = df[["app_complete_flag"]]
    print("Storing target features to 'target' table")
    target.to_sql(name=constants.TABLE_TARGET, con=cnx, if_exists="replace", index=False)
    print("Storing rest of features to 'feature' table")
    encoded_df.to_sql(name=constants.TABLE_FEATURES, con=cnx, if_exists="replace", index=False)
    cnx.close()


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model():
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''

    cnx = sqlite3.connect(os.path.join(constants.DB_PATH, constants.DB_FILE_NAME))

    print("Loading 'features' table")
    X = pd.read_sql(f"select * from {constants.TABLE_FEATURES}", cnx)
    print("XXXXXXXXXXX")
    print(X)

    print("Loading 'target' table")
    y = pd.read_sql(f"select * from {constants.TABLE_TARGET}", cnx)
    print("yyyyyyyyy")
    print(y)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=100
    )

    # Model Training

    # make sure to run mlflow server before this.

    run_name = constants.EXPERIMENT + "" + date.today().strftime("%d%m_%Y_%H_%M_%S")
    mlflow.set_tracking_uri(constants.TRACKING_URI)

    with contextlib.suppress(Exception):
        # Creating an experiment
        print("Creating mlflow experiment")
        mlflow.create_experiment(constants.EXPERIMENT)
    # Setting the environment with the created experiment
    mlflow.set_experiment(constants.EXPERIMENT)

    with mlflow.start_run(run_name=run_name) as run:
        # Model Training
        clf = lgb.LGBMClassifier()
        clf.set_params(**constants.model_config)
        clf.fit(X_train, y_train)

        mlflow.sklearn.log_model(
            sk_model=clf, artifact_path="models", registered_model_name="LightGBM"
        )
        mlflow.log_params(constants.model_config)

        # predict the results on training dataset
        y_pred = clf.predict(X_test)

        # Log metrics
        acc = accuracy_score(y_pred, y_test)
        precision = precision_score(y_pred, y_test, average="macro")
        recall = recall_score(y_pred, y_test, average="macro")
        f1 = f1_score(y_pred, y_test, average="macro")
        auc = roc_auc_score(y_pred, y_test, average="weighted", multi_class="ovr")
        cm = confusion_matrix(y_test, y_pred)
        tn = cm[0][0]
        fn = cm[1][0]
        tp = cm[1][1]
        fp = cm[0][1]

        print("Precision=", precision)
        print("Recall=", recall)
        print("AUC=", auc)

        mlflow.log_metric("test_accuracy", acc)
        mlflow.log_metric("Precision", precision)
        mlflow.log_metric("Recall", recall)
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("AUC", auc)
        mlflow.log_metric("True Negative", tn)
        mlflow.log_metric("False Negative", fn)
        mlflow.log_metric("True Positive", tp)
        mlflow.log_metric("False Positive", fp)

        runID = run.info.run_uuid
        print(f"Inside MLflow Run with id {runID}")
   
