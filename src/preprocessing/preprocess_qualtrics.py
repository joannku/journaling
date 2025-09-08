"""
Process Qualtrics surveys (prescreening/baseline/exit), merge and score, map Email→PID, and export:
- data/processed/qualtrics_merged.csv
- data/processed/3_qualtrics_totals.csv
"""

import sys
import os
from path_utils import setup_paths

# Setup paths and get CORE_DIR
CORE_DIR = setup_paths()

import pandas as pd
import json
from modules.GoogleSheetHandler import GoogleSheetHandler
from modules.QualtricsProcessing import QualtricsProcessing
from modules.Logger import Logger
from modules.GoogleSheetHandler import GoogleSheetHandler
from modules.QualtricsProcessing import QualtricsProcessing, SuspiciousUsersChecks, QuestionnaireCompletion
from modules.JournalAnalysisManager import JournalAnalysisManager
from modules.Logger import Logger
from modules.GoogleSheetHandler import GoogleSheetHandler
from anonymise_emails import map_email_to_pid

def validate_email_matching(df):

    df_usertable = pd.read_csv(os.path.join(CORE_DIR, 'data/raw/onereachai/tsj_usertable.csv'))
    # Lowercase all Email values in df and df_usertable
    df_usertable['Email'] = df_usertable['Email'].str.lower()

    # Save all of these Email values in a list
    null_participant_ids = df[df['ParticipantID'].isnull()]['Email'].tolist()
    print(null_participant_ids)
    print(f"Null ParticipantIDs: {df['ParticipantID'].isnull().sum()}")
    # Check if any of Email values associated with null ParticipantIDs are present in email_matching.json
    email_matching_path = os.path.join(CORE_DIR, 'config/email_matching.json')
    with open(email_matching_path, 'r') as f:
        email_matching = json.load(f)

    # Check if any of the keys in email_matching are present in null_participant_ids
    for key,value in email_matching.items():
        print(f"Key: {key}, Value: {value}")
        if value in null_participant_ids:
            print(f"Value {value} found in null_participant_ids.")
            if key in df_usertable['Email'].tolist():
                # print(f"Key {key} found in df_usertable.")
                # Update the ParticipantID in df with the value from df_usertable where Email == key
                pid = df_usertable.loc[df_usertable['Email'] == key, 'ParticipantID'].values[0]
                print(f"ParticipantID: {pid}")
                df.loc[df['Email'] == value, 'ParticipantID'] = pid
                print(f"Updated ParticipantID for {value} [{key}] to {pid}")
        if key in null_participant_ids:
            if value in df_usertable['Email'].tolist():
                # print(f"Value {value} found in df_usertable.")
                pid = df_usertable.loc[df_usertable['Email'] == value, 'ParticipantID'].values[0]
                df.loc[df['Email'] == key, 'ParticipantID'] = pid
                print(f"Updated ParticipantID for {key} [{value}] to {pid}")

    # Print the number of null ParticipantIDs
    print(f"After correction, null ParticipantIDs count: {df['ParticipantID'].isnull().sum()}")

    return df

if __name__ == '__main__':

    creds = os.path.join(CORE_DIR, 'config/creds.json')
    dfu = pd.read_csv(os.path.join(CORE_DIR, 'data/raw/onereachai/tsj_usertable.csv'))

    qualtrics_pre = QualtricsProcessing()
    qualtrics_quest = QuestionnaireCompletion()
    qualtrics_susp = SuspiciousUsersChecks()
    journal = JournalAnalysisManager(creds)
    gs_handler = GoogleSheetHandler(creds)
    logger = Logger()

    df1 = qualtrics_pre.process_prescreening()
    df2 = qualtrics_pre.process_baseline()
    df3 = qualtrics_pre.process_exit()

    gs_handler.process_for_sheet(gs_handler.lowercase_emails, 0)
    gs_handler.process_for_sheet(gs_handler.lowercase_emails, 1)
    gs_handler.process_for_sheet(gs_handler.lowercase_emails, 3)

    merged_df = qualtrics_pre.merge_processed_qualtrics(df1, df2, df3)
    merged_df = merged_df[~merged_df.index.str.endswith('prolific.com')]
    merged_df = qualtrics_quest.remove_duplicates(merged_df)

    merged_df['Prescreening_Duration (in seconds)'] = pd.to_numeric(merged_df['Prescreening_Duration (in seconds)'], errors='coerce')
    merged_df['Baseline_Duration (in seconds)'] = pd.to_numeric(merged_df['Baseline_Duration (in seconds)'], errors='coerce')
    merged_df['Exit_Duration (in seconds)'] = pd.to_numeric(merged_df['Exit_Duration (in seconds)'], errors='coerce')
    
    # Process for totals
    totals_df = qualtrics_quest.scoring_questionnaires(merged_df)
    totals_df = map_email_to_pid(totals_df, dfu, matching_column='Email', core_dir=CORE_DIR)
    totals_df = validate_email_matching(totals_df)

    # process for merged
    merged_df = map_email_to_pid(merged_df, dfu, matching_column='Email', core_dir=CORE_DIR)
    merged_df = validate_email_matching(merged_df)

    merged_df.to_csv(os.path.join(CORE_DIR, 'data/processed/qualtrics_merged.csv'))

    col_list = totals_df.columns.tolist()
    print(col_list)
    # Remove Email and Participant ID from col_list
    if 'Email' in col_list:
        col_list.remove('Email')
    if 'ParticipantID' in col_list:
        col_list.remove('ParticipantID')
    # Reorder the columns
    col_list = ['Email', 'ParticipantID',] + col_list
    totals_df = totals_df[col_list]

    # Export the DataFrame to CSV
    filepath = os.path.join(CORE_DIR, 'data/processed/3_qualtrics_totals.csv')
    print(filepath)
    totals_df.to_csv(filepath)
    print(f"File exported to {filepath}.")