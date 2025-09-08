import sys
import os
from path_utils import setup_paths

# Setup paths and get CORE_DIR
CORE_DIR = setup_paths()

from modules.GoogleSheetHandler import GoogleSheetHandler
import pandas as pd
import json

if __name__ == '__main__':

    creds = os.path.join(CORE_DIR, 'config/creds.json')
    gs_handler = GoogleSheetHandler(creds)
    df_signups = gs_handler.get_dataframe('Journalling Sign Ups', 7)
    # print value counts for df_signups
    print(df_signups['Category'].value_counts())
    # Save email as key and Category as value to a dict
    email_category = df_signups.set_index('Email')['Category'].to_dict()
    
    # Export to json file
    with open(os.path.join(CORE_DIR, 'config/study_outcome_by_email.json'), 'w') as file:
        json.dump(email_category, file, indent=4)

    # Map Emails to PID based on the email_pid.json file
    with open(os.path.join(CORE_DIR, 'config/email_pid.json'), 'r') as file:
        email_pid = json.load(file)
    
    # Replace the keys from email_category with values from email_pid
    pid_category = {email_pid[key]: value for key, value in email_category.items()}

    with open(os.path.join(CORE_DIR, 'config/study_outcome_by_pid.json'), 'w') as file:
        json.dump(pid_category, file, indent=4)
        
    print('Exported to json file')