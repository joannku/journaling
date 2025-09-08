"""
This script pulls survey data from Qualtrics.
"""

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(src_dir)

import requests
import os
import json
import time
import zipfile
import io
import time
from modules.Qualtrics import Qualtrics
from datetime import datetime

if __name__ == "__main__":

    creds_path = '/Users/joannakuc/Code/journaling/config/creds.json'

    with open(creds_path) as file:
        creds_path = json.load(file)

    survey_ids = creds_path['qualtrics_survey_ids']

    clientId = creds_path['qualtrics_client_id']
    clientSecret = creds_path['qualtrics_client_secret']
    dataCenter = creds_path["qualtrics_datacenter_id"]

    qualtrics = Qualtrics(clientId, clientSecret, dataCenter)

    TOKEN_URL, OAUTH_TOKEN = qualtrics.getToken()
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    for name, SURVEY_ID in survey_ids.items():
        qualtrics.exportSurvey(OAUTH_TOKEN, SURVEY_ID, dataCenter, "csv")