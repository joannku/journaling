import sys
sys.path.append('../')
import requests
import os
import json
import time
import requests
import zipfile
import io
import time
from .Logger import Logger
from datetime import datetime

class Qualtrics:

    def __init__(self, clientId, clientSecret, dataCenter):
        self.logger = Logger()
        self.clientId = clientId
        self.clientSecret = clientSecret
        self.dataCenter = dataCenter

    def getToken(self):
        tokenUrl = "https://{0}.qualtrics.com/oauth2/token".format(self.dataCenter) 
        data = {'grant_type': 'client_credentials','scope': 'manage:all'}
        r = requests.post(tokenUrl, auth=(self.clientId, self.clientSecret), data=data)
        
        return tokenUrl, r.json()['access_token']

    def exportSurvey(self, oauthToken, surveyId, dataCenter, fileFormat):
        baseUrl = f"https://{dataCenter}.qualtrics.com/API/v3/surveys/{surveyId}/export-responses/"
        headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {oauthToken}"
        }

        # Step 1: Creating Data Export
        downloadRequestPayload = {"format": fileFormat, "useLabels": True}
        downloadRequestResponse = requests.post(baseUrl, json=downloadRequestPayload, headers=headers)
        progressId = downloadRequestResponse.json()["result"]["progressId"]

        # Step 2: Checking on Data Export Progress
        progressStatus = "inProgress"
        while progressStatus != "complete" and progressStatus != "failed":
            time.sleep(2)  # Adding a short delay before checking again
            requestCheckUrl = baseUrl + progressId
            requestCheckResponse = requests.get(requestCheckUrl, headers=headers)
            progressStatus = requestCheckResponse.json()["result"]["status"]

        if progressStatus == "failed":
            raise Exception("Export failed")

        fileId = requestCheckResponse.json()["result"]["fileId"]

        # Step 3: Downloading file
        requestDownloadUrl = baseUrl + fileId + '/file'
        requestDownload = requests.get(requestDownloadUrl, headers=headers, stream=True)

        
        # Determine the base directory (navigate two levels up from the current file's directory)
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        # Construct target_dir relative to BASE_DIR
        target_dir = os.path.join(BASE_DIR, 'data', 'raw', 'qualtrics')

        # Ensure the directory exists
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # Step 4: Unzipping the file
        with zipfile.ZipFile(io.BytesIO(requestDownload.content)) as z:
            for member in z.infolist():
                # If a file with this name already exists in the directory, remove it
                existing_file_path = os.path.join(target_dir, member.filename)
                if os.path.exists(existing_file_path):
                    os.remove(existing_file_path)
                # Extract the file from the zip
                z.extract(member, target_dir)

        self.logger.log_message(f'Qualtrics download of {member.filename} at {datetime.now()} complete')
