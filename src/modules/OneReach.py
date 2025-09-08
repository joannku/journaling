from .Logger import Logger
from .GoogleSheetHandler import GoogleSheetHandler
import requests
import pandas as pd
import json
import os
from datetime import datetime

class OneReachRequests:
    def __init__(self, creds_path, pagesize=200, output_dir=None):

        with open(creds_path) as file:
            creds = json.load(file)

        self.logger = Logger()
        self.url = creds["onereach_sqlurl"]
        self.pagesize = pagesize
        self.headers = {"auth": creds["authSQL"]}
        self.output_csv = "journals.csv"
        
        # Set output directory - use provided path or default to project's data/raw/onereachai
        if output_dir is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.output_dir = os.path.join(project_root, 'data', 'raw', 'onereachai')
        else:
            self.output_dir = output_dir

        self.table_list = [
            'tsj_analysis',
            'tsj_botmessage',
            'tsj_gptprompt',
            'tsj_gptsummaries',
            'tsj_journals',
            'tsj_journals_saved',
            'tsj_moodanswers',
            'tsj_moodquestions',
            'tsj_mooduseranswers',
            'tsj_notifications',
            'tsj_timezonecheck',
            'tsj_usernames',
            'tsj_usertable'
            ]

    def sql_query(self, tablename):
        data = {"query": f"SELECT COUNT(*) FROM {tablename}"}
        print(data)
        response = requests.post(self.url, json=data, headers=self.headers)

        if response.status_code != 200:
            self.logger.log_message(f"Error when counting rows. Status code: {response.status_code}. Message: {response.text}")

        total_rows = response.json()[0]['COUNT(*)']
        print(f"Total rows: {total_rows}")
        total_pages = (total_rows + self.pagesize - 1) // self.pagesize
        print(f"Total pages: {total_pages}")

        if total_rows < 200: 
            data = {
                    "query": f"SELECT * FROM {tablename}"
                }
            response = requests.post(self.url, json=data, headers=self.headers)
            response_data = response.json()
            df = pd.DataFrame(response_data)
            return df

        else:

            # Create an empty list to store all the dataframes
            dfs = []

            # Fetch journal data page by page
            for page_number in range(total_pages):
                data = {
                    "query": f"SELECT * FROM {tablename} LIMIT {self.pagesize} OFFSET {page_number * self.pagesize}"
                }
                response = requests.post(self.url, json=data, headers=self.headers)

                if response.status_code != 200:
                    print(f"Error on page {page_number}. Status code: {response.status_code}. Message: {response.text}")
                    continue

                response_data = response.json()
                dfx = pd.DataFrame(response_data)
                dfs.append(dfx)

                # Verify counts after each page
                print(f"After processing page {page_number+1}, total rows retrieved: {sum(len(df) for df in dfs)}")

            # Concatenate all dataframes
            complete_df = pd.concat(dfs, ignore_index=True)

            # Verify total rows
            print(f"Expected rows based on COUNT: {total_rows}")
            print(f"Actual rows retrieved: {len(complete_df)}")

            return complete_df
    
    def pull_all_data(self, path=None):
        # Use provided path or default to instance's output_dir
        if path is None:
            path = self.output_dir
        
        # Ensure the output directory exists
        os.makedirs(path, exist_ok=True)
        
        for table_name in self.table_list:
            
            dfs = {}
            
            dfs[table_name] = self.sql_query(table_name)

            # Save all tables to CSV
            dfs[table_name].to_csv(os.path.join(path, f"{table_name}.csv"), index=False)

        # Write date to txt file
        with open(f"{path}/info.txt", "w") as file:
            file.write(f"Data pulled from OneReach on {datetime.now()}.")

        self.logger.log_message("All tables saved to CSV files.")

        # Write a txt file with a timestamp
        with open(f"{path}/info.txt", "w") as file:
            file.write(f"{datetime.now()}")
