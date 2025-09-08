import numpy as np
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from .Logger import Logger

class GoogleSheetHandler:

    def __init__(self, creds):
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(creds, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
        self.logger = Logger()
        self.client = gspread.authorize(self.creds)
        # self.sheet_name = sheet_name

    def get_dataframe(self, sheet_name, sheet_number):
        return pd.DataFrame(self.attempt_request(self.read_google_sheet, sheet_name, sheet_number-1))

    def get_dataframes(self, sheet_name, sheet_numbers):
        dfs = {}
        for num in sheet_numbers:
            dfs[f'df{num}'] = pd.DataFrame(self.attempt_request(self.read_google_sheet, sheet_name, num-1))
        return dfs
    
    def update_dfs(self, target_dfs, new_dfs):
        for key, df in new_dfs.items():
            target_dfs[key] = df
        return target_dfs


    def read_google_sheet(self, sheet_name, sheet_number=0):
        # Define the scope
        # Add your service account file
        # Get the instance of the Spreadsheet
        sheet = self.client.open(sheet_name)
        # Get the first sheet of the Spreadsheet
        worksheet = sheet.get_worksheet(sheet_number)
        # Get all the records of the data
        records_data = worksheet.get_all_records()
        return records_data

    def find_row(self, worksheet, column_name, column_value):
        email_col = worksheet.find(column_name).col
        cells = worksheet.findall(column_value, in_column=email_col)
        
        if cells:  # if email is found
            return cells[0].row
        else:  # if email is not found
            # get all values in desired column
            email_column_values = worksheet.col_values(email_col)
            # return index of first empty cell in 'Email' column
            return len(email_column_values) + 1


    def update_column_value(self, sheet_name, sheet_number, column_name, column_value, matching_column=None, matching_value=None, empty_column_value=""):
        # Define the scope
        # Authorize the clientsheet 
        # Get the instance of the Spreadsheet
        sheet = self.client.open(sheet_name)
        # Get the first sheet of the Spreadsheet
        worksheet = sheet.get_worksheet(sheet_number)
        if matching_column == None:
            row = self.find_row(worksheet, column_name, empty_column_value)
            if row:
                col = worksheet.find(column_name).col
                worksheet.update_cell(row, col, column_value)
            self.logger.log_message(f"Google sheet {sheet_number} column {column_name} updated for {column_value}.")
        else:
            try:
                row = worksheet.find(matching_column).row
                col = worksheet.find(column_name).col
                worksheet.update_cell(row, col, column_value)
                self.logger.log_message(f"Google sheet {sheet_number} column {column_name} updated for {matching_column}.")
            except:
                print(f"Could not find {matching_column} in {sheet_name} {sheet_number}.")


    def attempt_request(self, request_function, *args, **kwargs):
        retry_count = 0
        while retry_count < 3:
            try:
                return request_function(*args, **kwargs)  # Return the result of the request
            except Exception as e:
                error_message = str(e)
                if 'quota' in error_message or 'RATE_LIMIT_EXCEEDED' in error_message or 'service is currently unavailable' in error_message:
                    self.logger.log_message(f"Request rate limit exceeded. Retrying in 30 seconds...")
                    time.sleep(30)  # Wait for 30 seconds
                    retry_count += 1
                else:
                    raise  # Reraise any other exceptions

    def process_for_sheet(self, function_name, sheet_number):
        self.attempt_request(function_name, 'Journalling Sign Ups', sheet_number)
    
    def lowercase_emails(self, sheet_name, sheet_number):
        # Get the instance of the Spreadsheet
        sheet = self.client.open(sheet_name)
        # Get the specified sheet of the Spreadsheet
        worksheet = sheet.get_worksheet(sheet_number)
        
        # Determine the range for the "Email" column
        try:
            email_col_number = worksheet.find("Email").col
        except AttributeError:
            raise ValueError("The column 'Email' was not found in the worksheet.")
        
        # Get all rows of the worksheet
        all_rows = worksheet.get_all_values()
        
        # Loop through each row, starting from the second one
        for row_num, row in enumerate(all_rows[1:], start=2):  # skipping the header
            email = row[email_col_number - 1]  # Adjusting because list indices start at 0
            if email != email.lower():
                worksheet.update_cell(row_num, email_col_number, email.lower())
                self.logger.log_message(f"{email} on sheet number {sheet_number} converted to lowercase.")

        
    def append_row(self, sheet_name, sheet_number, row_values):
        """
        Appends a new row to the specified worksheet.

        Parameters:
        - sheet_name: Name of the Google Sheet.
        - sheet_number: Index of the worksheet (0-indexed).
        - row_values: List of values to be appended.

        Returns:
        - None
        """
        sheet = self.client.open(sheet_name)
        worksheet = sheet.get_worksheet(sheet_number)
        worksheet.append_row(row_values)

    def update_rows_with_changes(self, df_old, df_updated, sheet_name, sheet_number):

        # Identify emails in pivot_df that are not in df7monitoring
        missing_emails = df_updated[~df_updated['Email'].isin(df_old['Email'])]['Email'].tolist()

        # Append these missing emails to the Google Sheet in the first column
        for email in missing_emails:
            self.attempt_request(self.append_row, sheet_name, sheet_number, [email])

        df_old = self.get_dataframe('Journalling Sign Ups', 7)

        # Merge the old and updated dataframes on 'Email'
        merged = df_old.merge(df_updated, on='Email', suffixes=('_old', '_new'))

        # Function to check if there's a change in the row
        def row_has_changes(row):
            for col in df_old.columns:
                if col != 'Email' and row[f"{col}_old"] != row[f"{col}_new"]:
                    return True
            return False

        # Extract the rows with changes
        rows_with_changes = merged[merged.apply(row_has_changes, axis=1)]

        # For each row with changes, update the necessary columns
        for _, row in rows_with_changes.iterrows():
            email = row['Email']
            for col in df_old.columns:
                if col != 'Email' and row[f"{col}_old"] != row[f"{col}_new"]:
                    new_value = row[f"{col}_new"]
                    self.attempt_request(self.update_column_value, sheet_name, sheet_number, col, new_value, matching_column=email)
                    self.logger.log_message(f"Updated {col} for {email} with new value: {new_value}")

    def write_dict_to_sheet(self, sheet_name, sheet_number, data_dict, column_name="Email", ignore_columns=[], mode="match"):
  
        # Get the instance of the Spreadsheet
        sheet = self.client.open(sheet_name)
        # Get the first sheet of the Spreadsheet
        worksheet = sheet.get_worksheet(sheet_number)
        column = self.attempt_request(worksheet.find, column_name).col
        allcolvals = worksheet.col_values(column)
        
        # Find the relevant columns
        for email, participant_info in data_dict.items():
            self.logger.log_message(f"Processing for participant {email}...")
            if mode == "match":
                if email in allcolvals:
                    row = allcolvals.index(email) + 1  
                else: 
                    row = len(allcolvals) + 1 
                    allcolvals.append(email)  
            elif mode == "add_new_row":
                row = len(allcolvals) + 1

            for colname, value in participant_info.items():
                if colname in ignore_columns:
                    continue
                else:
                    print(f"Processing {colname}...")
                    try:
                        if value is not np.nan:
                            col_num = self.attempt_request(worksheet.find, colname).col
                            self.attempt_request(worksheet.update_cell, row, col_num, value)
                        else: 
                            print(f"Value {value} is invalid for {email} in {colname}.")
                    except Exception as e:
                        print(e)
            self.logger.log_message(f"Completed writing {email} participant info into google sheet!")

    def get_changes_dict(self, df_old, df_updated, sheet_name, sheet_number, ignore_columns=[]):
        # Convert emails to lowercase
        df_old['Email'] = df_old['Email'].str.lower()
        df_updated['Email'] = df_updated['Email'].str.lower()

        # Identify emails in df_updated that are not in df_old
        missing_emails = df_updated[~df_updated['Email'].isin(df_old['Email'])]['Email'].tolist()

        # Append these missing emails to the Google Sheet in the first column
        for email in missing_emails:
            self.attempt_request(self.append_row, sheet_name, sheet_number, [email])

        # Refresh df_old from Google Sheet
        df_old = self.get_dataframe('Journalling Sign Ups', 7)
        df_old['Email'] = df_old['Email'].str.lower()

        # Merge the old and updated dataframes on 'Email'
        merged = df_old.merge(df_updated, on='Email', suffixes=('_old', '_new'))

        # Function to normalize boolean-like values
        def normalize_boolean(val):
            val_str = str(val).lower()
            if val_str.lower() in ["true", "t", "yes"]:
                return "TRUE"
            elif val_str.lower() in ["false", "f", "no"]:
                return "FALSE"
            else:
                return val_str
            
        # Function to check if there's a change in the row
        def row_has_changes(row):
            changes = {}
            for col in df_old.columns:
                if col not in ignore_columns:
                    if col != 'Email':
                        old_val = normalize_boolean(row[f"{col}_old"])
                        new_val = normalize_boolean(row[f"{col}_new"])
                            # Try converting to integer for numeric comparison
                        try:
                            old_val = int(float(old_val))
                            new_val = int(float(new_val))
                        except ValueError:
                            pass  # If conversion fails, keep them as strings
                        if str(new_val).lower() not in ["checkwarranted", "nan"]:
                            # print(f"Old value: {old_val}, New value: {new_val}")
                            if old_val != new_val:
                                if col != 'PaymentType':
                                    changes[col] = new_val
                                else:
                                    if old_val == "":
                                        changes[col] = new_val
            return changes

        # Construct a dictionary of changes
        data_dict = {}
        for _, row in merged.iterrows():
            email = row['Email']
            changes = row_has_changes(row)
            if changes:
                data_dict[email] = changes

        return data_dict