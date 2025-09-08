import sys
sys.path.append('../')
import requests
import os
import json
import time
import zipfile
import io
import time
import pandas as pd
from .Logger import Logger
from datetime import datetime

class QualtricsProcessing:

    def __init__(self):
        self.logger = Logger()
        # Get CORE_DIR from environment (set by _main.py) or derive from file location  
        self.core_dir = os.environ.get('JOURNALING_CORE_DIR') or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.folder = os.path.join(self.core_dir, "data", "raw", "qualtrics")
        self.prefixes = ['Prescreening', 'Baseline', 'Exit']
        self.questionnaires = ['WEMWBS', 'GAD7', 'PHQ9', 'RRS', 'BIS', 'ASQ', 'NIEQ', 'VISQ']

    def process_prescreening(self, file_name="Journalling - 1 Prescreening.csv"):
        path = f"{self.folder}/{file_name}"
        df1 = pd.read_csv(path)
        df1 = df1.rename(columns={'intro-email': 'Email'})
        df1.set_index("Email", inplace=True)
        df1.index = df1.index.astype(str).str.lower()
        df1 = df1.drop(['please provide your email address.', '{"importid":"qid906_text"}', 'nan', 'test@test.pl'])
        df1['StartDate'] = pd.to_datetime(df1['StartDate'], errors='coerce')
        # Keep first entry
        df1 = self.recode_answers(df1)
        df1 = df1.add_prefix('Prescreening_')
        return df1

    def process_baseline(self,  file_name="Journalling - 2 Baseline.csv"):
        path = f"{self.folder}/{file_name}"
        df2 = pd.read_csv(path)
        df2.set_index("Email", inplace=True)
        df2.index = df2.index.astype(str).str.lower()
        # Format 2023-06-28 17:45:27
        df2['StartDate'] = pd.to_datetime(df2['StartDate'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        df2 = df2.drop(['{"importid":"qid31_text"}', "please enter your email to continue:\nmake sure it's the same email you used in your first survey!"])
        df2 = self.recode_answers(df2)
        df2 = df2.add_prefix('Baseline_')
        return df2

    def process_exit(self,  file_name="Journalling - 3 Exit.csv"):
        path = f"{self.folder}/{file_name}"
        df3 = pd.read_csv(path)
        df3.set_index("Email", inplace=True)
        df3.index = df3.index.astype(str).str.lower()
        df3 = df3.drop(["please enter your email to continue:\nmake sure it's the same email you used across the study.", '{"importid":"qid31_text"}'])
        df3['StartDate'] = pd.to_datetime(df3['StartDate'], errors='coerce')
        df3 = self.recode_answers(df3)
        df3 = df3.add_prefix('Exit_')
        return df3
    
    def recode_answers(self, df):
        # Mapping text responses to numeric values for each questionnaire
        response_mapping = {

            'WEMWBS': {
                "None of the time": 1,
                "Rarely": 2,
                "Some of the time": 3,
                "Often": 4,
                "All of the time": 5
            },

             'VISQ': {
                "Never": 1,
                "Rarely": 2,
                "Occasionally": 3,
                "Sometimes": 4,
                "Often": 5,
                "Very often": 6,
                "All the time": 7
            },

            'GAD7': {
                "Not at all": 0,
                "Several days": 1,
                "More than half the days": 2,
                "Nearly every day": 3
            }, 

            'ASQ': {
                "Definitely disagree": 1,
                "Slightly disagree": 2,
                "Slightly agree": 3,
                "Definitely agree": 4
            },

            'PHQ9': {
                "Not at all": 0,
                "Several days": 1,
                "More than half the days": 2,
                "Nearly every day": 3
            }, 

            'BIS': {
                "Rarely or never": 1,
                "Occasionally": 2,
                "Often": 3,
                "Almost always or always": 4
            },

            'RRS': {
                "Almost Never": 0,
                "Sometimes": 1,
                "Often": 2,
                "Almost always": 3
            }


        }

        # Iterate through each column and apply recoding if necessary
        for col in df.columns:
            for questionnaire, mapping in response_mapping.items():
                if questionnaire in col:
                    df[col] = df[col].map(mapping)

        return df


    def merge_processed_qualtrics(self, df_prescreening, df_baseline, df_exit):
        df1df2 = df_prescreening.join(df_baseline, how='outer')
        df1df2df3 = df1df2.join(df_exit, how='outer')
        fulldf = df1df2df3
        return fulldf

class SuspiciousUsersChecks(QualtricsProcessing):

    def __init__(self):
        super().__init__()
        self.ignore_total = ['Prescreening_Completed', 'Baseline_Completed', 'Exit_Completed', 
                      'Flagged_Baseline_IPAddress_Emails', 'Flagged_Baseline_IPAddress_Count',
                      'Flagged_Exit_IPAddress_Count', 'Flagged_Exit_IPAddress_Emails']


    def qualtrics_completion(self, merged_df, suspicious_df, df1, df2, df3):
        """
        Check whether user data is present in each of the dataframes (df1, df2, df3)
        and update suspicious_df accordingly.
        """
        # Assuming 'UserID' is the column to match user data across dataframes
        user_ids = merged_df.index

        # Check for user data presence in each dataframe
        for prefix, df in zip(['Prescreening', 'Baseline', 'Exit'], [df1, df2, df3]):
            column_name = f'{prefix}_Completed'
            suspicious_df[column_name] = user_ids.isin(df.index)

        return suspicious_df

    def has_consecutive_answers(self, series, threshold=10):
        """
        Check if the series has more than 'threshold' consecutive identical answers.
        """
        count = 0
        last_answer = None
        for answer in series:
            if answer == last_answer:
                count += 1
                if count > threshold:
                    return True
            else:
                count = 1
                last_answer = answer
        return False

    def flag_consecutive_answers(self, merged_df, suspicious_df, consecutive_threshold=10):
        for prefix in self.prefixes:
            suspicious_df[f'Flagged_SameAnswer_{prefix}'] = False
            suspicious_df[f'Flagged_ConsecutiveSameAnswer_{prefix}'] = False

            for q in self.questionnaires:
                questionnaire_columns = merged_df.filter(regex=f'^{prefix}_{q}').columns
                if not questionnaire_columns.empty:
                    # Check if all answers are the same
                    all_same = merged_df[questionnaire_columns].nunique(axis=1) == 1
                    suspicious_df[f'Flagged_SameAnswer_{prefix}'] |= all_same

                    # Check for more than consecutive_threshold consecutive identical answers
                    for index, row in merged_df.iterrows():
                        if self.has_consecutive_answers(row[questionnaire_columns], threshold=consecutive_threshold):
                            suspicious_df.at[index, f'Flagged_ConsecutiveSameAnswer_{prefix}'] = True

            # Calculate and print the count of flagged instances for each type of flag
            count_all_same = suspicious_df[f'Flagged_SameAnswer_{prefix}'].sum()
            count_consecutive = suspicious_df[f'Flagged_ConsecutiveSameAnswer_{prefix}'].sum()
            self.logger.log_message(f"Total flags for all same answers in {prefix}: {count_all_same}")
            self.logger.log_message(f"Total flags for consecutive same answers in {prefix}: {count_consecutive}")

        return suspicious_df

    def flag_ips(self, merged_df, suspicious_df):
        IPs = ['Baseline_IPAddress', 'Exit_IPAddress']    
        for ip in IPs:
            merged_df[ip] = merged_df[ip].fillna('0.0.0')
            merged_df[ip] = merged_df[ip].apply(lambda x: '.'.join(str(x).split('.')[:3]))
            ip_dict = merged_df.groupby(ip).apply(
                        lambda x: list(x.index) if len(x.index) > 1 and x.index.nunique() > 1 else None
                    ).dropna().to_dict()
            
            accepted_ips = ['0.0.0', '144.82.8']
            for ip_prefix in accepted_ips:
                if ip_prefix in ip_dict:
                    del ip_dict[ip_prefix]
            email_to_ip = {email: ip for ip, emails in ip_dict.items() for email in emails if len(set(emails)) > 1}
            suspicious_df[f"Flagged_{ip}"] = merged_df.index.map(email_to_ip).notna()
            suspicious_df[f"Flagged_{ip}_Emails"] = merged_df.index.map(lambda email: ip_dict[email_to_ip[email]] if email in email_to_ip else [])
            # add column Flagged_{ip}_Count with the len of list from Flagged_{ip}_Emails
            suspicious_df[f"Flagged_{ip}_Count"] = suspicious_df[f"Flagged_{ip}_Emails"].apply(lambda x: len(x))
        return suspicious_df

    def remove_outliers(self,df, column_name):
        Q1 = df[column_name].quantile(0.25)
        Q3 = df[column_name].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return df[(df[column_name] >= lower_bound) & (df[column_name] <= upper_bound)][column_name]

    def flag_duration(self, prefixes, merged_df, suspicious_df):
        for stage in prefixes:
            duration_col = f'{stage}_Duration (in seconds)'

            # Filter out the outliers and calculate the mean
            clean_duration = self.remove_outliers(merged_df, duration_col)
            mean_duration = clean_duration.mean()

            # Print out the mean value excluding outliers
            self.logger.log_message(f"Mean {stage} Duration (in seconds, excluding outliers): {round(mean_duration,1)}")

            # Set cutoff point at 40% of the mean duration
            cutoff_duration = 0.4 * mean_duration

            # Add a flag to the original dataframe for suspiciously short durations
            suspicious_df[f'Flagged_{stage}_Duration'] = merged_df[duration_col] < cutoff_duration

            # Calculate and print the count of flagged durations
            count_flagged = suspicious_df[f'Flagged_{stage}_Duration'].sum()
            self.logger.log_message(f"Count of flagged {stage} Durations: {count_flagged}")

        return suspicious_df

    def flag_total(self, row, columns):

        count = 0
        for idx, item in enumerate(row):
            # Check if the column is not in the ignore list
            if columns[idx] not in self.ignore_total:
                # Count if the item is True (for boolean flags)
                if item is True:
                    count += 1
                # Count if the item is a non-empty list
                elif isinstance(item, list) and len(item) > 0:
                    count += 1
        return count
    

class QuestionnaireCompletion(QualtricsProcessing):

    def __init__(self):
        super().__init__()
        self.visq_categories = {
            'C': ['VISQ-01', 'VISQ-07', 'VISQ-08', 'VISQ-14', 'VISQ-15'],
            'D': ['VISQ-02', 'VISQ-06', 'VISQ-10', 'VISQ-13', 'VISQ-21'],
            'E': ['VISQ-09', 'VISQ-11', 'VISQ-17', 'VISQ-18', 'VISQ-19', 'VISQ-20', 'VISQ-22', 'VISQ-23', 'VISQ-24'],
            'O': ['VISQ-03', 'VISQ-04', 'VISQ-05', 'VISQ-12', 'VISQ-16'],
            'P': ['VISQ-19', 'VISQ-21', 'VISQ-22', 'VISQ-25', 'VISQ-26']
        }
        self.prefixes = ['Prescreening', 'Baseline', 'Exit']
        self.questionnaires = ['WEMWBS', 'GAD7', 'PHQ9', 'RRS', 'BIS', 'ASQ', 'NIEQ', 'VISQ']
        self.VISQ_categories = {
            'C': ['VISQ-01', 'VISQ-07', 'VISQ-08', 'VISQ-14', 'VISQ-15'],
            'D': ['VISQ-02', 'VISQ-06', 'VISQ-10', 'VISQ-13', 'VISQ-21'],
            'E': ['VISQ-09', 'VISQ-11', 'VISQ-17', 'VISQ-18', 'VISQ-19', 'VISQ-20', 'VISQ-22', 'VISQ-23', 'VISQ-24'],
            'O': ['VISQ-03', 'VISQ-04', 'VISQ-05', 'VISQ-12', 'VISQ-16'],
            'P': ['VISQ-19', 'VISQ-21', 'VISQ-22', 'VISQ-25', 'VISQ-26']
        }
        self.BIS_categories = {
            'Motor': ['BIS-1', 'BIS-2', 'BIS-3', 'BIS-4', 'BIS-5', 'BIS-7', 'BIS-10'],
            'Non-Planning': ['BIS-6', 'BIS-8', 'BIS-9', 'BIS-11', 'BIS-15'],
            'Attention': ['BIS-12', 'BIS-13', 'BIS-14']
        }

        self.NIEQ_categories = {'ISpeaking': ['NIEQ_1', 'NIEQ_6'],
                                'ISeeing': ['NIEQ_2', 'NIEQ_7'],
                                'Feeling': ['NIEQ_3', 'NIEQ_8'],
                                'SensAw': ['NIEQ_4', 'NIEQ_9'],
                                'UnsTh': ['NIEQ_5', 'NIEQ_10'],
                                }


    def remove_duplicates(self, merged_df):
        # TODO: This needs to be fixed; for simplicity we just keep the first entry for each email
        # Probably need to go case by case and see which users completed full version and when
        df = merged_df[~merged_df.index.duplicated(keep='first')].drop('Baseline_WEMWBS_Total', axis=1)
        return df

    def scoring_questionnaires(self, df):
        totals_df = pd.DataFrame(index=df.index)  # Ensure it has the same index as your main DataFrame.
        for prefix in self.prefixes:
            for q in self.questionnaires:
                questionnaire_columns = df.filter(regex=f'^{prefix}_{q}').columns
                if not questionnaire_columns.empty:
                    numeric_cols = df[questionnaire_columns].apply(pd.to_numeric, errors='coerce')
                    for col in numeric_cols:
                        if q == 'ASQ':
                            # Answers 1,2 recoded to 0 and 3,4 recoded to 1
                            numeric_cols[col] = numeric_cols[col].apply(lambda x: 0 if x in [1, 2] else (1 if x in [3, 4] else x))
                        elif q == 'NIEQ':
                            # NIEQ has question pairs which need to be averaged and then total sum calculated from averages
                            for category, questions in self.NIEQ_categories.items():
                                category_cols = [f'{prefix}_{item}' for item in questions]
                                totals_df[f'{prefix[:1]}_NIEQ_{category}'] = numeric_cols[category_cols].sum(axis=1)
                            nieq_pairs = [(f'{prefix}_{q}_{i}', f'{prefix}_{q}_{i+5}') for i in range(1, 6)]
                            totals_df[f'{prefix[:1]}_{q}_Total'] = sum(numeric_cols[[col1, col2]].mean(axis=1) for col1, col2 in nieq_pairs)

                        elif q == 'VISQ':
                            for category, questions in self.VISQ_categories.items():
                                category_cols = [f'{prefix}_{item}' for item in questions]
                                totals_df[f'{prefix[:1]}_VISQ_{category}'] = numeric_cols[category_cols].sum(axis=1)
                            totals_df[f'{prefix[:1]}_VISQ_Total'] = numeric_cols.sum(axis=1)
                        
                        elif q == 'BIS':
                            for category, questions in self.BIS_categories.items():
                                category_cols = [f'{prefix}_{item}' for item in questions]
                                # Sum the scores for each category
                                totals_df[f'{prefix[:1]}_BIS_{category.replace(" ", "_")}'] = numeric_cols[category_cols].sum(axis=1)
                            # Sum all BIS scores for total
                            totals_df[f'{prefix[:1]}_BIS_Total'] = numeric_cols.sum(axis=1)
                            
                    if q not in ['NIEQ', 'VISQ']:
                        totals_df[questionnaire_columns] = numeric_cols
                        totals_df[f'{prefix[:1]}_{q}_Total'] = numeric_cols.sum(axis=1)
                        totals_df = totals_df.copy()

        totals_df = totals_df[totals_df.columns.drop(totals_df.filter(regex='\d$').columns)]

        return totals_df 