import pandas as pd
import json
import os

# from .GoogleSheetHandler import GoogleSheetHandler


class JournalAnalysisManager:
    def __init__(self, creds_path=None):
        
        # Load telegram ID from credentials file
        if creds_path is None:
            # Default path relative to project root
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            creds_path = os.path.join(project_root, 'config', 'creds.json')
        
        with open(creds_path, 'r') as f:
            creds = json.load(f)
        
        self.my_telegram_id = creds['my_telegram_id']  # Required field in creds
        
        # Use relative path from project root
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.path_data = os.path.join(project_root, 'data', 'raw', 'onereachai')
        self.path_gptsummaries = os.path.join(self.path_data, 'tsj_gptsummaries.csv')
        self.flagged_columns = [
            "IP_Emails_Baseline_Flagged",
            "IP_Emails_Exit_Flagged",
            "SameAnswer_Prescreening_Flagged",
            "SameAnswer_Baseline_Flagged",
            "SameAnswer_Exit_Flagged",
            "Consecutive_Prescreening_Flagged",
            "Consecutive_Baseline_Flagged",
            "Consecutive_Exit_Flagged",
            "Duration_Prescreening_Flagged",
            "Duration_Baseline_Flagged",
            "Duration_Exit_Flagged",
        ]

        self.desired_colorder = [
            "Email",
            "MatchedEmail",
            "TestUser",
            "Prescreening_Completed",
            "Baseline_Completed",
            "Exit_Completed",
            "Group",
            "SummariesReceived",
            "TotalWords",
            "AverageWordsPerDay",
            "DaysJournalled",
            "PercentageOfDaysJournalled",
            "SufficientCompletion",
            "IP_Emails_Baseline_Flagged",
            "IP_Emails_Exit_Flagged",
            "SameAnswer_Prescreening_Flagged",
            "SameAnswer_Baseline_Flagged",
            "SameAnswer_Exit_Flagged",
            "Consecutive_Prescreening_Flagged",
            "Consecutive_Baseline_Flagged",
            "Consecutive_Exit_Flagged",
            "Duration_Prescreening_Flagged",
            "Duration_Baseline_Flagged",
            "Duration_Exit_Flagged",
            "PaymentType",
            "PaymentQualified",
            "CreditsQualified",
            "RewardGranted",
            "EmailSent",
            "DayCount",
            "Day0",
            "Day1",
            "Day2",
            "Day3",
            "Day4",
            "Day5",
            "Day6",
            "Day7",
            "Day8",
            "Day9",
            "Day10",
            "Day11",
            "Day12",
            "Day13",
            "Day14",
            "Day15",
            "Day16",
            "Day17",
            "Day18",
            "Day19",
            "Day20",
        ]

    def load_df(self, table_name):
        df = pd.read_csv(os.path.join(self.path_data, f"tsj_{table_name}.csv"))
        return df

    def lowercase_column(self, df, colname):
        df[colname] = df[colname].str.lower()
        return df

    def move_column(self, dataframe, col_to_move, pos):
        """
        Move a column to a specific position in a DataFrame.

        Parameters:
        - dataframe: The DataFrame whose column you want to move.
        - col_to_move: The column you want to move.
        - pos: The position where you want the column to be moved.

        Returns:
        - A DataFrame with the column moved to the desired position.
        """
        cols = dataframe.columns.tolist()
        cols.insert(pos, cols.pop(cols.index(col_to_move)))
        return dataframe[cols]

    def sort_columns_to_desired_order(self, df):
        df = df[self.desired_colorder]

        return df

    def offset_to_timedelta(self, offset_str):
        sign = 1 if offset_str[0] == "+" else -1
        hours, minutes = map(int, offset_str[1:].split(":"))
        return pd.Timedelta(hours=sign * hours, minutes=sign * minutes)

    def qualify_payment(self, df):
        # Iterate over every fow
        for index, row in df.iterrows():
            # Count the number of flagged columns where value is True
            flagged = sum(row[self.flagged_columns])

            if row["SufficientCompletion"].lower() != "true":
                df.at[index, "PaymentQualified"] = False
                df.at[index, "CreditsQualified"] = False
                continue

            if (
                str(row["Prescreening_Completed"]).lower() != "true"
                and str(row["Baseline_Completed"]).lower() != "true"
                and str(row["Exit_Completed"]).lower() != "true"
            ):
                df.at[index, "PaymentQualified"] = False
                df.at[index, "CreditsQualified"] = False
                continue

            if pd.isna(row["PaymentType"]):
                df.at[index, "PaymentQualified"] = "CheckWarranted"
                df.at[index, "CreditsQualified"] = "CheckWarranted"
                continue

            elif row["PaymentType"] == "Voucher":
                df.at[index, "CreditsQualified"] = False
                if flagged > 2 and flagged < 5:
                    df.at[index, "PaymentQualified"] = "CheckWarranted"
                    continue
                elif flagged >= 5:
                    df.at[index, "PaymentQualified"] = False
                    continue
                else:
                    df.at[index, "PaymentQualified"] = True
                    continue

            elif row["PaymentType"] == "Credits":
                df.at[index, "PaymentQualified"] = False
                df.at[index, "CreditsQualified"] = True
                continue

            elif row["PaymentType"] == "Donation":
                df.at[index, "PaymentQualified"] = False
                df.at[index, "CreditsQualified"] = False
                continue

            else:
                df.at[index, "PaymentQualified"] = "Error"

                continue

        return df

    def has_dropped_out(self, row):
        day_columns = [
            "Day0",
            "Day1",
            "Day2",
            "Day3",
            "Day4",
            "Day5",
            "Day6",
            "Day7",
            "Day8",
            "Day9",
            "Day10",
            "Day11",
            "Day12",
            "Day13",
        ]

        # Get the current DayCount for the user
        current_day_count = row["DayCount"]

        # Convert day columns up to current DayCount into a binary string of 0s and 1s
        # 0 if no journaling on that day (word count == 0), 1 otherwise.
        binary_string = "".join(
            [
                "0" if row[col] == 0 else "1"
                for col in day_columns[: current_day_count + 1]
            ]
        )

        # Check if '000' is present in the binary string
        return "000" in binary_string

    def calculate_summary_count(self):
        df = pd.read_csv(self.path_gptsummaries)
        # Count the number of summaries per TelegramID
        df["SummariesReceived"] = df.groupby("TelegramID")["TelegramID"].transform(
            "count"
        )
        # Drop duplicate rows, keeping the first occurrence
        df = df.drop_duplicates(subset="TelegramID")
        # Only keep the TelegramID and SummariesReceived columns
        df = df[["TelegramID", "SummariesReceived"]]
        return df

    def remove_myself(self, df):
        df = df[df.TelegramID != self.my_telegram_id]
        return df