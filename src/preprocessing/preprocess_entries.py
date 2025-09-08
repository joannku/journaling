import os
import sys
import pandas as pd
from path_utils import setup_paths

# Setup paths and get CORE_DIR
CORE_DIR = setup_paths()

def map_telegram_to_pid(df, dfu, matching_column='TelegramID'):

    """
    df: Merged table where participant ID is to be mapped
    dfu: User table dataframe (must contain PID and TelegramID)
    """
    
    # Take ParticipantID and TelegramID from dfu and create a dict
    pid_telegram_dict = dfu.set_index('TelegramID')['ParticipantID'].to_dict()
    # Iterate over all rows in df and map TelegramID to ParticipantID
    df['ParticipantID'] = df[matching_column].map(pid_telegram_dict)
    return df


def preprocess_data(dfu, dfj, dfs):

    """ 
    dfu: User table dataframe
    dfj: Journal table dataframe
    dfs: GPT-4 Summary table dataframe

    Returns a merged dataframe of the user and journal tables
    """

    dfs = map_telegram_to_pid(dfs, dfu)

    # Process the user table
    dfu = dfu[['TelegramID', 'StudyGroup']]
    

    # Process the journal table
    dfj = dfj.rename(columns={'JournalContent': 'Content', 'EntryType': 'Type', 'JournalTimestamp': 'Timestamp'})
    dfj['EntryCount'] = 1

    # Process the GPT-4 Summary table
    dfs['Type'] = 'Summary'
    dfs = dfs.rename(columns={'GptSummary': 'Content'})

    # Merge the tables
    dfuj = pd.merge(dfu, dfj, on='TelegramID', how='left')
    dfus = pd.merge(dfu, dfs, on='TelegramID', how='left')
    dfujs = pd.concat([dfuj, dfus])

    # Process the merged table
    cols_to_check = ['Timestamp', 'JournalUniqueID', 'Type', 'Content', 'EntryCount']
    dfujs = dfujs.dropna(subset=cols_to_check, how='all')

    dfujs['Content'] = dfujs['Content'].str.replace('2023: none', '2023:')
    dfujs['Content'] = dfujs['Content'].str.replace(r'^(.*?\d{4}):\s*', '', regex=True)
    dfujs['Content'] = dfujs['Content'].str.replace(r'Here are (my|the) responses to.*', '', regex=True)
    dfujs['WordCount'] = dfujs['Content'].str.split().str.len()

    # Reorder columns 
    dfujs = dfujs[['ParticipantID', 'TelegramID', 'StudyGroup', 'Timestamp', 'JournalUniqueID', 'Type', 'EntryCount', 'Content']]
    # dfujs = map_telegram_to_pid(dfujs, dfu)

    dfujs.sort_values(by=['Timestamp'], inplace=True)

    return dfujs
    

if __name__ == '__main__':

    dfu = pd.read_csv(os.path.join(CORE_DIR, 'data', 'raw', 'onereachai', 'tsj_usertable.csv'))
    dfj = pd.read_csv(os.path.join(CORE_DIR, 'data', 'processed', '1_journals_anon_email.csv'))
    dfs = pd.read_csv(os.path.join(CORE_DIR, 'data', 'raw', 'onereachai', 'tsj_gptsummaries.csv'))

    filepath = os.path.join(CORE_DIR, 'data', 'processed', '2_journals_preprocessed.csv')
    df = preprocess_data(dfu, dfj, dfs)
    df.to_csv(filepath, index=False)
    
    print(f"File exported to {filepath}.")


