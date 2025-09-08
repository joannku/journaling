import os
import sys
from datetime import datetime
import subprocess
import pandas as pd
import json
from path_utils import get_core_dir


def map_email_to_pid(df, dfu, matching_column='TelegramID', core_dir=None):
    if 'Email' in df.columns:
        df['Email'] = df['Email'].str.lower()
    if 'Email' in dfu.columns:
        dfu['Email'] = dfu['Email'].str.lower()

    df = df.merge(dfu[[matching_column, 'ParticipantID']], on=matching_column, how='left')
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    # Get core directory if not provided
    if core_dir is None:
        core_dir = get_core_dir()
    
    email_pid = dfu.set_index('Email')['ParticipantID'].to_dict() if 'Email' in dfu.columns else {}
    with open(os.path.join(core_dir, 'config', 'email_pid.json'), 'w') as file:
        json.dump(email_pid, file, indent=4)
    return df


if __name__ == '__main__':
    BASE_DIR = get_core_dir()

    info_path = os.path.join(BASE_DIR, 'data', 'raw', 'onereachai', 'info.txt')
    if os.path.exists(info_path):
        with open(info_path, 'r') as file:
            timestamp = file.read().strip()
        if (datetime.now() - datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')).total_seconds() > 43200:
            print('Pulling new data...')
            subprocess.run([sys.executable, os.path.join(BASE_DIR, 'src', 'preprocessing', 'pullBotData.py')], check=False)
        else:
            print('Data is up to date.')

    dfu = pd.read_csv(os.path.join(BASE_DIR, 'data', 'raw', 'onereachai', 'tsj_usertable.csv'))
    dfj = pd.read_csv(os.path.join(BASE_DIR, 'data', 'raw', 'onereachai', 'tsj_journals_saved.csv'))

    out_path = os.path.join(BASE_DIR, 'data', 'processed', '1_journals_anon_email.csv')

    # Exclude already processed JournalUniqueID values if file exists
    if os.path.exists(out_path):
        dfj_processed = pd.read_csv(out_path)
        existing_ids = set(dfj_processed.get('JournalUniqueID', pd.Series(dtype=str)).tolist())
        dfj = dfj[~dfj['JournalUniqueID'].isin(existing_ids)]

    dfx = map_email_to_pid(dfj, dfu, matching_column='TelegramID')

    if dfx.empty:
        print('No new data to process.')
        sys.exit(0)

    if os.path.exists(out_path):
        old = pd.read_csv(out_path)
        dfx = pd.concat([old, dfx], ignore_index=True)
        if 'Unnamed: 0' in dfx.columns:
            dfx = dfx.drop(columns=['Unnamed: 0'])

    dfx = dfx.sort_values(by='JournalTimestamp')
    dfx = dfx[['ParticipantID', 'TelegramID', 'JournalTimestamp', 'JournalUniqueID', 'EntryType', 'JournalContent']]
    dfx = dfx.reset_index(drop=True)
    dfx.to_csv(out_path, index=False)
    print(f"File exported to {out_path}.")