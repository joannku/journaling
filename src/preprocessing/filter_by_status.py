import pandas as pd 
import json
import os
from path_utils import get_core_dir

if __name__ == '__main__':
    
    CORE_DIR = get_core_dir()
    
    # Load the data
    df = pd.read_csv(os.path.join(CORE_DIR, 'data/processed/8_qual_jour_merged_anon.csv'))
    # Load the json file with the study outcome
    with open(os.path.join(CORE_DIR, 'config/study_outcome_by_pid.json'), 'r') as file:
        email_category = json.load(file)

    # Filter by status - include only Eligible or Insufficient
    df = df[df['ParticipantID'].map(email_category).isin(['Eligible', 'Insufficient'])]
    # Find len of unique ParticipantID
    print(f"Included count: {len(df['ParticipantID'].unique())}")

    # Save to csv
    df.to_csv(os.path.join(CORE_DIR, 'data/processed/11_journals_qualified_for_analysis.csv'), index=False)

    # Do the same for utterances data
    
    # Load the data
    dfx = pd.read_csv(os.path.join(CORE_DIR, 'data/processed/10_qual_utt_merged_anon.csv'))

    # Filter by status - include only Eligible or Insufficient
    dfx = dfx[dfx['ParticipantID'].map(email_category).isin(['Eligible', 'Insufficient'])]
    # Find len of unique ParticipantID
    print(f"Included count: {len(dfx['ParticipantID'].unique())}")

    # Save to csv
    dfx.to_csv(os.path.join(CORE_DIR, 'data/processed/12_utterances_qualified_for_analysis.csv'), index=False)