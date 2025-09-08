import nltk
from nltk.tokenize import sent_tokenize
import pandas as pd
import os
from path_utils import get_core_dir

nltk.download('punkt')

def split_into_sentences(text):
    return sent_tokenize(text)

if __name__ == '__main__':

    CORE_DIR = get_core_dir()
    print(f"Base directory: {CORE_DIR}")

    df = pd.read_csv(os.path.join(CORE_DIR, 'data/processed/5_journals_anon_content_only.csv'))
    # Remove all rows of df where value of Content col is nan
    df = df[df['JournalAnonymised'].notna()]

    # Apply the function and explode the DataFrame
    df['Utterance'] = df['JournalAnonymised'].apply(split_into_sentences)
    df_exploded = df.explode('Utterance').reset_index(drop=True)

    # Add a column "WordCount" to df_exploded
    df_exploded['WordCount'] = df_exploded['Utterance'].str.split().str.len()

    df_exploded['UtteranceID'] = df_exploded.groupby('JournalUniqueID').cumcount() + 1
    df_exploded['UtteranceID'] = df_exploded['UtteranceID'].fillna(0).astype(int).astype(str)
    df_exploded['UtteranceID'] = df_exploded['JournalUniqueID'] + '_' + df_exploded['UtteranceID']

    df = df_exploded
    filepath = os.path.join(CORE_DIR, 'data', 'processed', '6_anon_utterances.csv')
    df.to_csv(filepath)
    print(f"File exported to {filepath}.")