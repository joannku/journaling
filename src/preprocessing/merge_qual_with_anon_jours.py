import os
import pandas as pd
from path_utils import get_core_dir


def load_data(base_dir):
    qual = pd.read_csv(os.path.join(base_dir, 'data/processed/3_qualtrics_totals.csv'))
    journals = pd.read_csv(os.path.join(base_dir, 'data/processed/5_journals_anon_content_only.csv'))
    users = pd.read_csv(os.path.join(base_dir, 'data/raw/onereachai/tsj_usertable.csv'))
    jour_src = pd.read_csv(os.path.join(base_dir, 'data/raw/onereachai/tsj_journals_saved.csv'))
    utt = pd.read_csv(os.path.join(base_dir, 'data/processed/6_anon_utterances.csv'))
    return qual, journals, users, jour_src, utt


def show_overlap(df, users):
    print("Emails where ParticipantID is NaN:", df[df['ParticipantID'].isna()]['Email'].unique())
    print("ParticipantIDs where Email is NaN:", df[df['Email'].isna()]['ParticipantID'].unique())
    valid = df['Email'].notna() & df['ParticipantID'].notna()
    print("Number of journal entries from valid participants:", valid.sum())
    print("Number of unique participants:", df.loc[valid, 'ParticipantID'].nunique())
    print("Users who registered with the bot but are not present in the df:",
          set(users['ParticipantID'].unique()) - set(df['ParticipantID'].unique()))


def merge_and_write(left, right, users, jour_src, full_out, anon_out):
    df = pd.merge(left, right, on='ParticipantID', how='outer')
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    show_overlap(df, users)
    df.to_csv(full_out, index=False); print(f"Saved {full_out}")
    anon = df[df['ParticipantID'].notna()].drop(columns=['Email'])
    print("Journal IDs from source data that are not present in the df:",
          set(jour_src['JournalUniqueID'].unique()) - set(anon['JournalUniqueID'].unique()))
    anon.to_csv(anon_out, index=False); print(f"Saved {anon_out}")


if __name__ == '__main__':
    base_dir = get_core_dir()
    print(f"Base directory: {base_dir}")
    qual, journals, users, jour_src, utt = load_data(base_dir)

    print("### Full journals ###")
    merge_and_write(
        left=qual, right=journals, users=users, jour_src=jour_src,
        full_out=os.path.join(base_dir, 'data/processed/7_qual_jour_merged.csv'),
        anon_out=os.path.join(base_dir, 'data/processed/8_qual_jour_merged_anon.csv'),
    )

    print("### Utterances ###")
    merge_and_write(
        left=qual, right=utt, users=users, jour_src=jour_src,
        full_out=os.path.join(base_dir, 'data/processed/9_qual_utt_merged.csv'),
        anon_out=os.path.join(base_dir, 'data/processed/10_qual_utt_merged_anon.csv'),
    )
