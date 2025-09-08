"""
Final participant filtering for analysis datasets.

This script applies comprehensive filtering criteria to create a final dataset
of high-quality participants suitable for all downstream analyses.
It serves as the final step in the preprocessing pipeline.

Filtering criteria:
- Complete mental health data (WEMWBS, GAD7, PHQ9 baseline & exit)
- Non-zero WEMWBS scores (if configured)
- Journal entries with > 3 words
- Exclude summary entries (if configured)
- Minimum 6 entries per participant
- Only eligible/insufficient participants (excludes fraudulent)
"""

import os
import pandas as pd
import json
import warnings
from dataclasses import dataclass, field
from typing import List, Optional

warnings.filterwarnings('ignore')

# Setup paths using same logic as other preprocessing scripts
def _find_repo_root(start_dir):
    current = os.path.abspath(start_dir)
    while True:
        if os.path.isdir(os.path.join(current, 'src')) and os.path.isdir(os.path.join(current, 'config')):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return os.path.abspath(start_dir)
        current = parent

_here = os.path.dirname(os.path.abspath(__file__))
CORE_DIR = os.environ.get('JOURNALING_CORE_DIR', _find_repo_root(_here))
DATA_DIR = os.path.join(CORE_DIR, 'data', 'processed')
CONFIG_DIR = os.path.join(CORE_DIR, 'config')


@dataclass
class FinalFilterConfig:
    """Configuration for final participant filtering parameters."""
    required_cols: List[str] = field(default_factory=lambda: [
        'B_WEMWBS_Total', 'E_WEMWBS_Total', 'B_GAD7_Total',
        'E_GAD7_Total', 'B_PHQ9_Total', 'E_PHQ9_Total'
    ])
    enforce_nonzero_wemwbs: bool = True
    min_word_count: int = 4  # strictly greater than 3 words
    exclude_type: Optional[str] = 'Summary'
    min_entries_per_participant: int = 6  # strictly more than 5
    eligible_only: bool = True  # keep only 'Eligible' and 'Insufficient' from outcomes
    study_outcome_filename: str = 'study_outcome_by_pid.json'


def load_study_outcomes():
    """Load study outcome data from config."""
    outcome_path = os.path.join(CONFIG_DIR, 'study_outcome_by_pid.json')
    if os.path.exists(outcome_path):
        with open(outcome_path, 'r') as f:
            return json.load(f)
    else:
        print(f"⚠️  Study outcome file not found: {outcome_path}")
        return {}


def apply_final_filters(df, config=None):
    """Apply final filtering criteria to the dataset."""
    if config is None:
        config = FinalFilterConfig()
    
    print("Starting final participant filtering...")
    print(f"Initial dataset: {len(df)} journal entries, {df['ParticipantID'].nunique()} unique participants")
    
    # Step 1: Remove rows with missing mental health data
    initial_entries = len(df)
    df = df.dropna(subset=config.required_cols).reset_index(drop=True)
    entries_after_mh = len(df)
    print(f"After removing missing mental health data: {entries_after_mh} entries ({initial_entries - entries_after_mh} removed)")
    
    # Step 2: Remove zero WEMWBS scores if configured
    if config.enforce_nonzero_wemwbs:
        df = df[(df['B_WEMWBS_Total'] != 0) & (df['E_WEMWBS_Total'] != 0)]
        entries_after_zero = len(df)
        print(f"After removing zero WEMWBS scores: {entries_after_zero} entries ({entries_after_mh - entries_after_zero} removed)")
    
    # Step 3: Calculate word count and filter by content quality
    df['WordCount'] = df['JournalAnonymised'].str.split().str.len()
    df = df[df['WordCount'] >= config.min_word_count]
    entries_after_words = len(df)
    print(f"After filtering entries with < {config.min_word_count} words: {entries_after_words} entries ({entries_after_zero - entries_after_words} removed)")
    
    # Step 4: Exclude specific entry types (e.g., Summary)
    if config.exclude_type is not None:
        df = df[df['Type'] != config.exclude_type]
        entries_after_type = len(df)
        print(f"After excluding '{config.exclude_type}' entries: {entries_after_type} entries ({entries_after_words - entries_after_type} removed)")
    
    # Step 5: Calculate entry count per participant and filter
    df['EntryCount'] = df.groupby('ParticipantID')['ParticipantID'].transform('count')
    participants_before_entries = df['ParticipantID'].nunique()
    df = df[df['EntryCount'] >= config.min_entries_per_participant]
    participants_after_entries = df['ParticipantID'].nunique()
    print(f"After requiring >= {config.min_entries_per_participant} entries per participant: {participants_after_entries} participants ({participants_before_entries - participants_after_entries} removed)")
    
    # Step 6: Keep only first row per participant for participant-level analysis
    df_participant = df.groupby('ParticipantID').first().reset_index()
    
    # Step 7: Filter out fraudulent participants
    if config.eligible_only:
        study_outcomes = load_study_outcomes()
        if study_outcomes:
            df_participant['StudyOutcome'] = df_participant['ParticipantID'].map(study_outcomes)
            
            participants_before_fraud = len(df_participant)
            df_participant = df_participant[
                df_participant['StudyOutcome'].isin(['Eligible', 'Insufficient'])
            ].copy()
            participants_after_fraud = len(df_participant)
            
            excluded_by_fraud = participants_before_fraud - participants_after_fraud
            print(f"After removing fraudulent participants: {participants_after_fraud} participants ({excluded_by_fraud} removed)")
            
            # Show breakdown of excluded participants
            if excluded_by_fraud > 0:
                temp_df = df.groupby('ParticipantID').first().reset_index()
                temp_df['StudyOutcome'] = temp_df['ParticipantID'].map(study_outcomes)
                excluded_outcomes = temp_df[~temp_df['StudyOutcome'].isin(['Eligible', 'Insufficient'])]['StudyOutcome'].value_counts().to_dict()
                print(f"  Breakdown of excluded participants: {excluded_outcomes}")
        else:
            print("⚠️  No study outcome data available - skipping fraud filtering")
    
    print(f"\nFinal participant-level dataset: {len(df_participant)} participants")
    print(f"Study group distribution: {df_participant['StudyGroup'].value_counts().to_dict()}")
    
    return df_participant


def filter_journals_and_utterances(filtered_participant_ids):
    """Filter journal and utterance datasets to match final filtered participants."""
    print("\n" + "=" * 60)
    print("FILTERING JOURNALS AND UTTERANCES")
    print("=" * 60)
    
    # Filter journals dataset
    journals_path = os.path.join(DATA_DIR, '11_journals_qualified_for_analysis.csv')
    if os.path.exists(journals_path):
        print(f"Loading journals from: {journals_path}")
        df_journals = pd.read_csv(journals_path)
        
        # Filter to only participants in final filtered set
        initial_journal_entries = len(df_journals)
        df_journals_filtered = df_journals[df_journals['ParticipantID'].isin(filtered_participant_ids)].copy()
        final_journal_entries = len(df_journals_filtered)
        
        print(f"Journal entries: {initial_journal_entries} → {final_journal_entries} ({initial_journal_entries - final_journal_entries} removed)")
        print(f"Unique participants in filtered journals: {df_journals_filtered['ParticipantID'].nunique()}")
        
        # Save filtered journals
        journals_output_path = os.path.join(DATA_DIR, '14_journals_final_filtered.csv')
        df_journals_filtered.to_csv(journals_output_path, index=False)
        print(f"✅ Filtered journals saved to: {journals_output_path}")
    else:
        print(f"⚠️  Journals file not found: {journals_path}")
    
    # Filter utterances dataset
    utterances_path = os.path.join(DATA_DIR, '12_utterances_qualified_for_analysis.csv')
    if os.path.exists(utterances_path):
        print(f"\nLoading utterances from: {utterances_path}")
        df_utterances = pd.read_csv(utterances_path)
        
        # Filter to only participants in final filtered set
        initial_utterance_entries = len(df_utterances)
        df_utterances_filtered = df_utterances[df_utterances['ParticipantID'].isin(filtered_participant_ids)].copy()
        final_utterance_entries = len(df_utterances_filtered)
        
        print(f"Utterance entries: {initial_utterance_entries} → {final_utterance_entries} ({initial_utterance_entries - final_utterance_entries} removed)")
        print(f"Unique participants in filtered utterances: {df_utterances_filtered['ParticipantID'].nunique()}")
        
        # Save filtered utterances
        utterances_output_path = os.path.join(DATA_DIR, '15_utterances_final_filtered.csv')
        df_utterances_filtered.to_csv(utterances_output_path, index=False)
        print(f"✅ Filtered utterances saved to: {utterances_output_path}")
    else:
        print(f"⚠️  Utterances file not found: {utterances_path}")


def main():
    """Main function to apply final participant filtering."""
    print("=" * 60)
    print("FINAL PARTICIPANT FILTERING")
    print("=" * 60)
    
    # Load input dataset
    input_path = os.path.join(DATA_DIR, '11_journals_qualified_for_analysis.csv')
    if not os.path.exists(input_path):
        print(f"❌ Input file not found: {input_path}")
        print("Please run the main preprocessing pipeline first.")
        return
    
    print(f"Loading dataset from: {input_path}")
    df = pd.read_csv(input_path)
    
    # Apply filters
    config = FinalFilterConfig()
    df_filtered = apply_final_filters(df, config)
    
    # Calculate change scores
    df_filtered['Change_WEMWBS'] = df_filtered['E_WEMWBS_Total'] - df_filtered['B_WEMWBS_Total']
    df_filtered['Change_GAD7'] = df_filtered['E_GAD7_Total'] - df_filtered['B_GAD7_Total']
    df_filtered['Change_PHQ9'] = df_filtered['E_PHQ9_Total'] - df_filtered['B_PHQ9_Total']
    
    # Save filtered participant dataset
    output_path = os.path.join(DATA_DIR, '13_participants_final_filtered.csv')
    df_filtered.to_csv(output_path, index=False)
    
    print(f"\n✅ Final filtered participants saved to: {output_path}")
    print(f"   {len(df_filtered)} participants ready for all analyses")
    
    # Show summary statistics
    print("\nSummary Statistics:")
    print("-" * 30)
    for group in ['A', 'B', 'C']:
        group_data = df_filtered[df_filtered['StudyGroup'] == group]
        if len(group_data) > 0:
            group_name = {'A': 'Cognitive Sum.', 'B': 'Emotional Sum.', 'C': 'No Sum.'}.get(group, group)
            print(f"{group_name}: {len(group_data)} participants")
    
    print(f"\nMental Health Measures Available:")
    for col in config.required_cols:
        non_null = df_filtered[col].notna().sum()
        print(f"  {col}: {non_null}/{len(df_filtered)} participants")
    
    # Filter journals and utterances to match final participants
    filtered_participant_ids = df_filtered['ParticipantID'].tolist()
    filter_journals_and_utterances(filtered_participant_ids)


if __name__ == "__main__":
    main()
