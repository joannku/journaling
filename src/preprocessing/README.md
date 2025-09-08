# Preprocessing Pipeline

Data preprocessing pipeline for journaling study analysis. Processes raw data from Qualtrics surveys, bot interactions, and Google Sheets into analysis-ready datasets.

## Quick Start

```bash
# Run full pipeline
python _main.py

# Or run individual scripts
python preprocess_qualtrics.py
```

## Pipeline Overview

```
Raw Data → Cleaning → Integration → Anonymization → Analysis-Ready
```

## Execution Order

Scripts run in dependency order via `_main.py`:

1. **`pull_bot_data.py`** - Pull journal data from bot server
2. **`pull_qualtrics_data.py`** - Pull survey data from Qualtrics
3. **`preprocess_qualtrics.py`** - Clean, merge, score Qualtrics surveys
4. **`anonymise_emails.py`** - Map emails to participant IDs, anonymize
5. **`preprocess_entries.py`** - Integrate journals + summaries, clean content
6. **`anonymise_content.py`** - Anonymize journal content using BERT NER
7. **`tokenize_sentences.py`** - Split journal entries into sentences
8. **`merge_qual_with_anon_jours.py`** - Merge surveys with journal data
9. **`update_status.py`** - Sync participant status from Google Sheets
10. **`study_group.py`** - Extract experimental group assignments
11. **`filter_by_status.py`** - Filter to analysis-eligible participants

## File Functions

### Data Collection
- **`pull_bot_data.py`** - Downloads journal entries from OneReach bot
- **`pull_qualtrics_data.py`** - Downloads survey responses from Qualtrics API

### Survey Processing
- **`preprocess_qualtrics.py`** - Processes prescreening, baseline, exit surveys; calculates scores (PHQ9, GAD7, WEMWBS, etc.)

### Journal Processing
- **`anonymise_emails.py`** - Creates email→PID mapping; anonymizes participant identifiers
- **`preprocess_entries.py`** - Merges journals + GPT summaries; standardizes format; cleans content
- **`anonymise_content.py`** - Removes personal information using BERT NER model
- **`tokenize_sentences.py`** - Splits journal entries into individual sentences for analysis

### Data Integration
- **`merge_qual_with_anon_jours.py`** - Combines survey data with journal data; creates full and anonymized versions

### Study Management
- **`update_status.py`** - Pulls participant eligibility status from Google Sheets
- **`study_group.py`** - Extracts experimental group assignments (control/intervention)
- **`filter_by_status.py`** - Creates final datasets with only eligible participants

### Utilities
- **`path_utils.py`** - Centralized path management for all scripts
- **`_main.py`** - Pipeline orchestrator; runs scripts in correct order

## Key Outputs

| File | Description |
|------|-------------|
| `1_journals_anon_email.csv` | Journals with anonymized emails |
| `2_journals_preprocessed.csv` | Cleaned journals + summaries |
| `3_qualtrics_totals.csv` | Scored survey responses |
| `4_journals_anon_content_both.csv` | Content-anonymized journals |
| `5_journals_anon_content_only.csv` | Fully anonymized journals |
| `6_anon_utterances.csv` | Sentence-level journal data |
| `7_qual_jour_merged.csv` | Surveys + journals (with emails) |
| `8_qual_jour_merged_anon.csv` | Surveys + journals (anonymized) |
| `9_qual_utt_merged.csv` | Surveys + utterances (with emails) |
| `10_qual_utt_merged_anon.csv` | Surveys + utterances (anonymized) |
| `11_journals_qualified_for_analysis.csv` | Final journal dataset |
| `12_utterances_qualified_for_analysis.csv` | Final utterance dataset |

## Dependencies

- pandas, numpy - Data processing
- transformers, torch - BERT NER for anonymization
- nltk - Sentence tokenization
- gspread, oauth2client - Google Sheets integration
- requests - API calls

## Configuration

- **`config/creds.json`** - API credentials
- **`config/email_pid.json`** - Email to participant ID mapping
- **`config/study_outcome_by_pid.json`** - Participant eligibility status
- **`config/study_groups_by_pid.json`** - Experimental group assignments
