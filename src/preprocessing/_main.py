import os
import sys
import subprocess

def _find_repo_root(start_dir):
    current = os.path.abspath(start_dir)
    while True:
        if os.path.isdir(os.path.join(current, 'src')) and os.path.isdir(os.path.join(current, 'config')):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return os.path.abspath(start_dir)
        current = parent

# Define the core directory for the entire project (works from interactive runs)
_here = os.path.dirname(os.path.abspath(__file__))
CORE_DIR = os.environ.get('JOURNALING_CORE_DIR', _find_repo_root(_here))

SCRIPTS = [
    'pull_bot_data.py', # pull data from bot server
    'pull_qualtrics_data.py', # pull data from qualtrics
    'preprocess_qualtrics.py', # clean, merge and score qualtrics data
    'anonymise_emails.py',  # anonymise emails
    'preprocess_entries.py', # data integration, standardise, metadata
    'anonymise_content.py', # anonymise content using BERT NER
    'tokenize_sentences.py', # tokenize sentences using NLTK sentence tokenizer
    'merge_qual_with_anon_jours.py', # merge qualtrics data with anonymised journal data
    'update_status.py', # study completion management tool
    'study_group.py', # export study groups by PID
    'filter_by_status.py' # filter by status - include only Eligible ppts
]

def run_scripts(preproc_dir):
    # Set working directory to repo root for all scripts
    repo_root = CORE_DIR
    print(f"Python executable: {sys.executable}")
    print(f"Repo root: {repo_root}")
    for script in SCRIPTS:
        path = os.path.abspath(os.path.join(preproc_dir, script))
        if not os.path.isfile(path):
            print(f"Missing script: {path}")
            sys.exit(2)
        print(f"=> Running {path}")
        # Pass CORE_DIR as environment variable to child processes
        env = os.environ.copy()
        env['JOURNALING_CORE_DIR'] = CORE_DIR
        result = subprocess.run([sys.executable, path], cwd=repo_root, env=env)
        if result.returncode != 0:
            print(f"Failed: {script} (exit {result.returncode})")
            sys.exit(result.returncode)


if __name__ == '__main__':
    run_scripts(os.path.join(CORE_DIR, 'src', 'preprocessing'))

