import pandas as pd
import json
import os
from path_utils import get_core_dir

if __name__ == '__main__':
    
    CORE_DIR = get_core_dir()
    
    # Load user table
    df = pd.read_csv(os.path.join(CORE_DIR, 'data/raw/onereachai/tsj_usertable.csv'))
    # Use ParticipantID as Key and StudyGroup as value
    study_group = df.set_index('ParticipantID')['StudyGroup'].to_dict()
    # Export 
    with open(os.path.join(CORE_DIR, 'config/study_groups_by_pid.json'), 'w') as f:
        json.dump(study_group, f, indent=4)
