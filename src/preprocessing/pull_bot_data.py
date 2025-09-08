"""
This script pulls data from the bot server.
"""

import sys
import os

# Set up paths relative to script location
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
SRC_DIR = os.path.join(BASE_DIR, 'src')
sys.path.append(SRC_DIR)

from modules.OneReach import OneReachRequests

if __name__ == "__main__":
    creds = os.path.join(BASE_DIR, 'config', 'creds.json')
    onereach = OneReachRequests(creds)
    onereach.pull_all_data(path=os.path.join(BASE_DIR, 'data', 'raw', 'onereachai'))