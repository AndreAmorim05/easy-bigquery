import os
import pathlib

from decouple import Config, RepositoryEnv
from decouple import config as cfg

ROOT = pathlib.Path(__file__).resolve().parent.parent
BASE_DIR = ROOT.parent
LOCAL_ENV_FILE = os.path.join(BASE_DIR, 'secrets', '.env')
if os.path.exists('/secrets/.env'):
    config = Config(RepositoryEnv('/secrets/.env'))
elif os.path.exists(LOCAL_ENV_FILE):
    config = Config(RepositoryEnv(LOCAL_ENV_FILE))
else:
    config = cfg

# Configuration for Google BigQuery connection
# These values can be set in the environment or in a .env file.
BQ_JSON_CREDENTIALS = config('BQ_JSON_CREDENTIALS', cast=str, default=None)
BQ_PROJECT_ID = config('BQ_PROJECT_ID', cast=str, default=None)
BQ_TABLE_NAME = config('BQ_TABLE_NAME', cast=str, default=None)
BQ_DATASET = config('BQ_DATASET', cast=str, default=None)
