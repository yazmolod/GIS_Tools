import os
import sys
from dotenv import load_dotenv
import warnings
from pathlib import Path


env_path = Path(__file__).parent / '.env'
load_dotenv(str(env_path.resolve()))