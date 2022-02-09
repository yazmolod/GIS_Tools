import os
from dotenv import load_dotenv
import warnings
from pathlib import Path
env_path = Path(__file__).parent / '.env'
load_dotenv(str(env_path.resolve()))

try:
	HERE_API_KEY = os.environ['HERE_API_KEY']
except KeyError:
    warnings.warn("Can't find HERE api key in env", UserWarning, stacklevel=2)
try:
	YANDEX_API_KEY = os.environ['YANDEX_API_KEY']
except KeyError:
	warnings.warn("Can't find YANDEX api key in env", UserWarning, stacklevel=2)