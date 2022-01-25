import os
from dotenv import load_dotenv
from pathlib import Path
env_path = Path(__file__).parent / '.env'
load_dotenv(str(env_path.resolve()))

HERE_API_KEY = os.environ['HERE_API_KEY']
YANDEX_API_KEY = os.environ['YANDEX_API_KEY']