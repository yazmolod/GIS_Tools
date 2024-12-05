from pathlib import Path

from dotenv import load_dotenv

env_path = Path(__file__).parent / ".env"
load_dotenv(str(env_path.resolve()))
