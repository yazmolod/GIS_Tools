import os
import sys
from dotenv import load_dotenv
import warnings
from pathlib import Path
env_path = Path(__file__).parent / '.env'
load_dotenv(str(env_path.resolve()))

__env_keys = [
	'HERE_API_KEY',
	'YANDEX_API_KEY',
	'HIDEMY_NAME_API_CODE'
]

thismodule = sys.modules[__name__]
for k in __env_keys:
	v = os.environ.get(k)
	setattr(thismodule, k, v)
	if not v:
	    warnings.warn(f"Can not find {k} in env", UserWarning, stacklevel=2)