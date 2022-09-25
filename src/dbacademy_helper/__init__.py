try: import dbacademy_gems.dbgems
except ImportError: raise Exception("The runtime dependency dbgems was not found. Please install https://github.com/databricks-academy/dbacademy-gems")

try: import dbacademy.dbrest
except ImportError: raise Exception("The runtime dependency dbrest was not found. Please install https://github.com/databricks-academy/dbacademy-rest")

from .dbacademy_helper_class import DBAcademyHelper
from .paths_class import Paths
