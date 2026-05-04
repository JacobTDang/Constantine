import sys
from pathlib import Path

# Ensure python-ml/ is on the path so all package imports resolve
# regardless of which directory pytest is invoked from.
sys.path.insert(0, str(Path(__file__).parent))
