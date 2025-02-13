import sys
print(f"Python path: {sys.path}")
print(f"Python version: {sys.version}")

try:
    import pandas as pd
    print(f"Pandas version: {pd.__version__}")
except ImportError as e:
    print(f"Import error: {e}")
    print(f"Detailed error: {str(e.__class__.__name__)}: {str(e)}")