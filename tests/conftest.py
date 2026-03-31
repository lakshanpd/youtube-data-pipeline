"""Pytest shared setup for test imports."""
from pathlib import Path
import sys

# Ensure project root is importable so `src.*` imports work in all test runs.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
