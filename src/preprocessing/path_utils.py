"""
Centralized path utilities for preprocessing scripts.
"""
import os

def get_core_dir():
    """Get the core project directory, either from environment or by deriving from file location."""
    return os.environ.get('JOURNALING_CORE_DIR') or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def setup_paths():
    """Setup paths and add src to Python path."""
    import sys
    core_dir = get_core_dir()
    src_dir = os.path.join(core_dir, 'src')
    if src_dir not in sys.path:
        sys.path.append(src_dir)
    return core_dir



