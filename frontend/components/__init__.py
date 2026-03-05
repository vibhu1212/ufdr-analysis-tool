"""
UFDR Frontend Components Package

Contains reusable UI components for the UFDR Analysis Tool frontend.
"""

__version__ = "2.0.0"

# Export main component functions for easier imports
try:
    from .ufdr_upload_component import render_ufdr_upload
except ImportError:
    render_ufdr_upload = None

render_file_browser = None

__all__ = [
    'render_ufdr_upload',
    'render_file_browser'
]
