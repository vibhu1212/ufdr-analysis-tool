"""
UFDR Format-Agnostic Ingestion Pipeline
Version: 0.1.0

A comprehensive forensic data ingestion system with:
- Format-agnostic file ingestion
- Multi-path text extraction
- Dynamic schema inference
- Multi-locale support (Hindi, Telugu, Tamil, etc.)
- Full provenance tracking
- Human-in-the-loop correction
"""

__version__ = "0.1.0"
__author__ = "UFDR Analysis Tool Team"

# Import main components
from .config import (
    ProcessingMode,
    ConfidenceThreshold,
    ProcessingConfig,
    SuccessCriteria,
    LegalConstraints,
    FileTypeConfig,
    SecurityConfig,
    get_config,
    validate_config,
)

from .file_ingestor import (
    FileIngestor,
    FileManifestEntry,
    IngestionManifest,
    load_manifest,
)

from .file_type_detector import (
    FileTypeDetector,
    FileTypeInfo,
    FileCategory,
    UnicodeScript,
    TextNormalizer,
    get_language_from_script,
)

__all__ = [
    # Version
    "__version__",
    "__author__",
    
    # Configuration
    "ProcessingMode",
    "ConfidenceThreshold",
    "ProcessingConfig",
    "SuccessCriteria",
    "LegalConstraints",
    "FileTypeConfig",
    "SecurityConfig",
    "get_config",
    "validate_config",
    
    # File Ingestion
    "FileIngestor",
    "FileManifestEntry",
    "IngestionManifest",
    "load_manifest",
    
    # File Type Detection
    "FileTypeDetector",
    "FileTypeInfo",
    "FileCategory",
    "UnicodeScript",
    "TextNormalizer",
    "get_language_from_script",
]
