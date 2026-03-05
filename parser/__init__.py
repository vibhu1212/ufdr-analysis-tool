"""
UFDR Parser Package
Handles extraction and parsing of UFDR forensic files
"""

from .ufdr_unzip import UFDRExtractor, ExtractionManifest
from .ufdr_parser import (
    UFDRStreamParser,
    ArtifactType,
    Message,
    Call,
    Contact,
    PhoneNumberNormalizer,
    TimestampNormalizer
)
from .ingest_cli import UFDRIngestor

__version__ = "1.0.0"
__all__ = [
    "UFDRExtractor",
    "UFDRStreamParser",
    "UFDRIngestor",
    "ExtractionManifest",
    "ArtifactType",
    "Message",
    "Call",
    "Contact",
    "PhoneNumberNormalizer",
    "TimestampNormalizer"
]