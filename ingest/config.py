"""
Ingestion Pipeline Configuration
Defines success criteria, constraints, and system parameters
"""

from dataclasses import dataclass, field
from typing import List
from enum import Enum


class ProcessingMode(Enum):
    """Processing modes for different use cases"""
    MVP = "mvp"  # Fast, basic processing
    STANDARD = "standard"  # Balanced accuracy/speed
    FORENSIC = "forensic"  # Maximum accuracy, full audit trail
    

class ConfidenceThreshold(Enum):
    """Confidence level thresholds for automated processing"""
    HIGH = 0.9  # Auto-accept
    MEDIUM = 0.7  # Review recommended
    LOW = 0.5  # Manual review required
    REJECT = 0.3  # Below this, reject


@dataclass
class SuccessCriteria:
    """Define success metrics for the ingestion pipeline"""
    # Precision/Recall targets
    min_precision_high_conf: float = 0.95  # At high confidence threshold
    min_recall_low_conf: float = 0.85  # At low confidence threshold
    max_false_positive_rate: float = 0.05  # 5% max false positives
    
    # Throughput targets
    target_throughput_mb_per_minute: float = 50.0  # Processing speed
    max_latency_seconds: float = 300.0  # Max time for single file
    
    # Quality targets
    min_field_extraction_rate: float = 0.90  # 90% fields extracted
    min_name_detection_accuracy: float = 0.85  # Name detection accuracy
    
    # Review targets
    max_human_review_rate: float = 0.15  # Max 15% items need review


@dataclass
class LegalConstraints:
    """Legal and privacy constraints"""
    jurisdiction: str = "IN"  # India
    data_retention_days: int = 2555  # 7 years
    requires_consent: bool = False  # Law enforcement use
    pii_redaction_required: bool = False  # Keep full data for forensics
    chain_of_custody_required: bool = True  # Mandatory for legal evidence
    audit_log_retention_days: int = 3650  # 10 years
    encryption_required: bool = True  # At rest and in transit


@dataclass
class ProcessingConfig:
    """Configuration for processing pipeline"""
    mode: ProcessingMode = ProcessingMode.STANDARD
    
    # Batch processing
    batch_size: int = 5000  # Records per batch
    max_workers: int = 4  # Parallel workers
    checkpoint_interval: int = 1000  # Save checkpoint every N records
    
    # LLM configuration
    use_cloud_llm: bool = True  # Use cloud LLM for complex analysis
    cloud_llm_model: str = "deepseek-r1:671b"
    local_llm_model: str = "llama3.1:8b"
    llm_timeout_seconds: int = 60
    llm_max_retries: int = 3
    
    # OCR/STT configuration
    ocr_engine: str = "tesseract"  # tesseract, paddle, easyocr
    ocr_languages: List[str] = field(default_factory=lambda: ["eng", "hin", "tel", "tam", "ben", "mar"])
    stt_engine: str = "whisper"  # whisper, google, azure
    stt_languages: List[str] = field(default_factory=lambda: ["en", "hi", "te", "ta", "bn", "mr"])
    
    # Name detection configuration
    enable_multi_signal_detection: bool = True
    min_detection_confidence: float = ConfidenceThreshold.MEDIUM.value
    enable_llm_fallback: bool = True
    
    # Schema inference
    schema_sample_size: int = 1000  # Records to sample for schema inference
    schema_version: str = "1.0.0"
    allow_schema_evolution: bool = True
    
    # Deduplication
    enable_deduplication: bool = True
    fuzzy_match_threshold: float = 0.85
    clustering_algorithm: str = "dbscan"  # dbscan, hierarchical
    
    # Storage
    database_type: str = "sqlite"  # sqlite, postgresql
    enable_vector_storage: bool = True
    vector_dimensions: int = 384  # For embeddings
    
    # Monitoring
    enable_metrics: bool = True
    enable_drift_detection: bool = True
    metrics_export_interval: int = 60  # seconds


@dataclass
class FileTypeConfig:
    """Configuration for different file types"""
    # Structured formats
    supported_structured: List[str] = field(default_factory=lambda: [
        ".json", ".xml", ".csv", ".jsonl", ".tsv", ".yaml", ".yml"
    ])
    
    # Plain text formats
    supported_text: List[str] = field(default_factory=lambda: [
        ".txt", ".log", ".md", ".html", ".htm"
    ])
    
    # Image formats for OCR
    supported_images: List[str] = field(default_factory=lambda: [
        ".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".webp", ".heic"
    ])
    
    # PDF formats
    supported_pdf: List[str] = field(default_factory=lambda: [".pdf"])
    
    # Audio formats for STT
    supported_audio: List[str] = field(default_factory=lambda: [
        ".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".opus", ".amr"
    ])
    
    # Video formats for STT
    supported_video: List[str] = field(default_factory=lambda: [
        ".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm", ".3gp"
    ])
    
    # Archive formats
    supported_archives: List[str] = field(default_factory=lambda: [
        ".zip", ".ufdr", ".tar", ".tar.gz", ".tgz", ".rar", ".7z"
    ])
    
    # Binary/blob formats
    supported_binary: List[str] = field(default_factory=lambda: [
        ".db", ".sqlite", ".bin", ".dat"
    ])
    
    # Maximum file sizes (MB)
    max_image_size_mb: float = 50.0
    max_audio_size_mb: float = 500.0
    max_video_size_mb: float = 2000.0
    max_archive_size_mb: float = 5000.0


@dataclass
class SecurityConfig:
    """Security and compliance configuration"""
    # Encryption
    encryption_algorithm: str = "AES-256-GCM"
    key_derivation: str = "PBKDF2"
    
    # Access control
    enable_rbac: bool = True
    require_2fa: bool = False  # For analyst access
    
    # Audit logging
    log_all_access: bool = True
    log_all_queries: bool = True
    log_all_modifications: bool = True
    
    # Redaction (for data sharing)
    redaction_patterns: List[str] = field(default_factory=lambda: [
        r'\b\d{12}\b',  # Aadhaar numbers
        r'\b\d{10}\b',  # PAN numbers  
        r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',  # Credit cards
    ])
    
    # Hash algorithms for integrity
    integrity_hash: str = "SHA256"
    enable_perceptual_hash: bool = True  # For media deduplication


# Default configuration instance
DEFAULT_CONFIG = ProcessingConfig()
DEFAULT_SUCCESS_CRITERIA = SuccessCriteria()
DEFAULT_LEGAL_CONSTRAINTS = LegalConstraints()
DEFAULT_FILE_TYPE_CONFIG = FileTypeConfig()
DEFAULT_SECURITY_CONFIG = SecurityConfig()


# Color codes for logging
class LogColors:
    """ANSI color codes for terminal logging"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def get_config(mode: ProcessingMode = ProcessingMode.STANDARD) -> ProcessingConfig:
    """
    Get configuration for specific processing mode
    
    Args:
        mode: Processing mode (MVP, STANDARD, FORENSIC)
        
    Returns:
        ProcessingConfig instance
    """
    config = ProcessingConfig(mode=mode)
    
    if mode == ProcessingMode.MVP:
        # Fast, basic processing
        config.batch_size = 10000
        config.use_cloud_llm = False
        config.enable_multi_signal_detection = False
        config.enable_deduplication = False
        config.enable_metrics = False
        
    elif mode == ProcessingMode.FORENSIC:
        # Maximum accuracy, full audit trail
        config.batch_size = 1000
        config.use_cloud_llm = True
        config.enable_multi_signal_detection = True
        config.enable_deduplication = True
        config.enable_metrics = True
        config.checkpoint_interval = 500
        config.llm_max_retries = 5
        
    return config


def validate_config(config: ProcessingConfig) -> List[str]:
    """
    Validate configuration and return list of warnings/errors
    
    Args:
        config: Configuration to validate
        
    Returns:
        List of validation messages
    """
    warnings = []
    
    if config.batch_size > 20000:
        warnings.append("⚠️  Large batch size may cause memory issues")
        
    if config.max_workers > 8:
        warnings.append("⚠️  High worker count may cause resource contention")
        
    if config.use_cloud_llm and not config.local_llm_model:
        warnings.append("⚠️  No fallback LLM configured for cloud failures")
        
    if config.enable_deduplication and config.fuzzy_match_threshold < 0.7:
        warnings.append("⚠️  Low fuzzy match threshold may cause over-merging")
        
    if not config.enable_metrics and config.mode == ProcessingMode.FORENSIC:
        warnings.append("⚠️  Metrics disabled in forensic mode - audit trail incomplete")
        
    return warnings


if __name__ == "__main__":
    # Test configuration
    print("🔧 Ingestion Pipeline Configuration\n")
    
    for mode in ProcessingMode:
        config = get_config(mode)
        print(f"\n{mode.value.upper()} Mode:")
        print(f"  Batch size: {config.batch_size}")
        print(f"  Cloud LLM: {config.use_cloud_llm}")
        print(f"  Multi-signal detection: {config.enable_multi_signal_detection}")
        print(f"  Deduplication: {config.enable_deduplication}")
        
        warnings = validate_config(config)
        if warnings:
            print(f"  Warnings: {len(warnings)}")
            for w in warnings:
                print(f"    {w}")
