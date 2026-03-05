"""
Format-Agnostic File Ingestion and Unpacking
Step 1: Accept any input (folder, archive, single file) and generate manifest
"""

import os
import zipfile
import tarfile
import hashlib
import shutil
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, asdict
import logging

try:
    import magic  # python-magic for MIME detection
    HAS_MAGIC = True
except ImportError:
    HAS_MAGIC = False

from .config import FileTypeConfig, DEFAULT_FILE_TYPE_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class FileManifestEntry:
    """Single file entry in the manifest with full provenance"""
    file_id: str  # UUID for this file
    original_path: str  # Path in original archive/folder
    extracted_path: str  # Path in extraction workspace
    filename: str  # Original filename
    file_size: int  # Size in bytes
    mime_type: str  # MIME type detected
    extension: str  # File extension
    sha256_hash: str  # SHA256 integrity hash
    byte_offset: int  # Offset in archive (0 for non-archived files)
    timestamp: str  # ISO timestamp of ingestion
    parent_archive: Optional[str] = None  # If extracted from archive
    metadata: Dict[str, Any] = None  # Additional metadata
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class IngestionManifest:
    """Complete manifest for an ingestion job"""
    ingestion_id: str  # Unique ID for this ingestion
    source_path: str  # Original input path
    source_type: str  # folder, archive, file
    total_files: int  # Total files in manifest
    total_size_bytes: int  # Total size of all files
    created_at: str  # ISO timestamp
    workspace_path: str  # Path to extraction workspace
    files: List[FileManifestEntry]  # All file entries
    metadata: Dict[str, Any]  # Additional metadata
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "ingestion_id": self.ingestion_id,
            "source_path": self.source_path,
            "source_type": self.source_type,
            "total_files": self.total_files,
            "total_size_bytes": self.total_size_bytes,
            "created_at": self.created_at,
            "workspace_path": self.workspace_path,
            "files": [f.to_dict() for f in self.files],
            "metadata": self.metadata
        }
    
    def save(self, output_path: str):
        """Save manifest to JSON file"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
        logger.info(f"✅ Manifest saved to {output_path}")


class FileIngestor:
    """
    Format-agnostic file ingestion with unpacking and manifest generation
    Handles: folders, archives (.zip, .ufdr, .tar, .tar.gz, .7z), single files
    """
    
    def __init__(self, 
                 workspace_root: str = "data/ingestion_workspace",
                 config: FileTypeConfig = None):
        """
        Initialize the file ingestor
        
        Args:
            workspace_root: Root directory for extraction workspaces
            config: File type configuration
        """
        self.workspace_root = Path(workspace_root)
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        self.config = config or DEFAULT_FILE_TYPE_CONFIG
        
        # Initialize python-magic for MIME detection
        if HAS_MAGIC:
            try:
                self.mime_detector = magic.Magic(mime=True)
            except Exception as e:
                logger.warning(f"⚠️  python-magic not available: {e}")
                self.mime_detector = None
        else:
            logger.info("python-magic not installed, using fallback MIME detection")
            self.mime_detector = None
    
    def _generate_id(self) -> str:
        """Generate unique ID for ingestion job"""
        import uuid
        return str(uuid.uuid4())
    
    def _calculate_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def _detect_mime_type(self, file_path: Path) -> str:
        """
        Detect MIME type using python-magic (reliable)
        Falls back to extension-based detection
        """
        try:
            if self.mime_detector:
                mime = self.mime_detector.from_file(str(file_path))
                return mime
        except Exception as e:
            logger.debug(f"MIME detection failed for {file_path}: {e}")
        
        # Fallback: extension-based MIME type
        extension = file_path.suffix.lower()
        mime_map = {
            '.json': 'application/json',
            '.xml': 'application/xml',
            '.csv': 'text/csv',
            '.txt': 'text/plain',
            '.pdf': 'application/pdf',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.mp4': 'video/mp4',
            '.mp3': 'audio/mpeg',
            '.zip': 'application/zip',
            '.ufdr': 'application/zip',  # UFDR is ZIP-based
        }
        return mime_map.get(extension, 'application/octet-stream')
    
    def _is_archive(self, file_path: Path) -> bool:
        """Check if file is an archive"""
        extension = file_path.suffix.lower()
        return extension in self.config.supported_archives
    
    def _extract_zip(self, archive_path: Path, extract_to: Path) -> List[Path]:
        """Extract ZIP/UFDR archive"""
        extracted_files = []
        with zipfile.ZipFile(archive_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            for member in zip_ref.namelist():
                extracted_path = extract_to / member
                if extracted_path.is_file():
                    extracted_files.append(extracted_path)
        return extracted_files
    
    def _extract_tar(self, archive_path: Path, extract_to: Path) -> List[Path]:
        """Extract TAR/TAR.GZ archive"""
        extracted_files = []
        with tarfile.open(archive_path, 'r:*') as tar_ref:
            tar_ref.extractall(extract_to)
            for member in tar_ref.getmembers():
                if member.isfile():
                    extracted_path = extract_to / member.name
                    extracted_files.append(extracted_path)
        return extracted_files
    
    def _extract_archive(self, archive_path: Path, extract_to: Path) -> List[Path]:
        """
        Extract archive based on type
        Returns list of extracted file paths
        """
        extension = archive_path.suffix.lower()
        
        logger.info(f"📦 Extracting archive: {archive_path.name}")
        
        if extension in ['.zip', '.ufdr']:
            return self._extract_zip(archive_path, extract_to)
        elif extension in ['.tar', '.tar.gz', '.tgz']:
            return self._extract_tar(archive_path, extract_to)
        elif extension == '.7z':
            # Requires py7zr library
            try:
                import py7zr
                extracted_files = []
                with py7zr.SevenZipFile(archive_path, 'r') as archive:
                    archive.extractall(extract_to)
                    for name in archive.getnames():
                        extracted_path = extract_to / name
                        if extracted_path.is_file():
                            extracted_files.append(extracted_path)
                return extracted_files
            except ImportError:
                logger.error("❌ py7zr not installed - cannot extract .7z archives")
                return []
        else:
            logger.warning(f"⚠️  Unsupported archive format: {extension}")
            return []
    
    def _create_manifest_entry(self, 
                              file_path: Path, 
                              original_path: str,
                              extracted_path: str,
                              parent_archive: Optional[str] = None,
                              byte_offset: int = 0) -> FileManifestEntry:
        """Create manifest entry for a single file"""
        file_id = self._generate_id()
        file_size = file_path.stat().st_size
        mime_type = self._detect_mime_type(file_path)
        sha256_hash = self._calculate_hash(file_path)
        
        return FileManifestEntry(
            file_id=file_id,
            original_path=original_path,
            extracted_path=extracted_path,
            filename=file_path.name,
            file_size=file_size,
            mime_type=mime_type,
            extension=file_path.suffix.lower(),
            sha256_hash=sha256_hash,
            byte_offset=byte_offset,
            timestamp=datetime.utcnow().isoformat() + 'Z',
            parent_archive=parent_archive,
            metadata={}
        )
    
    def ingest_file(self, file_path: Union[str, Path]) -> IngestionManifest:
        """
        Ingest a single file
        
        Args:
            file_path: Path to file
            
        Returns:
            IngestionManifest with single file entry
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        ingestion_id = self._generate_id()
        workspace = self.workspace_root / ingestion_id
        workspace.mkdir(parents=True, exist_ok=True)
        
        # Copy file to workspace
        dest_path = workspace / file_path.name
        shutil.copy2(file_path, dest_path)
        
        logger.info(f"📄 Ingesting single file: {file_path.name}")
        
        # Create manifest entry
        entry = self._create_manifest_entry(
            dest_path,
            original_path=str(file_path),
            extracted_path=str(dest_path)
        )
        
        manifest = IngestionManifest(
            ingestion_id=ingestion_id,
            source_path=str(file_path),
            source_type="file",
            total_files=1,
            total_size_bytes=entry.file_size,
            created_at=datetime.utcnow().isoformat() + 'Z',
            workspace_path=str(workspace),
            files=[entry],
            metadata={"source_type": "single_file"}
        )
        
        # Save manifest
        manifest.save(str(workspace / "manifest.json"))
        
        logger.info(f"✅ File ingestion complete: {entry.filename} ({entry.file_size} bytes)")
        return manifest
    
    def ingest_folder(self, folder_path: Union[str, Path]) -> IngestionManifest:
        """
        Ingest all files from a folder (recursive)
        
        Args:
            folder_path: Path to folder
            
        Returns:
            IngestionManifest with all file entries
        """
        folder_path = Path(folder_path)
        
        if not folder_path.is_dir():
            raise NotADirectoryError(f"Not a directory: {folder_path}")
        
        ingestion_id = self._generate_id()
        workspace = self.workspace_root / ingestion_id
        workspace.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📁 Ingesting folder: {folder_path}")
        
        entries = []
        total_size = 0
        
        # Walk through all files recursively
        for root, dirs, files in os.walk(folder_path):
            for filename in files:
                source_file = Path(root) / filename
                
                # Create relative path structure in workspace
                rel_path = source_file.relative_to(folder_path)
                dest_file = workspace / rel_path
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Copy file
                shutil.copy2(source_file, dest_file)
                
                # Create manifest entry
                entry = self._create_manifest_entry(
                    dest_file,
                    original_path=str(source_file),
                    extracted_path=str(dest_file)
                )
                entries.append(entry)
                total_size += entry.file_size
                
                logger.debug(f"  ✓ {filename} ({entry.file_size} bytes)")
        
        manifest = IngestionManifest(
            ingestion_id=ingestion_id,
            source_path=str(folder_path),
            source_type="folder",
            total_files=len(entries),
            total_size_bytes=total_size,
            created_at=datetime.utcnow().isoformat() + 'Z',
            workspace_path=str(workspace),
            files=entries,
            metadata={"source_type": "folder", "recursive": True}
        )
        
        # Save manifest
        manifest.save(str(workspace / "manifest.json"))
        
        logger.info(f"✅ Folder ingestion complete: {len(entries)} files ({total_size} bytes)")
        return manifest
    
    def ingest_archive(self, archive_path: Union[str, Path]) -> IngestionManifest:
        """
        Ingest archive (ZIP, UFDR, TAR, 7Z) with unpacking
        
        Args:
            archive_path: Path to archive file
            
        Returns:
            IngestionManifest with all extracted file entries
        """
        archive_path = Path(archive_path)
        
        if not archive_path.exists():
            raise FileNotFoundError(f"Archive not found: {archive_path}")
        
        if not self._is_archive(archive_path):
            raise ValueError(f"Not a supported archive: {archive_path.suffix}")
        
        ingestion_id = self._generate_id()
        workspace = self.workspace_root / ingestion_id
        workspace.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📦 Ingesting archive: {archive_path.name}")
        
        # Extract archive
        extracted_files = self._extract_archive(archive_path, workspace)
        
        entries = []
        total_size = 0
        
        # Create manifest entries for all extracted files
        for extracted_file in extracted_files:
            rel_path = extracted_file.relative_to(workspace)
            
            entry = self._create_manifest_entry(
                extracted_file,
                original_path=str(rel_path),  # Path within archive
                extracted_path=str(extracted_file),
                parent_archive=archive_path.name
            )
            entries.append(entry)
            total_size += entry.file_size
            
            logger.debug(f"  ✓ {rel_path} ({entry.file_size} bytes)")
        
        manifest = IngestionManifest(
            ingestion_id=ingestion_id,
            source_path=str(archive_path),
            source_type="archive",
            total_files=len(entries),
            total_size_bytes=total_size,
            created_at=datetime.utcnow().isoformat() + 'Z',
            workspace_path=str(workspace),
            files=entries,
            metadata={
                "source_type": "archive",
                "archive_type": archive_path.suffix,
                "archive_hash": self._calculate_hash(archive_path)
            }
        )
        
        # Save manifest
        manifest.save(str(workspace / "manifest.json"))
        
        logger.info(f"✅ Archive ingestion complete: {len(entries)} files ({total_size} bytes)")
        return manifest
    
    def ingest(self, input_path: Union[str, Path]) -> IngestionManifest:
        """
        Universal ingestion - automatically detect type and ingest
        
        Args:
            input_path: Path to file, folder, or archive
            
        Returns:
            IngestionManifest
        """
        input_path = Path(input_path)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input not found: {input_path}")
        
        if input_path.is_file():
            if self._is_archive(input_path):
                return self.ingest_archive(input_path)
            else:
                return self.ingest_file(input_path)
        elif input_path.is_dir():
            return self.ingest_folder(input_path)
        else:
            raise ValueError(f"Unsupported input type: {input_path}")


def load_manifest(manifest_path: Union[str, Path]) -> IngestionManifest:
    """
    Load manifest from JSON file
    
    Args:
        manifest_path: Path to manifest.json
        
    Returns:
        IngestionManifest object
    """
    with open(manifest_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    files = [FileManifestEntry(**entry) for entry in data['files']]
    
    return IngestionManifest(
        ingestion_id=data['ingestion_id'],
        source_path=data['source_path'],
        source_type=data['source_type'],
        total_files=data['total_files'],
        total_size_bytes=data['total_size_bytes'],
        created_at=data['created_at'],
        workspace_path=data['workspace_path'],
        files=files,
        metadata=data['metadata']
    )


if __name__ == "__main__":
    # Test ingestion
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python file_ingestor.py <path_to_file_or_folder_or_archive>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    
    ingestor = FileIngestor()
    manifest = ingestor.ingest(input_path)
    
    print("\n" + "="*60)
    print(f"🎯 Ingestion Complete")
    print("="*60)
    print(f"Ingestion ID: {manifest.ingestion_id}")
    print(f"Source: {manifest.source_path}")
    print(f"Type: {manifest.source_type}")
    print(f"Total Files: {manifest.total_files}")
    print(f"Total Size: {manifest.total_size_bytes:,} bytes")
    print(f"Workspace: {manifest.workspace_path}")
    print(f"\nManifest saved to: {manifest.workspace_path}/manifest.json")
    
    # Show first 10 files
    print(f"\nFirst {min(10, len(manifest.files))} files:")
    for i, entry in enumerate(manifest.files[:10], 1):
        print(f"  {i}. {entry.filename} ({entry.file_size:,} bytes) - {entry.mime_type}")
