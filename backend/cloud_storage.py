"""
Cloud Storage Integration
Supports Azure Blob Storage and AWS S3 for scalable storage
While keeping all AI/ML processing local for security
"""

import os
import json
import hashlib
import logging
from pathlib import Path
from typing import Optional, Dict, List, BinaryIO
from dataclasses import dataclass
from datetime import datetime
import boto3
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError
from botocore.exceptions import NoCredentialsError

logger = logging.getLogger(__name__)


@dataclass
class StorageConfig:
    """Configuration for cloud storage"""
    provider: str  # 'azure' or 's3'
    container_name: str  # Azure container or S3 bucket
    connection_string: Optional[str] = None  # For Azure
    access_key: Optional[str] = None  # For S3
    secret_key: Optional[str] = None  # For S3
    region: str = 'us-east-1'  # For S3
    encryption_enabled: bool = True
    use_local_cache: bool = True
    cache_dir: str = "data/cache"


class CloudStorageManager:
    """Unified cloud storage manager for Azure and S3"""
    
    def __init__(self, config: StorageConfig):
        self.config = config
        self.provider = config.provider.lower()
        
        # Initialize cache directory
        if config.use_local_cache:
            self.cache_dir = Path(config.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize storage client
        if self.provider == 'azure':
            self._init_azure()
        elif self.provider == 's3':
            self._init_s3()
        else:
            # Fallback to local storage
            self.provider = 'local'
            self._init_local()
    
    def _init_azure(self):
        """Initialize Azure Blob Storage client"""
        if not self.config.connection_string:
            raise ValueError("Azure connection string required")
        
        self.blob_service = BlobServiceClient.from_connection_string(
            self.config.connection_string
        )
        self.container = self.blob_service.get_container_client(
            self.config.container_name
        )
        
        # Create container if it doesn't exist
        try:
            self.container.get_container_properties()
        except ResourceNotFoundError:
            self.container.create_container()
            logger.info(f"Created Azure container: {self.config.container_name}")
    
    def _init_s3(self):
        """Initialize AWS S3 client"""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            region_name=self.config.region
        )
        
        # Create bucket if it doesn't exist
        try:
            self.s3_client.head_bucket(Bucket=self.config.container_name)
        except:
            self.s3_client.create_bucket(
                Bucket=self.config.container_name,
                CreateBucketConfiguration={'LocationConstraint': self.config.region}
                if self.config.region != 'us-east-1' else {}
            )
            logger.info(f"Created S3 bucket: {self.config.container_name}")
    
    def _init_local(self):
        """Initialize local file storage as fallback"""
        self.local_dir = Path("data/storage")
        self.local_dir.mkdir(parents=True, exist_ok=True)
        logger.warning("Using local storage - no cloud provider configured")
    
    def upload_file(self, 
                   local_path: str, 
                   remote_path: str,
                   metadata: Optional[Dict] = None) -> Dict:
        """
        Upload file to cloud storage
        
        Args:
            local_path: Local file path
            remote_path: Remote path in cloud storage
            metadata: Optional metadata to attach
            
        Returns:
            Upload confirmation with details
        """
        local_file = Path(local_path)
        
        if not local_file.exists():
            raise FileNotFoundError(f"File not found: {local_path}")
        
        # Calculate file hash
        file_hash = self._calculate_file_hash(local_file)
        
        # Prepare metadata
        if metadata is None:
            metadata = {}
        
        metadata.update({
            'original_name': local_file.name,
            'upload_time': datetime.utcnow().isoformat(),
            'file_size': str(local_file.stat().st_size),
            'sha256': file_hash
        })
        
        # Upload based on provider
        if self.provider == 'azure':
            return self._upload_azure(local_file, remote_path, metadata)
        elif self.provider == 's3':
            return self._upload_s3(local_file, remote_path, metadata)
        else:
            return self._upload_local(local_file, remote_path, metadata)
    
    def _upload_azure(self, local_file: Path, remote_path: str, metadata: Dict) -> Dict:
        """Upload to Azure Blob Storage"""
        blob_client = self.container.get_blob_client(remote_path)
        
        with open(local_file, 'rb') as data:
            blob_client.upload_blob(
                data,
                overwrite=True,
                metadata=metadata
            )
        
        return {
            'status': 'success',
            'provider': 'azure',
            'remote_path': remote_path,
            'url': blob_client.url,
            'metadata': metadata
        }
    
    def _upload_s3(self, local_file: Path, remote_path: str, metadata: Dict) -> Dict:
        """Upload to AWS S3"""
        try:
            # Convert metadata to S3 format
            s3_metadata = {f'x-amz-meta-{k}': v for k, v in metadata.items()}
            
            self.s3_client.upload_file(
                str(local_file),
                self.config.container_name,
                remote_path,
                ExtraArgs={
                    'Metadata': metadata,
                    'ServerSideEncryption': 'AES256' if self.config.encryption_enabled else None
                }
            )
            
            url = f"s3://{self.config.container_name}/{remote_path}"
            
            return {
                'status': 'success',
                'provider': 's3',
                'remote_path': remote_path,
                'url': url,
                'metadata': metadata
            }
            
        except NoCredentialsError:
            raise ValueError("AWS credentials not configured")
    
    def _upload_local(self, local_file: Path, remote_path: str, metadata: Dict) -> Dict:
        """Upload to local storage"""
        dest_path = self.local_dir / remote_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy file
        import shutil
        shutil.copy2(local_file, dest_path)
        
        # Save metadata
        meta_path = dest_path.with_suffix('.meta.json')
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return {
            'status': 'success',
            'provider': 'local',
            'remote_path': str(dest_path),
            'url': f"file://{dest_path.absolute()}",
            'metadata': metadata
        }
    
    def download_file(self, 
                     remote_path: str,
                     local_path: Optional[str] = None) -> str:
        """
        Download file from cloud storage
        
        Args:
            remote_path: Remote path in cloud storage
            local_path: Optional local destination path
            
        Returns:
            Local path of downloaded file
        """
        # Use cache if enabled
        if self.config.use_local_cache:
            cache_path = self.cache_dir / remote_path
            if cache_path.exists():
                logger.info(f"Using cached file: {cache_path}")
                return str(cache_path)
        
        # Determine local path
        if local_path is None:
            local_path = self.cache_dir / remote_path if self.config.use_local_cache else remote_path
        
        local_file = Path(local_path)
        local_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Download based on provider
        if self.provider == 'azure':
            self._download_azure(remote_path, local_file)
        elif self.provider == 's3':
            self._download_s3(remote_path, local_file)
        else:
            self._download_local(remote_path, local_file)
        
        return str(local_file)
    
    def _download_azure(self, remote_path: str, local_file: Path):
        """Download from Azure Blob Storage"""
        blob_client = self.container.get_blob_client(remote_path)
        
        with open(local_file, 'wb') as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())
    
    def _download_s3(self, remote_path: str, local_file: Path):
        """Download from AWS S3"""
        self.s3_client.download_file(
            self.config.container_name,
            remote_path,
            str(local_file)
        )
    
    def _download_local(self, remote_path: str, local_file: Path):
        """Download from local storage"""
        src_path = self.local_dir / remote_path
        if not src_path.exists():
            raise FileNotFoundError(f"File not found: {src_path}")
        
        import shutil
        shutil.copy2(src_path, local_file)
    
    def list_files(self, prefix: str = "") -> List[Dict]:
        """List files in cloud storage"""
        if self.provider == 'azure':
            return self._list_azure(prefix)
        elif self.provider == 's3':
            return self._list_s3(prefix)
        else:
            return self._list_local(prefix)
    
    def _list_azure(self, prefix: str) -> List[Dict]:
        """List files in Azure Blob Storage"""
        blobs = self.container.list_blobs(name_starts_with=prefix)
        
        files = []
        for blob in blobs:
            files.append({
                'name': blob.name,
                'size': blob.size,
                'last_modified': blob.last_modified.isoformat(),
                'metadata': blob.metadata
            })
        
        return files
    
    def _list_s3(self, prefix: str) -> List[Dict]:
        """List files in AWS S3"""
        response = self.s3_client.list_objects_v2(
            Bucket=self.config.container_name,
            Prefix=prefix
        )
        
        files = []
        for obj in response.get('Contents', []):
            files.append({
                'name': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat()
            })
        
        return files
    
    def _list_local(self, prefix: str) -> List[Dict]:
        """List files in local storage"""
        search_path = self.local_dir / prefix
        
        files = []
        for file_path in search_path.rglob('*'):
            if file_path.is_file() and not file_path.name.endswith('.meta.json'):
                files.append({
                    'name': str(file_path.relative_to(self.local_dir)),
                    'size': file_path.stat().st_size,
                    'last_modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                })
        
        return files
    
    def delete_file(self, remote_path: str) -> bool:
        """Delete file from cloud storage"""
        if self.provider == 'azure':
            blob_client = self.container.get_blob_client(remote_path)
            blob_client.delete_blob()
        elif self.provider == 's3':
            self.s3_client.delete_object(
                Bucket=self.config.container_name,
                Key=remote_path
            )
        else:
            local_file = self.local_dir / remote_path
            if local_file.exists():
                local_file.unlink()
        
        # Clear cache if exists
        if self.config.use_local_cache:
            cache_file = self.cache_dir / remote_path
            if cache_file.exists():
                cache_file.unlink()
        
        return True
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file"""
        sha256_hash = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            for byte_block in iter(lambda: f.read(65536), b""):
                sha256_hash.update(byte_block)
        
        return sha256_hash.hexdigest()
    
    def get_signed_url(self, remote_path: str, expiry_hours: int = 24) -> str:
        """Generate signed URL for secure access"""
        if self.provider == 'azure':
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            from datetime import timedelta
            
            sas_token = generate_blob_sas(
                account_name=self.blob_service.account_name,
                container_name=self.config.container_name,
                blob_name=remote_path,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
            )
            
            blob_client = self.container.get_blob_client(remote_path)
            return f"{blob_client.url}?{sas_token}"
            
        elif self.provider == 's3':
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.config.container_name,
                    'Key': remote_path
                },
                ExpiresIn=expiry_hours * 3600
            )
            return url
        
        else:
            # Local file URL
            local_file = self.local_dir / remote_path
            return f"file://{local_file.absolute()}"


# Factory function to create storage manager
def create_storage_manager(provider: str = None) -> CloudStorageManager:
    """
    Create storage manager based on environment variables or config
    """
    if provider is None:
        provider = os.getenv('STORAGE_PROVIDER', 'local')
    
    if provider == 'azure':
        config = StorageConfig(
            provider='azure',
            container_name=os.getenv('AZURE_CONTAINER', 'ufdr-storage'),
            connection_string=os.getenv('AZURE_CONNECTION_STRING')
        )
    elif provider == 's3':
        config = StorageConfig(
            provider='s3',
            container_name=os.getenv('S3_BUCKET', 'ufdr-storage'),
            access_key=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region=os.getenv('AWS_REGION', 'us-east-1')
        )
    else:
        config = StorageConfig(
            provider='local',
            container_name='local-storage'
        )
    
    return CloudStorageManager(config)