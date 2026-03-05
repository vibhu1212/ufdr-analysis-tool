"""
Image Utility Functions
Handles multiple image formats and common operations
"""

from pathlib import Path
from typing import List, Union

# Supported image formats
SUPPORTED_IMAGE_FORMATS = {
    '.jpg',
    '.jpeg',
    '.png',
    '.bmp',
    '.gif',
    '.tiff',
    '.tif',
    '.webp',
    '.ico',
    '.jfif'
}


def get_image_files(
    directory: Union[str, Path],
    recursive: bool = False
) -> List[Path]:
    """
    Get all image files from a directory
    
    Args:
        directory: Path to directory
        recursive: Search subdirectories
    
    Returns:
        List of image file paths
    """
    directory = Path(directory)
    
    if not directory.exists():
        return []
    
    image_files = []
    
    for ext in SUPPORTED_IMAGE_FORMATS:
        if recursive:
            pattern = f"**/*{ext}"
        else:
            pattern = f"*{ext}"
        
        # Case insensitive search
        image_files.extend(directory.glob(pattern))
        # Also check uppercase extensions
        image_files.extend(directory.glob(pattern.replace(ext, ext.upper())))
    
    # Remove duplicates and sort
    image_files = sorted(set(image_files))
    
    return image_files


def is_image_file(file_path: Union[str, Path]) -> bool:
    """
    Check if file is a supported image format
    
    Args:
        file_path: Path to file
    
    Returns:
        True if supported image format
    """
    file_path = Path(file_path)
    return file_path.suffix.lower() in SUPPORTED_IMAGE_FORMATS


def get_supported_formats() -> List[str]:
    """Get list of supported image formats"""
    return sorted(list(SUPPORTED_IMAGE_FORMATS))


def format_supported_formats() -> str:
    """Get human-readable list of supported formats"""
    formats = get_supported_formats()
    return ', '.join(formats)
