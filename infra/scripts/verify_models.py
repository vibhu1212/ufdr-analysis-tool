#!/usr/bin/env python3
"""
Model Verification Script for UFDR Analysis Tool
Verifies all ML models are present and have correct checksums
"""

import hashlib
import json
import sys
from pathlib import Path
from typing import Dict, Tuple

# Colors for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file"""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def format_size(size_bytes: int) -> str:
    """Format file size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def verify_model(
    model_name: str,
    model_info: Dict,
    models_dir: Path
) -> Tuple[bool, str]:
    """
    Verify a single model file
    
    Returns:
        (is_valid, message)
    """
    # Check if file exists
    model_path = models_dir / model_name
    
    if not model_path.exists():
        if model_info.get('required', True):
            return False, f"{RED}✗{RESET} Missing (required)"
        else:
            return True, f"{YELLOW}○{RESET} Missing (optional)"
    
    # Check file size
    actual_size = model_path.stat().st_size
    expected_size_mb = model_info.get('size_mb', 0)
    expected_size = expected_size_mb * 1024 * 1024
    
    size_diff_percent = abs(actual_size - expected_size) / expected_size * 100 if expected_size > 0 else 0
    
    if size_diff_percent > 10:  # Allow 10% variance
        return False, f"{RED}✗{RESET} Size mismatch (expected: {format_size(expected_size)}, got: {format_size(actual_size)})"
    
    # Check SHA256 if provided and not placeholder
    expected_sha = model_info.get('sha256', '')
    if expected_sha and not expected_sha.startswith('PLACEHOLDER') and not expected_sha.startswith('COMPUTED'):
        print(f"  Computing checksum for {model_name}...", end='', flush=True)
        actual_sha = compute_sha256(model_path)
        print(" done")
        
        if actual_sha != expected_sha:
            return False, f"{RED}✗{RESET} Checksum mismatch"
    
    return True, f"{GREEN}✓{RESET} Valid ({format_size(actual_size)})"


def main():
    """Main verification function"""
    print(f"\n{BLUE}{'='*60}")
    print("UFDR Analysis Tool - Model Verification")
    print(f"{'='*60}{RESET}\n")
    
    # Locate project root and models directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    models_dir = project_root / 'infra' / 'models'
    manifest_path = models_dir / 'MODEL_MANIFEST.json'
    
    print(f"Project root: {project_root}")
    print(f"Models directory: {models_dir}")
    print(f"Manifest: {manifest_path}\n")
    
    # Create models directory if it doesn't exist
    models_dir.mkdir(parents=True, exist_ok=True)
    
    # Load manifest
    if not manifest_path.exists():
        print(f"{RED}ERROR: MODEL_MANIFEST.json not found!{RESET}")
        print(f"Expected at: {manifest_path}")
        sys.exit(1)
    
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    
    print(f"Manifest version: {manifest.get('manifest_version', 'unknown')}")
    print(f"Total models: {len(manifest.get('models', {}))}\n")
    
    # Verify each model
    results = []
    missing_required = []
    missing_optional = []
    invalid_models = []
    
    print(f"{BLUE}Verifying models...{RESET}\n")
    
    for model_name, model_info in manifest.get('models', {}).items():
        print(f"  {model_name:50s} ", end='')
        is_valid, message = verify_model(model_name, model_info, models_dir)
        print(message)
        
        results.append((model_name, is_valid, message))
        
        if not is_valid:
            if model_info.get('required', True):
                missing_required.append(model_name)
            else:
                missing_optional.append(model_name)
        
        if 'mismatch' in message.lower():
            invalid_models.append(model_name)
    
    # Summary
    print(f"\n{BLUE}{'='*60}")
    print("VERIFICATION SUMMARY")
    print(f"{'='*60}{RESET}\n")
    
    total_models = len(results)
    valid_models = sum(1 for _, is_valid, _ in results if is_valid)
    
    print(f"Total models checked: {total_models}")
    print(f"{GREEN}Valid models: {valid_models}{RESET}")
    print(f"{RED}Invalid/Missing models: {total_models - valid_models}{RESET}")
    
    if missing_required:
        print(f"\n{RED}Missing required models:{RESET}")
        for model in missing_required:
            model_info = manifest['models'][model]
            print(f"  - {model}")
            print(f"    Download: {model_info.get('download_command', 'N/A')}")
    
    if missing_optional:
        print(f"\n{YELLOW}Missing optional models:{RESET}")
        for model in missing_optional:
            model_info = manifest['models'][model]
            print(f"  - {model}")
            print(f"    Download: {model_info.get('download_command', 'N/A')}")
    
    if invalid_models:
        print(f"\n{RED}Invalid models (checksum/size mismatch):{RESET}")
        for model in invalid_models:
            print(f"  - {model}")
            print(f"    Action: Delete and re-download")
    
    # Storage summary
    required_size = manifest.get('total_size_required_mb', 0)
    optional_size = manifest.get('total_size_with_optional_mb', 0) - required_size
    
    print(f"\n{BLUE}Storage requirements:{RESET}")
    print(f"  Required models: {required_size:.1f} MB")
    print(f"  Optional models: {optional_size:.1f} MB")
    print(f"  Total with optional: {manifest.get('total_size_with_optional_mb', 0):.1f} MB")
    
    # Exit code
    if missing_required or invalid_models:
        print(f"\n{RED}VERIFICATION FAILED{RESET}")
        print(f"\nTo download missing models, run:")
        print(f"  python infra/scripts/download_models.py")
        sys.exit(1)
    elif missing_optional:
        print(f"\n{YELLOW}VERIFICATION PASSED (with warnings){RESET}")
        print(f"Optional models missing but system will work.")
        sys.exit(0)
    else:
        print(f"\n{GREEN}VERIFICATION PASSED{RESET}")
        print(f"All required models are present and valid.")
        sys.exit(0)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{YELLOW}Verification interrupted by user{RESET}")
        sys.exit(130)
    except Exception as e:
        print(f"\n{RED}ERROR: {e}{RESET}")
        import traceback
        traceback.print_exc()
        sys.exit(1)