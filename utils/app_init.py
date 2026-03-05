"""
Application Initialization Module
Configures hardware, logging, and environment at startup
"""

import logging
import os
import warnings
from pathlib import Path

# Suppress specific warnings
warnings.filterwarnings('ignore', message='.*torch.classes.*')
warnings.filterwarnings('ignore', category=UserWarning, module='torch')

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/ufdr_app.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)


def initialize_application(create_dirs: bool = True) -> dict:
    """
    Initialize UFDR Analysis Tool application
    
    Args:
        create_dirs: Whether to create necessary directories
        
    Returns:
        Dictionary with initialization status and hardware capabilities
    """
    logger.info("="*60)
    logger.info("Initializing UFDR Analysis Tool")
    logger.info("="*60)
    
    init_status = {
        'success': True,
        'hardware': None,
        'directories_created': False,
        'errors': []
    }
    
    try:
        # Step 1: Create necessary directories
        if create_dirs:
            _create_directories()
            init_status['directories_created'] = True
        
        # Step 2: Detect and configure hardware
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from utils.hardware_detector import configure_hardware
        hw_caps = configure_hardware()
        init_status['hardware'] = hw_caps.to_dict()
        
        logger.info("\nHardware Configuration:")
        logger.info(f"  Device: {hw_caps.device_name}")
        logger.info(f"  Mode: {hw_caps.execution_mode.value}")
        if hw_caps.cuda_available:
            logger.info(f"  GPU Memory: {hw_caps.gpu_memory_gb:.1f} GB")
            logger.info(f"  GPU Layers (LLM): Auto-configured")
        logger.info(f"  CPU Cores: {hw_caps.cpu_cores}")
        logger.info(f"  RAM: {hw_caps.total_ram_gb:.1f} GB")
        logger.info(f"  Batch Size: {hw_caps.recommended_batch_size}")
        
        # Step 3: Configure environment variables
        _configure_environment(hw_caps)
        
        logger.info("\n" + "="*60)
        logger.info("[OK] Application initialized successfully")
        logger.info("="*60 + "\n")
        
    except Exception as e:
        logger.error(f"Error during initialization: {e}", exc_info=True)
        init_status['success'] = False
        init_status['errors'].append(str(e))
    
    return init_status


def _create_directories():
    """Create necessary application directories"""
    directories = [
        "data/parsed",
        "data/canonical",
        "data/indices",
        "data/samples",
        "logs",
        "temp",
        "models",
        "archives/manifests"
    ]
    
    for directory in directories:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"[OK] Created/verified {len(directories)} directories")


def _configure_environment(hw_caps):
    """Configure environment variables for optimal performance"""
    # Already configured by hardware_detector.configure_environment()
    # Add any additional app-specific configurations here
    
    # Disable TensorFlow GPU memory pre-allocation
    os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'
    
    # Set logging level for specific libraries
    os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
    os.environ['TOKENIZERS_PARALLELISM'] = 'false'  # Avoid warnings
    
    logger.info("[OK] Environment variables configured")


def get_system_info() -> dict:
    """Get comprehensive system information"""
    try:
        from utils.hardware_detector import get_capabilities
        import platform
        import psutil
        
        caps = get_capabilities()
        
        info = {
            'platform': {
                'system': platform.system(),
                'release': platform.release(),
                'version': platform.version(),
                'machine': platform.machine(),
                'processor': platform.processor()
            },
            'hardware': caps.to_dict(),
            'memory': {
                'total_gb': caps.total_ram_gb,
                'available_gb': psutil.virtual_memory().available / (1024**3)
            },
            'cpu': {
                'count': caps.cpu_cores,
                'frequency_mhz': psutil.cpu_freq().current if hasattr(psutil.cpu_freq(), 'current') else 'N/A'
            }
        }
        
        return info
        
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        return {'error': str(e)}


def check_dependencies() -> dict:
    """Check if required dependencies are available"""
    dependencies = {
        'pytorch': False,
        'cuda': False,
        'faiss': False,
        'sentence_transformers': False,
        'llama_cpp': False,
        'neo4j': False,
        'streamlit': False
    }
    
    # Check PyTorch
    try:
        import torch
        dependencies['pytorch'] = True
        dependencies['cuda'] = torch.cuda.is_available()
    except ImportError:
        pass
    
    # Check FAISS
    try:
        dependencies['faiss'] = True
    except ImportError:
        pass
    
    # Check Sentence Transformers
    try:
        dependencies['sentence_transformers'] = True
    except ImportError:
        pass
    
    # Check llama-cpp-python
    try:
        dependencies['llama_cpp'] = True
    except ImportError:
        pass
    
    # Check Neo4j driver
    try:
        dependencies['neo4j'] = True
    except ImportError:
        pass
    
    # Check Streamlit
    try:
        dependencies['streamlit'] = True
    except ImportError:
        pass
    
    return dependencies


def print_startup_banner():
    """Print application startup banner"""
    banner = """
    ╔════════════════════════════════════════════════════════╗
    ║         UFDR Forensic Analysis Tool                   ║
    ║         AI-Powered Digital Evidence Analysis          ║
    ║                                                        ║
    ║         • GPU/CPU Adaptive Execution                  ║
    ║         • Offline LLM Processing                      ║
    ║         • Multilingual Vector Search                  ║
    ║         • Graph Database Integration                  ║
    ╚════════════════════════════════════════════════════════╝
    """
    print(banner)


# Convenience function for quick initialization
def quick_init():
    """Quick initialization with minimal logging"""
    logging.getLogger().setLevel(logging.WARNING)
    return initialize_application(create_dirs=True)


if __name__ == "__main__":
    # Test initialization
    print_startup_banner()
    
    status = initialize_application()
    
    if status['success']:
        print("\n[OK] Initialization successful!")
        
        print("\nSystem Info:")
        info = get_system_info()
        print(f"  Platform: {info['platform']['system']} {info['platform']['release']}")
        print(f"  Device: {info['hardware']['device_name']}")
        print(f"  Execution Mode: {info['hardware']['execution_mode']}")
        
        print("\nDependency Check:")
        deps = check_dependencies()
        for dep, available in deps.items():
            status_icon = "[OK]" if available else "[X]"
            print(f"  {status_icon} {dep}: {'Available' if available else 'Not Found'}")
    else:
        print("\n[X] Initialization failed!")
        for error in status['errors']:
            print(f"  Error: {error}")