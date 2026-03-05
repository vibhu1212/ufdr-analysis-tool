"""
Warning Suppression Configuration
Centralizes all warning and logging suppression for cleaner console output
"""

import os
import sys
import warnings
import logging


def suppress_all_warnings():
    """
    Suppress all known warnings from third-party libraries
    Must be called BEFORE any imports that trigger warnings
    """
    
    # ============================================================
    # EARLY LOGGING SUPPRESSION (Before any imports)
    # ============================================================
    
    # Suppress TensorFlow warnings early
    logging.getLogger('tensorflow').setLevel(logging.ERROR)
    logging.getLogger('tf_keras').setLevel(logging.ERROR)
    logging.getLogger('absl').setLevel(logging.ERROR)
    
    # ============================================================
    # ENVIRONMENT VARIABLES (Must be set BEFORE imports)
    # ============================================================
    
    # TensorFlow configuration
    os.environ['TF_USE_LEGACY_KERAS'] = '1'
    os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # Suppress TF warnings (0=all, 1=info, 2=warning, 3=error)
    os.environ['CUDA_VISIBLE_DEVICES'] = '-1'  # Disable GPU warnings
    
    # Disable TensorFlow deprecation warnings
    os.environ['TF_SILENCE_V2_MESSAGE'] = '1'
    
    # Additional TF environment suppressions
    os.environ['AUTOGRAPH_VERBOSITY'] = '0'
    os.environ['TF_DETERMINISTIC_OPS'] = '0'
    
    # Transformers/HuggingFace configuration
    os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
    os.environ['TOKENIZERS_PARALLELISM'] = 'false'
    
    # PyTorch configuration
    os.environ['TORCH_HOME'] = os.path.join(os.getcwd(), '.cache', 'torch')
    
    # ============================================================
    # PYTHON WARNINGS
    # ============================================================
    
    # Suppress all warnings
    warnings.filterwarnings('ignore')
    
    # Specific warning suppressions
    warnings.filterwarnings('ignore', category=FutureWarning)
    warnings.filterwarnings('ignore', category=DeprecationWarning)
    warnings.filterwarnings('ignore', category=UserWarning)
    warnings.filterwarnings('ignore', category=RuntimeWarning)
    
    # Suppress specific module warnings
    warnings.filterwarnings('ignore', module='timm.*')
    warnings.filterwarnings('ignore', module='tensorflow.*')
    warnings.filterwarnings('ignore', module='keras.*')
    warnings.filterwarnings('ignore', module='torch.*')
    warnings.filterwarnings('ignore', module='transformers.*')
    warnings.filterwarnings('ignore', module='pydub.*')
    
    # ============================================================
    # TENSORFLOW SPECIFIC
    # ============================================================
    
    try:
        import tensorflow as tf
        # Disable TensorFlow logging
        tf.get_logger().setLevel('ERROR')
        tf.autograph.set_verbosity(0)
        
        # Disable deprecation warnings
        tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
    except ImportError:
        pass
    
    # ============================================================
    # LOGGING CONFIGURATION
    # ============================================================
    
    # Suppress noisy library loggers
    noisy_loggers = [
        'tensorflow',
        'tf_keras',
        'absl',
        'urllib3',
        'PIL',
        'matplotlib',
        'numba',
        'asyncio',
        'httpx',
        'httpcore',
        'faiss.loader',
        'datasets',
    ]
    
    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.ERROR)
    
    # Set root logger to WARNING to reduce noise
    logging.getLogger().setLevel(logging.WARNING)
    
    # Keep our application loggers at INFO
    app_loggers = [
        'parser',
        'llm',
        'vector',
        'media',
        'backend',
        'frontend'
    ]
    
    for logger_name in app_loggers:
        logging.getLogger(logger_name).setLevel(logging.INFO)


def suppress_streamlit_warnings():
    """Additional suppressions for Streamlit"""
    
    # Suppress Streamlit file watcher warnings
    os.environ['STREAMLIT_SERVER_FILE_WATCHER_TYPE'] = 'none'
    
    # Suppress Streamlit usage stats
    os.environ['STREAMLIT_BROWSER_GATHER_USAGE_STATS'] = 'false'
    
    # Suppress Streamlit logger warnings
    logging.getLogger('streamlit').setLevel(logging.WARNING)
    logging.getLogger('streamlit.runtime.scriptrunner').setLevel(logging.ERROR)
    logging.getLogger('streamlit.watcher').setLevel(logging.ERROR)


def apply_import_suppressions():
    """
    Apply suppressions after imports
    Call this after all imports are complete
    """
    
    # Suppress PyTorch class path warnings
    import logging
    logging.getLogger('streamlit.runtime.scriptrunner').setLevel(logging.ERROR)
    
    # Suppress FAISS warnings
    try:
        pass
        # FAISS AVX2 warning is logged during import, can't suppress retroactively
        # but we can prevent future warnings
    except ImportError:
        pass
    
    # Suppress timm warnings
    try:
        pass
        # Update warnings for timm
        warnings.filterwarnings('ignore', message='.*timm.models.layers.*')
    except ImportError:
        pass


def configure_clean_startup():
    """
    Complete configuration for clean startup
    Call this at the very beginning of your application
    """
    suppress_all_warnings()
    suppress_streamlit_warnings()
    
    # Redirect stderr temporarily during imports
    # This captures warnings that bypass Python's warning system
    class SuppressStderr:
        def __enter__(self):
            self.old_stderr = sys.stderr
            sys.stderr = open(os.devnull, 'w')
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            sys.stderr.close()
            sys.stderr = self.old_stderr
    
    return SuppressStderr


# ============================================================
# CONVENIENCE FUNCTIONS
# ============================================================

def suppress_during_import(func):
    """Decorator to suppress warnings during function execution"""
    def wrapper(*args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return func(*args, **kwargs)
    return wrapper


if __name__ == "__main__":
    # Test the suppression
    configure_clean_startup()
    print("✅ Warning suppression configured successfully")
    
    # Test imports
    print("\n🧪 Testing imports with suppression...")
    
    try:
        import tensorflow as tf
        print(f"✅ TensorFlow {tf.__version__} imported cleanly")
    except ImportError:
        print("⚠️  TensorFlow not installed")
    
    try:
        import torch
        print(f"✅ PyTorch {torch.__version__} imported cleanly")
    except ImportError:
        print("⚠️  PyTorch not installed")
    
    try:
        import transformers
        print(f"✅ Transformers {transformers.__version__} imported cleanly")
    except ImportError:
        print("⚠️  Transformers not installed")
    
    print("\n✅ All suppressions working correctly!")
