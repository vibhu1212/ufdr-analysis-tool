"""
Hardware Detection and Capability Assessment
Automatically detects GPU availability and configures optimal execution mode
"""

import os
import logging
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class ExecutionMode(Enum):
    """Execution mode based on hardware"""
    GPU_CUDA = "gpu_cuda"           # NVIDIA GPU with CUDA
    GPU_ROCM = "gpu_rocm"           # AMD GPU with ROCm
    GPU_METAL = "gpu_metal"         # Apple Silicon GPU
    CPU_OPTIMIZED = "cpu_optimized" # CPU with optimizations
    CPU_BASIC = "cpu_basic"         # Basic CPU mode


@dataclass
class HardwareCapabilities:
    """Hardware capabilities and configuration"""
    execution_mode: ExecutionMode
    device_name: str
    cuda_available: bool = False
    cuda_version: Optional[str] = None
    gpu_count: int = 0
    gpu_memory_gb: float = 0.0
    cpu_cores: int = 0
    total_ram_gb: float = 0.0
    supports_fp16: bool = False
    supports_int8: bool = False
    recommended_batch_size: int = 1
    max_context_length: int = 512
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'execution_mode': self.execution_mode.value,
            'device_name': self.device_name,
            'cuda_available': self.cuda_available,
            'cuda_version': self.cuda_version,
            'gpu_count': self.gpu_count,
            'gpu_memory_gb': self.gpu_memory_gb,
            'cpu_cores': self.cpu_cores,
            'total_ram_gb': self.total_ram_gb,
            'supports_fp16': self.supports_fp16,
            'supports_int8': self.supports_int8,
            'recommended_batch_size': self.recommended_batch_size,
            'max_context_length': self.max_context_length
        }


class HardwareDetector:
    """
    Detects hardware capabilities and recommends optimal execution mode
    """
    
    def __init__(self):
        self.capabilities: Optional[HardwareCapabilities] = None
        self._detect_hardware()
    
    def _detect_hardware(self):
        """Detect and assess hardware capabilities"""
        logger.info("Detecting hardware capabilities...")
        
        # Detect CUDA (NVIDIA GPU)
        cuda_available, cuda_version, gpu_info = self._detect_cuda()
        
        # Detect CPU info
        cpu_cores, total_ram_gb = self._detect_cpu()
        
        # Determine execution mode
        if cuda_available and gpu_info['gpu_count'] > 0:
            execution_mode = ExecutionMode.GPU_CUDA
            device_name = gpu_info['device_name']
            gpu_memory_gb = gpu_info['gpu_memory_gb']
            
            # Determine capabilities based on GPU memory
            if gpu_memory_gb >= 8:
                supports_fp16 = True
                supports_int8 = True
                recommended_batch_size = 8
                max_context_length = 2048
            elif gpu_memory_gb >= 6:
                supports_fp16 = True
                supports_int8 = True
                recommended_batch_size = 4
                max_context_length = 1024
            elif gpu_memory_gb >= 4:
                supports_fp16 = True
                supports_int8 = True
                recommended_batch_size = 2
                max_context_length = 512
            else:
                # GPU too small, fall back to CPU
                execution_mode = ExecutionMode.CPU_OPTIMIZED
                device_name = "CPU (GPU insufficient)"
                supports_fp16 = False
                supports_int8 = True
                recommended_batch_size = 1
                max_context_length = 512
        else:
            # No GPU, use CPU
            execution_mode = ExecutionMode.CPU_OPTIMIZED
            device_name = f"CPU ({cpu_cores} cores)"
            gpu_memory_gb = 0.0
            supports_fp16 = False
            supports_int8 = True
            
            # CPU batch size based on RAM
            if total_ram_gb >= 16:
                recommended_batch_size = 4
                max_context_length = 1024
            elif total_ram_gb >= 8:
                recommended_batch_size = 2
                max_context_length = 512
            else:
                recommended_batch_size = 1
                max_context_length = 256
        
        self.capabilities = HardwareCapabilities(
            execution_mode=execution_mode,
            device_name=device_name,
            cuda_available=cuda_available,
            cuda_version=cuda_version,
            gpu_count=gpu_info['gpu_count'] if cuda_available else 0,
            gpu_memory_gb=gpu_memory_gb,
            cpu_cores=cpu_cores,
            total_ram_gb=total_ram_gb,
            supports_fp16=supports_fp16,
            supports_int8=supports_int8,
            recommended_batch_size=recommended_batch_size,
            max_context_length=max_context_length
        )
        
        self._log_capabilities()
    
    def _detect_cuda(self) -> Tuple[bool, Optional[str], Dict]:
        """Detect CUDA availability and GPU info"""
        gpu_info = {
            'gpu_count': 0,
            'device_name': 'Unknown',
            'gpu_memory_gb': 0.0
        }
        
        try:
            import torch
            
            if torch.cuda.is_available():
                cuda_version = torch.version.cuda
                gpu_count = torch.cuda.device_count()
                
                # Get first GPU info
                if gpu_count > 0:
                    device_name = torch.cuda.get_device_name(0)
                    gpu_memory_bytes = torch.cuda.get_device_properties(0).total_memory
                    gpu_memory_gb = gpu_memory_bytes / (1024**3)
                    
                    gpu_info = {
                        'gpu_count': gpu_count,
                        'device_name': device_name,
                        'gpu_memory_gb': gpu_memory_gb
                    }
                    
                    logger.info(f"CUDA detected: {device_name} ({gpu_memory_gb:.1f} GB)")
                    return True, cuda_version, gpu_info
            
            logger.info("CUDA not available")
            return False, None, gpu_info
            
        except ImportError:
            logger.warning("PyTorch not installed, cannot detect CUDA")
            return False, None, gpu_info
        except Exception as e:
            logger.error(f"Error detecting CUDA: {e}")
            return False, None, gpu_info
    
    def _detect_cpu(self) -> Tuple[int, float]:
        """Detect CPU cores and RAM"""
        try:
            import psutil
            
            cpu_cores = psutil.cpu_count(logical=False) or psutil.cpu_count(logical=True)
            total_ram_bytes = psutil.virtual_memory().total
            total_ram_gb = total_ram_bytes / (1024**3)
            
            logger.info(f"CPU: {cpu_cores} cores, RAM: {total_ram_gb:.1f} GB")
            return cpu_cores, total_ram_gb
            
        except ImportError:
            logger.warning("psutil not installed, using fallback CPU detection")
            import os
            cpu_cores = os.cpu_count() or 2
            # Rough estimate
            total_ram_gb = 8.0
            return cpu_cores, total_ram_gb
    
    def _log_capabilities(self):
        """Log detected capabilities"""
        caps = self.capabilities
        logger.info("="*60)
        logger.info("Hardware Configuration:")
        logger.info(f"  Execution Mode: {caps.execution_mode.value.upper()}")
        logger.info(f"  Device: {caps.device_name}")
        logger.info(f"  CUDA Available: {caps.cuda_available}")
        if caps.cuda_available:
            logger.info(f"  CUDA Version: {caps.cuda_version}")
            logger.info(f"  GPU Count: {caps.gpu_count}")
            logger.info(f"  GPU Memory: {caps.gpu_memory_gb:.1f} GB")
        logger.info(f"  CPU Cores: {caps.cpu_cores}")
        logger.info(f"  Total RAM: {caps.total_ram_gb:.1f} GB")
        logger.info(f"  FP16 Support: {caps.supports_fp16}")
        logger.info(f"  INT8 Support: {caps.supports_int8}")
        logger.info(f"  Recommended Batch Size: {caps.recommended_batch_size}")
        logger.info(f"  Max Context Length: {caps.max_context_length}")
        logger.info("="*60)
    
    def get_device_string(self) -> str:
        """Get PyTorch device string"""
        if self.capabilities.execution_mode == ExecutionMode.GPU_CUDA:
            return "cuda"
        return "cpu"
    
    def get_faiss_index_type(self) -> str:
        """Get recommended FAISS index type"""
        if self.capabilities.execution_mode == ExecutionMode.GPU_CUDA:
            return "gpu"
        return "cpu"
    
    def should_use_fp16(self) -> bool:
        """Check if FP16 should be used"""
        return self.capabilities.supports_fp16
    
    def get_optimal_threads(self) -> int:
        """Get optimal number of threads for CPU operations"""
        if self.capabilities.execution_mode in [ExecutionMode.GPU_CUDA, ExecutionMode.GPU_ROCM]:
            # Use fewer threads when GPU is primary
            return max(2, self.capabilities.cpu_cores // 4)
        else:
            # Use more threads for CPU-only mode
            return max(1, self.capabilities.cpu_cores - 1)
    
    def configure_environment(self):
        """Configure environment variables for optimal performance"""
        # Set number of threads
        threads = str(self.get_optimal_threads())
        os.environ['OMP_NUM_THREADS'] = threads
        os.environ['MKL_NUM_THREADS'] = threads
        os.environ['NUMEXPR_NUM_THREADS'] = threads
        
        if self.capabilities.execution_mode == ExecutionMode.GPU_CUDA:
            # GPU optimizations
            os.environ['CUDA_LAUNCH_BLOCKING'] = '0'  # Async kernel launches
            os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'  # TensorFlow GPU memory growth
        else:
            # CPU optimizations
            os.environ['TF_NUM_INTRAOP_THREADS'] = threads
            os.environ['TF_NUM_INTEROP_THREADS'] = '2'
        
        logger.info(f"Configured environment for {self.capabilities.execution_mode.value}")


# Global hardware detector instance
_hardware_detector: Optional[HardwareDetector] = None


def get_hardware_detector() -> HardwareDetector:
    """Get or create global hardware detector instance"""
    global _hardware_detector
    if _hardware_detector is None:
        _hardware_detector = HardwareDetector()
    return _hardware_detector


def get_capabilities() -> HardwareCapabilities:
    """Get hardware capabilities"""
    return get_hardware_detector().capabilities


def get_device() -> str:
    """Get PyTorch device string (cuda/cpu)"""
    return get_hardware_detector().get_device_string()


def configure_hardware():
    """Configure hardware environment (call at startup)"""
    detector = get_hardware_detector()
    detector.configure_environment()
    return detector.capabilities


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Detect and configure hardware
    caps = configure_hardware()
    
    print("\nHardware Summary:")
    print(f"Execution Mode: {caps.execution_mode.value}")
    print(f"Device: {caps.device_name}")
    print(f"Recommended Batch Size: {caps.recommended_batch_size}")
    
    # Example: Get device for PyTorch
    device = get_device()
    print(f"\nPyTorch device: {device}")