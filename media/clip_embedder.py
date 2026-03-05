"""
CLIP Image Embedder
Generates semantic embeddings for images using CLIP ViT-B/32
"""

import logging
from pathlib import Path
from typing import Optional, Union, List
import numpy as np

try:
    import torch
    from PIL import Image
    import open_clip
    CLIP_AVAILABLE = True
except ImportError:
    CLIP_AVAILABLE = False
    logging.warning("CLIP dependencies not available. Install with: pip install open-clip-torch pillow")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CLIPEmbedder:
    """
    CLIP-based image embedder for semantic image search
    Uses OpenAI CLIP ViT-B/32 model
    """
    
    def __init__(self, model_name: str = "ViT-B-32", pretrained: str = "openai"):
        """
        Initialize CLIP embedder
        
        Args:
            model_name: CLIP model architecture
            pretrained: Pretrained weights to use
        """
        if not CLIP_AVAILABLE:
            raise ImportError("CLIP dependencies required. Install: pip install open-clip-torch pillow")
        
        self.model_name = model_name
        self.pretrained = pretrained
        self.model = None
        self.preprocess = None
        self.tokenizer = None
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        
        logger.info(f"CLIP Embedder initialized (device: {self.device})")
    
    def load_model(self):
        """Load CLIP model (lazy loading)"""
        if self.model is not None:
            return  # Already loaded
        
        logger.info(f"Loading CLIP model: {self.model_name} ({self.pretrained})")
        
        try:
            self.model, _, self.preprocess = open_clip.create_model_and_transforms(
                self.model_name,
                pretrained=self.pretrained,
                device=self.device
            )
            self.tokenizer = open_clip.get_tokenizer(self.model_name)
            
            # Set to evaluation mode
            self.model.eval()
            
            logger.info(f"✅ CLIP model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load CLIP model: {e}")
            raise
    
    def encode_image(self, image_path: Union[str, Path]) -> Optional[np.ndarray]:
        """
        Generate embedding for a single image
        
        Args:
            image_path: Path to image file
            
        Returns:
            512-dimensional embedding vector (numpy array)
        """
        if self.model is None:
            self.load_model()
        
        try:
            # Load and preprocess image
            image = Image.open(image_path).convert("RGB")
            image_tensor = self.preprocess(image).unsqueeze(0).to(self.device)
            
            # Generate embedding
            with torch.no_grad():
                image_features = self.model.encode_image(image_tensor)
                # Normalize embedding
                image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            
            # Convert to numpy
            embedding = image_features.cpu().numpy()[0]
            
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to encode image {image_path}: {e}")
            return None
    
    def encode_images(self, image_paths: List[Union[str, Path]], batch_size: int = 32) -> np.ndarray:
        """
        Generate embeddings for multiple images in batches
        
        Args:
            image_paths: List of image file paths
            batch_size: Number of images to process at once
            
        Returns:
            Array of embeddings (n_images, 512)
        """
        if self.model is None:
            self.load_model()
        
        embeddings = []
        
        try:
            # Process in batches
            for i in range(0, len(image_paths), batch_size):
                batch_paths = image_paths[i:i + batch_size]
                batch_images = []
                
                # Load and preprocess batch
                for path in batch_paths:
                    try:
                        image = Image.open(path).convert("RGB")
                        image_tensor = self.preprocess(image)
                        batch_images.append(image_tensor)
                    except Exception as e:
                        logger.warning(f"Failed to load image {path}: {e}")
                        # Add zero embedding for failed images
                        batch_images.append(torch.zeros(3, 224, 224))
                
                # Stack into batch
                batch_tensor = torch.stack(batch_images).to(self.device)
                
                # Generate embeddings
                with torch.no_grad():
                    batch_features = self.model.encode_image(batch_tensor)
                    # Normalize
                    batch_features = batch_features / batch_features.norm(dim=-1, keepdim=True)
                
                embeddings.append(batch_features.cpu().numpy())
                
                if (i + batch_size) % 100 == 0:
                    logger.info(f"Processed {min(i + batch_size, len(image_paths))}/{len(image_paths)} images")
            
            # Concatenate all batches
            all_embeddings = np.vstack(embeddings)
            
            logger.info(f"✅ Generated embeddings for {len(image_paths)} images")
            return all_embeddings
            
        except Exception as e:
            logger.error(f"Failed to encode images batch: {e}")
            return np.array([])
    
    def encode_text(self, text: str) -> Optional[np.ndarray]:
        """
        Generate embedding for text query (for text-to-image search)
        
        Args:
            text: Text query
            
        Returns:
            512-dimensional embedding vector
        """
        if self.model is None:
            self.load_model()
        
        try:
            # Tokenize text
            text_tokens = self.tokenizer([text]).to(self.device)
            
            # Generate embedding
            with torch.no_grad():
                text_features = self.model.encode_text(text_tokens)
                # Normalize
                text_features = text_features / text_features.norm(dim=-1, keepdim=True)
            
            # Convert to numpy
            embedding = text_features.cpu().numpy()[0]
            
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to encode text '{text}': {e}")
            return None
    
    def encode_texts(self, texts: List[str]) -> np.ndarray:
        """
        Generate embeddings for multiple text queries
        
        Args:
            texts: List of text queries
            
        Returns:
            Array of embeddings (n_texts, 512)
        """
        if self.model is None:
            self.load_model()
        
        try:
            # Tokenize all texts
            text_tokens = self.tokenizer(texts).to(self.device)
            
            # Generate embeddings
            with torch.no_grad():
                text_features = self.model.encode_text(text_tokens)
                # Normalize
                text_features = text_features / text_features.norm(dim=-1, keepdim=True)
            
            # Convert to numpy
            embeddings = text_features.cpu().numpy()
            
            logger.info(f"✅ Generated embeddings for {len(texts)} text queries")
            return embeddings
            
        except Exception as e:
            logger.error(f"Failed to encode texts: {e}")
            return np.array([])
    
    def compute_similarity(self, image_embedding: np.ndarray, text_embedding: np.ndarray) -> float:
        """
        Compute cosine similarity between image and text embeddings
        
        Args:
            image_embedding: Image embedding vector
            text_embedding: Text embedding vector
            
        Returns:
            Similarity score (0-1, higher is better)
        """
        # Cosine similarity (already normalized)
        similarity = np.dot(image_embedding, text_embedding)
        return float(similarity)
    
    def find_most_similar(
        self, 
        query_text: str, 
        image_embeddings: np.ndarray,
        top_k: int = 10
    ) -> List[tuple]:
        """
        Find most similar images to text query
        
        Args:
            query_text: Text query
            image_embeddings: Array of image embeddings (n, 512)
            top_k: Number of top results to return
            
        Returns:
            List of (index, similarity_score) tuples
        """
        # Encode query text
        text_embedding = self.encode_text(query_text)
        
        if text_embedding is None:
            return []
        
        # Compute similarities
        similarities = np.dot(image_embeddings, text_embedding)
        
        # Get top-k indices
        top_indices = np.argsort(similarities)[::-1][:top_k]
        
        # Return (index, score) pairs
        results = [(int(idx), float(similarities[idx])) for idx in top_indices]
        
        return results
    
    def unload_model(self):
        """Unload model to free memory"""
        if self.model is not None:
            del self.model
            del self.preprocess
            del self.tokenizer
            self.model = None
            self.preprocess = None
            self.tokenizer = None
            
            # Clear CUDA cache if using GPU
            if self.device == "cuda":
                torch.cuda.empty_cache()
            
            logger.info("✅ CLIP model unloaded")


# Singleton instance
_clip_embedder_instance = None


def get_clip_embedder() -> CLIPEmbedder:
    """Get singleton CLIP embedder instance"""
    global _clip_embedder_instance
    if _clip_embedder_instance is None:
        _clip_embedder_instance = CLIPEmbedder()
    return _clip_embedder_instance
