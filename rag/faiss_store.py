"""
FAISS Vector Store for UFDR Analysis Tool

Drop-in replacement for ChromaStore. Uses Facebook AI Similarity Search (FAISS)
for fast, scalable vector similarity search.

Architecture:
- Each case stored as a separate FAISS index + metadata JSON
- Uses SentenceTransformer directly for embeddings (no ChromaDB wrapper)
- IndexFlatIP for exact inner-product (cosine) similarity
- Metadata filtering done post-retrieval (over-fetch → filter → return)
- Persisted to data/faiss_indices/{case_id}/

Scales to millions of vectors per case.
"""

import os
import json
import logging
import numpy as np
from typing import Optional

import faiss

from rag import FAISS_DIR
from rag.embeddings import get_embedder

logger = logging.getLogger(__name__)


class FAISSStore:
    """
    FAISS vector store for forensic case data.
    
    Same public API as ChromaStore for seamless migration.
    Each case gets its own FAISS index for isolation and easy deletion.
    """
    
    def __init__(self, persist_dir: str = FAISS_DIR):
        """
        Initialize FAISS store.
        
        Args:
            persist_dir: Directory for persistent storage of indices
        """
        self.persist_dir = persist_dir
        os.makedirs(persist_dir, exist_ok=True)
        self._embedder = get_embedder()
        self._dimension = self._embedder.get_sentence_embedding_dimension()
        
        # In-memory cache: case_id → {index, documents, metadatas, ids}
        self._cache: dict[str, dict] = {}
        
        logger.info(f"FAISS store initialized at: {persist_dir} (dim={self._dimension})")
    
    def _case_dir(self, case_id: str, modality: str = "text") -> str:
        """Get the directory for a case's index files."""
        suffix = f"_{modality}" if modality != "text" else ""
        d = os.path.join(self.persist_dir, f"{case_id}{suffix}")
        os.makedirs(d, exist_ok=True)
        return d
    
    def _load_case(self, case_id: str, modality: str = "text") -> dict:
        """Load a case's index and metadata from disk into memory."""
        cache_key = f"{case_id}_{modality}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        case_dir = self._case_dir(case_id, modality)
        index_path = os.path.join(case_dir, "index.faiss")
        meta_path = os.path.join(case_dir, "metadata.json")
        
        if not os.path.exists(index_path) or not os.path.exists(meta_path):
            # Return empty case data
            return {
                "index": faiss.IndexFlatIP(self._dimension),
                "documents": [],
                "metadatas": [],
                "ids": [],
            }
        
        try:
            index = faiss.read_index(index_path)
            with open(meta_path, "r") as f:
                meta = json.load(f)
            
            data = {
                "index": index,
                "documents": meta["documents"],
                "metadatas": meta["metadatas"],
                "ids": meta["ids"],
            }
            self._cache[cache_key] = data
            logger.debug(f"Loaded FAISS index for case '{case_id}' ({modality}): {index.ntotal} vectors")
            return data
        except Exception as e:
            logger.error(f"Failed to load FAISS index for '{case_id}': {e}")
            return {
                "index": faiss.IndexFlatIP(self._dimension),
                "documents": [],
                "metadatas": [],
                "ids": [],
            }
    
    def _save_case(self, case_id: str, data: dict, modality: str = "text"):
        """Save a case's index and metadata to disk."""
        case_dir = self._case_dir(case_id, modality)
        index_path = os.path.join(case_dir, "index.faiss")
        meta_path = os.path.join(case_dir, "metadata.json")
        
        faiss.write_index(data["index"], index_path)
        with open(meta_path, "w") as f:
            json.dump({
                "documents": data["documents"],
                "metadatas": data["metadatas"],
                "ids": data["ids"],
            }, f)
        
        logger.debug(f"Saved FAISS index for '{case_id}' ({modality}): {data['index'].ntotal} vectors")
    
    def add_documents(
        self,
        case_id: str,
        documents: list[str],
        metadatas: list[dict],
        ids: list[str],
        batch_size: int = 100,
        modality: str = "text",
        embeddings: Optional[list[list[float]]] = None
    ) -> int:
        """
        Add documents to a case's FAISS index in batches.
        
        Args:
            case_id: Case identifier
            documents: List of text documents
            metadatas: List of metadata dicts
            ids: List of unique IDs
            batch_size: Not strictly needed for FAISS but kept for API compat
            modality: "text" or "image"
            embeddings: Optional pre-computed embeddings (required for image)
        
        Returns:
            Number of documents added
        """
        if not documents:
            return 0
        
        cache_key = f"{case_id}_{modality}"
        data = self._load_case(case_id, modality)
        
        # Generate embeddings if not provided
        if embeddings is None:
            logger.info(f"Embedding {len(documents)} documents for case '{case_id}'...")
            emb_array = self._embedder.encode(
                documents, 
                batch_size=batch_size, 
                show_progress_bar=len(documents) > 100,
                normalize_embeddings=True
            )
        else:
            emb_array = np.array(embeddings, dtype=np.float32)
            # Normalize for cosine similarity via inner product
            norms = np.linalg.norm(emb_array, axis=1, keepdims=True)
            norms[norms == 0] = 1  # avoid division by zero
            emb_array = emb_array / norms
        
        # Ensure float32
        if not isinstance(emb_array, np.ndarray):
            emb_array = np.array(emb_array, dtype=np.float32)
        emb_array = emb_array.astype(np.float32)
        
        # Handle dimension mismatch for image embeddings
        if emb_array.shape[1] != self._dimension and modality == "image":
            # Create a separate index for images with their own dimension
            img_dim = emb_array.shape[1]
            if data["index"].ntotal == 0:
                data["index"] = faiss.IndexFlatIP(img_dim)
        
        # Sanitize metadata
        clean_metas = []
        for m in metadatas:
            clean = {}
            for k, v in m.items():
                if v is None:
                    clean[k] = ""
                elif isinstance(v, (str, int, float, bool)):
                    clean[k] = v
                else:
                    clean[k] = str(v)
            clean_metas.append(clean)
        
        # Add to index
        data["index"].add(emb_array)
        data["documents"].extend(documents)
        data["metadatas"].extend(clean_metas)
        data["ids"].extend(ids)
        
        # Update cache and save
        self._cache[cache_key] = data
        self._save_case(case_id, data, modality)
        
        logger.info(f"Indexed {len(documents)} documents for case '{case_id}' (total: {data['index'].ntotal})")
        return len(documents)
    
    def query(
        self,
        case_id: str,
        query_text: str,
        n_results: int = 20,
        where: Optional[dict] = None,
        where_document: Optional[dict] = None,
        modality: str = "text",
        query_embeddings: Optional[list[list[float]]] = None
    ) -> dict:
        """
        Query a case's index with semantic search.
        
        Args:
            case_id: Case to search
            query_text: Natural language query
            n_results: Max results
            where: Metadata filter (e.g., {"data_type": "message"})
            where_document: Document content filter (not used in FAISS)
            modality: "text" or "image"
            query_embeddings: Optional pre-computed query embedding
        
        Returns:
            Dict with keys: ids, documents, metadatas, distances
        """
        data = self._load_case(case_id, modality)
        
        if data["index"].ntotal == 0:
            return {"ids": [], "documents": [], "metadatas": [], "distances": []}
        
        # Generate query embedding
        if query_embeddings:
            q_vec = np.array(query_embeddings, dtype=np.float32)
            if q_vec.ndim == 3 and q_vec.shape[0] == 1:
                q_vec = q_vec[0]
            norms = np.linalg.norm(q_vec, axis=1, keepdims=True)
            norms[norms == 0] = 1
            q_vec = q_vec / norms
        else:
            q_vec = self._embedder.encode(
                [query_text], normalize_embeddings=True
            ).astype(np.float32)
        
        # Over-fetch if we need to filter
        fetch_k = min(n_results * 3 if where else n_results, data["index"].ntotal)
        
        # FAISS search (inner product — higher is better)
        scores, indices = data["index"].search(q_vec, fetch_k)
        scores = scores[0]  # flatten batch dim
        indices = indices[0]
        
        # Build results with optional filtering
        result_ids = []
        result_docs = []
        result_metas = []
        result_distances = []
        
        for score, idx in zip(scores, indices):
            if idx < 0:  # FAISS returns -1 for not-found
                continue
            
            meta = data["metadatas"][idx]
            
            # Apply metadata filter
            if where:
                match = all(meta.get(k) == v for k, v in where.items())
                if not match:
                    continue
            
            # Convert inner-product score to distance (lower = more similar for compatibility)
            distance = 1.0 - float(score)
            
            result_ids.append(data["ids"][idx])
            result_docs.append(data["documents"][idx])
            result_metas.append(meta)
            result_distances.append(distance)
            
            if len(result_ids) >= n_results:
                break
        
        return {
            "ids": result_ids,
            "documents": result_docs,
            "metadatas": result_metas,
            "distances": result_distances,
        }
    
    def query_multiple_cases(
        self,
        case_ids: list[str],
        query_text: str,
        n_results: int = 20,
        where: Optional[dict] = None,
        threshold: float = 0.6
    ) -> dict:
        """
        Query across multiple cases and merge results.
        
        Args:
            case_ids: List of case IDs to search
            query_text: Natural language query
            n_results: Max results per case
            where: Metadata filter
            threshold: Distance threshold (results with distance > threshold are ignored)
        
        Returns:
            Merged results sorted by distance (best first)
        """
        all_results = {"ids": [], "documents": [], "metadatas": [], "distances": []}
        
        # ⚡ Bolt Optimization: Pre-compute text embedding once for all cases
        # Avoids calling the SentenceTransformer model N times for N cases
        query_embeddings = None
        if query_text:
            query_embeddings = self._embedder.encode(
                [query_text], normalize_embeddings=True
            ).astype(np.float32).tolist()

        for case_id in case_ids:
            try:
                results = self.query(
                    case_id,
                    query_text,
                    n_results,
                    where,
                    query_embeddings=query_embeddings
                )
                
                for i in range(len(results["ids"])):
                    dist = results["distances"][i]
                    if dist <= threshold:
                        all_results["ids"].append(results["ids"][i])
                        all_results["documents"].append(results["documents"][i])
                        all_results["metadatas"].append(results["metadatas"][i])
                        all_results["distances"].append(dist)
            except Exception as e:
                logger.warning(f"Failed to query case '{case_id}': {e}")
        
        # Sort by distance (lower = more similar)
        if all_results["distances"]:
            combined = list(zip(
                all_results["distances"],
                all_results["ids"],
                all_results["documents"],
                all_results["metadatas"]
            ))
            combined.sort(key=lambda x: x[0])
            combined = combined[:n_results]
            
            all_results = {
                "distances": [c[0] for c in combined],
                "ids": [c[1] for c in combined],
                "documents": [c[2] for c in combined],
                "metadatas": [c[3] for c in combined],
            }
        
        return all_results
    
    def delete_case(self, case_id: str) -> bool:
        """Delete a case's entire index."""
        import shutil
        
        deleted = False
        for modality in ["text", "image"]:
            cache_key = f"{case_id}_{modality}"
            if cache_key in self._cache:
                del self._cache[cache_key]
            
            case_dir = self._case_dir(case_id, modality)
            if os.path.exists(case_dir):
                try:
                    shutil.rmtree(case_dir)
                    deleted = True
                    logger.info(f"Deleted FAISS index for case '{case_id}' ({modality})")
                except Exception as e:
                    logger.warning(f"Failed to delete case '{case_id}' ({modality}): {e}")
        
        return deleted
    
    def list_cases(self) -> list[str]:
        """List all indexed case IDs."""
        case_ids = set()
        if not os.path.exists(self.persist_dir):
            return []
        
        for dirname in os.listdir(self.persist_dir):
            full_path = os.path.join(self.persist_dir, dirname)
            if os.path.isdir(full_path):
                # Strip modality suffix
                case_id = dirname
                if case_id.endswith("_image"):
                    case_id = case_id[:-6]
                elif case_id.endswith("_text"):
                    case_id = case_id[:-5]
                
                # Verify it has index files
                meta_path = os.path.join(full_path, "metadata.json")
                if os.path.exists(meta_path):
                    case_ids.add(case_id)
        
        return sorted(case_ids)
    
    def get_case_doc_count(self, case_id: str) -> int:
        """Get document count for a case."""
        try:
            data = self._load_case(case_id)
            return data["index"].ntotal
        except Exception:
            return 0
