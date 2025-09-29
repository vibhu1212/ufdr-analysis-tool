"""
Vector Index Builder
Creates FAISS indices for semantic search of forensic artifacts
Supports multilingual embeddings and incremental indexing
"""

import json
import logging
import pickle
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import numpy as np
try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    print("Warning: FAISS not available. Using simple in-memory search.")
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class IndexedDocument:
    """Represents a document in the vector index"""
    doc_id: str
    case_id: str
    artifact_type: str
    content: str
    metadata: Dict
    vector_id: int
    source_file: str
    
    def to_dict(self) -> Dict:
        return asdict(self)


class VectorIndexBuilder:
    """Builds and manages FAISS vector indices for forensic artifacts"""
    
    def __init__(self, 
                 model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                 index_dir: str = "data/indices",
                 dimension: int = 384):
        """
        Initialize vector index builder
        
        Args:
            model_name: Name of sentence transformer model
            index_dir: Directory to store indices
            dimension: Embedding dimension
        """
        self.index_dir = Path(index_dir)
        self.index_dir.mkdir(parents=True, exist_ok=True)
        
        # Load embedding model
        logger.info(f"Loading embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.dimension = dimension
        
        # Initialize or load index
        self.index = None
        self.doc_mapping = {}  # vector_id -> document metadata
        self.next_id = 0
        
        self._load_or_create_index()
    
    def _load_or_create_index(self):
        """Load existing index or create new one"""
        index_path = self.index_dir / "faiss.index"
        mapping_path = self.index_dir / "doc_mapping.pkl"
        
        if index_path.exists() and mapping_path.exists():
            logger.info("Loading existing index...")
            self.index = faiss.read_index(str(index_path))
            
            with open(mapping_path, 'rb') as f:
                data = pickle.load(f)
                self.doc_mapping = data['mapping']
                self.next_id = data['next_id']
            
            logger.info(f"Loaded index with {self.index.ntotal} vectors")
        else:
            logger.info("Creating new index...")
            # Use HNSW index for better recall
            self.index = faiss.IndexHNSWFlat(self.dimension, 32)
            # Set efConstruction for index building
            self.index.hnsw.efConstruction = 200
            self.doc_mapping = {}
            self.next_id = 0
    
    def index_case_artifacts(self, case_id: str, parsed_dir: str = "data/parsed"):
        """
        Index all artifacts for a case
        
        Args:
            case_id: Case identifier
            parsed_dir: Directory containing parsed JSON files
        """
        case_path = Path(parsed_dir) / case_id
        
        if not case_path.exists():
            raise ValueError(f"Case directory not found: {case_path}")
        
        logger.info(f"Indexing artifacts for case: {case_id}")
        
        # Process different artifact types
        artifact_types = ["messages", "calls", "contacts", "media", "locations", "devices"]
        total_indexed = 0
        
        for artifact_type in artifact_types:
            # Find all JSON files for this artifact type
            json_files = list(case_path.glob(f"{artifact_type}_*.json"))
            
            for json_file in json_files:
                count = self._index_json_file(case_id, artifact_type, json_file)
                total_indexed += count
                logger.info(f"Indexed {count} {artifact_type} from {json_file.name}")
        
        # Save index after processing
        self._save_index()
        
        logger.info(f"Total artifacts indexed for case {case_id}: {total_indexed}")
        return total_indexed
    
    def _index_json_file(self, case_id: str, artifact_type: str, json_file: Path) -> int:
        """Index artifacts from a JSON file"""
        with open(json_file, 'r', encoding='utf-8') as f:
            artifacts = json.load(f)
        
        if not artifacts:
            return 0
        
        documents = []
        embeddings = []
        
        for artifact in artifacts:
            # Create searchable content based on artifact type
            content = self._create_searchable_content(artifact_type, artifact)
            
            if not content:
                continue
            
            # Create document
            doc = IndexedDocument(
                doc_id=artifact.get('id', self._generate_doc_id(artifact)),
                case_id=case_id,
                artifact_type=artifact_type,
                content=content,
                metadata=artifact,
                vector_id=self.next_id + len(documents),
                source_file=str(json_file)
            )
            
            documents.append(doc)
        
        if not documents:
            return 0
        
        # Generate embeddings in batch
        contents = [doc.content for doc in documents]
        embeddings = self.model.encode(contents, 
                                      show_progress_bar=False,
                                      convert_to_numpy=True)
        
        # Add to index
        self.index.add(embeddings)
        
        # Update mapping
        for doc in documents:
            self.doc_mapping[doc.vector_id] = doc.to_dict()
            self.next_id += 1
        
        return len(documents)
    
    def _create_searchable_content(self, artifact_type: str, artifact: Dict) -> Optional[str]:
        """Create searchable text content from artifact"""
        content_parts = []
        
        if artifact_type == "messages":
            # Combine sender, recipient, and message text
            if artifact.get('sender'):
                content_parts.append(f"from: {artifact['sender']}")
            if artifact.get('recipient'):
                content_parts.append(f"to: {artifact['recipient']}")
            if artifact.get('text'):
                content_parts.append(artifact['text'])
            if artifact.get('application'):
                content_parts.append(f"app: {artifact['application']}")
                
        elif artifact_type == "calls":
            # Combine caller, callee, and call metadata
            if artifact.get('caller'):
                content_parts.append(f"caller: {artifact['caller']}")
            if artifact.get('callee'):
                content_parts.append(f"callee: {artifact['callee']}")
            if artifact.get('call_type'):
                content_parts.append(f"type: {artifact['call_type']}")
            if artifact.get('duration'):
                content_parts.append(f"duration: {artifact['duration']} seconds")
                
        elif artifact_type == "contacts":
            # Combine name, phone numbers, and emails
            if artifact.get('name'):
                content_parts.append(f"name: {artifact['name']}")
            for phone in artifact.get('phone_numbers', []):
                content_parts.append(f"phone: {phone}")
            for email in artifact.get('emails', []):
                content_parts.append(f"email: {email}")
                
        elif artifact_type == "media":
            # Use filename and metadata
            if artifact.get('filename'):
                content_parts.append(f"file: {artifact['filename']}")
            if artifact.get('mime_type'):
                content_parts.append(f"type: {artifact['mime_type']}")
            # OCR text will be added later
            
        elif artifact_type == "locations":
            # Use address and coordinates
            if artifact.get('address'):
                content_parts.append(artifact['address'])
            if artifact.get('latitude') and artifact.get('longitude'):
                content_parts.append(f"coords: {artifact['latitude']}, {artifact['longitude']}")
                
        elif artifact_type == "devices":
            # Use device information
            if artifact.get('model'):
                content_parts.append(f"model: {artifact['model']}")
            if artifact.get('manufacturer'):
                content_parts.append(f"manufacturer: {artifact['manufacturer']}")
            if artifact.get('imei'):
                content_parts.append(f"imei: {artifact['imei']}")
        
        return " ".join(content_parts) if content_parts else None
    
    def _generate_doc_id(self, artifact: Dict) -> str:
        """Generate unique document ID"""
        content = json.dumps(artifact, sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def search(self, query: str, top_k: int = 10, case_ids: Optional[List[str]] = None) -> List[Dict]:
        """
        Search for similar documents
        
        Args:
            query: Search query
            top_k: Number of results to return
            case_ids: Filter results to specific cases
            
        Returns:
            List of matching documents with scores
        """
        if self.index.ntotal == 0:
            logger.warning("Index is empty")
            return []
        
        # Encode query
        query_vector = self.model.encode([query], convert_to_numpy=True)
        
        # Search index
        distances, indices = self.index.search(query_vector, min(top_k * 2, self.index.ntotal))
        
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx == -1:  # FAISS returns -1 for unfound results
                continue
                
            doc = self.doc_mapping.get(int(idx))
            if not doc:
                continue
            
            # Filter by case if specified
            if case_ids and doc['case_id'] not in case_ids:
                continue
            
            # Add score (convert distance to similarity)
            doc_result = doc.copy()
            doc_result['score'] = float(1 / (1 + dist))  # Convert distance to similarity
            results.append(doc_result)
            
            if len(results) >= top_k:
                break
        
        return results
    
    def add_text_batch(self, texts: List[str], metadata: List[Dict], case_id: str):
        """
        Add a batch of texts to the index
        
        Args:
            texts: List of text content
            metadata: List of metadata dictionaries
            case_id: Case identifier
        """
        if not texts:
            return
        
        # Generate embeddings
        embeddings = self.model.encode(texts, 
                                      show_progress_bar=False,
                                      convert_to_numpy=True)
        
        # Add to index
        self.index.add(embeddings)
        
        # Update mapping
        for text, meta in zip(texts, metadata):
            doc = IndexedDocument(
                doc_id=self._generate_doc_id(meta),
                case_id=case_id,
                artifact_type=meta.get('type', 'text'),
                content=text,
                metadata=meta,
                vector_id=self.next_id,
                source_file=meta.get('source', 'unknown')
            )
            
            self.doc_mapping[self.next_id] = doc.to_dict()
            self.next_id += 1
        
        self._save_index()
    
    def _save_index(self):
        """Save index and mapping to disk"""
        index_path = self.index_dir / "faiss.index"
        mapping_path = self.index_dir / "doc_mapping.pkl"
        
        # Save FAISS index
        faiss.write_index(self.index, str(index_path))
        
        # Save document mapping
        with open(mapping_path, 'wb') as f:
            pickle.dump({
                'mapping': self.doc_mapping,
                'next_id': self.next_id
            }, f)
        
        logger.info(f"Saved index with {self.index.ntotal} vectors")
    
    def get_index_stats(self) -> Dict:
        """Get statistics about the index"""
        stats = {
            'total_vectors': self.index.ntotal if self.index else 0,
            'total_documents': len(self.doc_mapping),
            'dimension': self.dimension,
            'cases': list(set(doc['case_id'] for doc in self.doc_mapping.values()))
        }
        
        # Count by artifact type
        type_counts = {}
        for doc in self.doc_mapping.values():
            artifact_type = doc['artifact_type']
            type_counts[artifact_type] = type_counts.get(artifact_type, 0) + 1
        
        stats['artifact_counts'] = type_counts
        
        return stats


def main():
    """CLI interface for index builder"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Build vector index for UFDR artifacts")
    parser.add_argument("--case-id", required=True, help="Case identifier to index")
    parser.add_argument("--parsed-dir", default="data/parsed", help="Directory with parsed data")
    parser.add_argument("--index-dir", default="data/indices", help="Directory to store indices")
    parser.add_argument("--model", default="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                       help="Sentence transformer model name")
    
    args = parser.parse_args()
    
    # Create index builder
    builder = VectorIndexBuilder(
        model_name=args.model,
        index_dir=args.index_dir
    )
    
    try:
        # Index the case
        count = builder.index_case_artifacts(args.case_id, args.parsed_dir)
        
        # Print stats
        stats = builder.get_index_stats()
        
        print(f"\n✓ Indexing successful!")
        print(f"  Case ID: {args.case_id}")
        print(f"  Artifacts indexed: {count}")
        print(f"  Total vectors: {stats['total_vectors']}")
        print(f"\n  Artifact breakdown:")
        for artifact_type, count in stats.get('artifact_counts', {}).items():
            print(f"    - {artifact_type}: {count}")
        
    except Exception as e:
        print(f"\n✗ Indexing failed: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())