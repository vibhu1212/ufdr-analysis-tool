"""
Case Indexer for UFDR Analysis Tool

After uploading a UFDR file and ingesting into SQLite, this module:
1. Reads all records for a case from SQLite
2. Chunks them into searchable text documents
3. Embeds and stores in ChromaDB (local)
4. Builds a BM25 keyword index (local)

ALL OFFLINE — no API key needed for indexing.
"""

import sqlite3
import json
import logging
from pathlib import Path
from typing import Optional, Callable

from rag import DB_PATH, PROJECT_ROOT

from rag.chunker import chunk_records
from rag.faiss_store import FAISSStore
from rag.retriever import BM25Index

logger = logging.getLogger(__name__)

# Tables to index (must match tables in forensic_data.db)
INDEXABLE_TABLES = ["messages", "contacts", "calls", "media", "locations"]
DATA_DIR = PROJECT_ROOT / "data"


class CaseIndexer:
    """
    Indexes a case's data into ChromaDB and BM25 after upload.
    
    Usage:
        indexer = CaseIndexer()
        stats = indexer.index_case("sample_case_001")
        # stats = {"messages": 150, "contacts": 25, ...}
    """
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._store = FAISSStore()
    
    def index_case(
        self,
        case_id: str,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> dict:
        """
        Index all data for a case into ChromaDB and BM25.
        
        Args:
            case_id: Case identifier to index
            progress_callback: Optional fn(current, total, status_msg)
            
        Returns:
            Dict with counts per table: {"messages": 150, "contacts": 25, ...}
        """
        stats = {}
        all_documents = []
        all_metadatas = []
        all_ids = []
        
        total_steps = len(INDEXABLE_TABLES) + 2  # tables + chroma + bm25
        current_step = 0
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        for table in INDEXABLE_TABLES:
            current_step += 1
            if progress_callback:
                progress_callback(current_step, total_steps, f"Reading {table}...")
            
            try:
                # Check if table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,)
                )
                if not cursor.fetchone():
                    logger.info(f"Table '{table}' not found, skipping")
                    stats[table] = 0
                    continue
                
                # Fetch all rows for this case
                cursor.execute(
                    f'SELECT * FROM "{table}" WHERE case_id = ?', (case_id,)
                )
                rows = [dict(row) for row in cursor.fetchall()]
                
                if not rows:
                    stats[table] = 0
                    continue
                
                # Chunk the records
                docs, metas, ids = chunk_records(table, rows, case_id)
                
                all_documents.extend(docs)
                all_metadatas.extend(metas)
                all_ids.extend(ids)
                
                stats[table] = len(docs)
                logger.info(f"Chunked {len(docs)} {table} records for case '{case_id}'")
                
            except Exception as e:
                logger.warning(f"Failed to read '{table}': {e}")
                stats[table] = 0
        
                stats[table] = 0
        
        conn.close()

        # Index Audio Transcripts
        current_step += 1
        if progress_callback:
            progress_callback(current_step, total_steps, "Indexing audio transcripts...")
        audio_count = self._index_audio(case_id)
        stats["audio"] = audio_count
        all_documents.extend(self._audio_docs)
        all_metadatas.extend(self._audio_metas)
        all_ids.extend(self._audio_ids)
        
        # Index Video Content (Text & Images)
        current_step += 1
        if progress_callback:
            progress_callback(current_step, total_steps, "Indexing video content...")
        video_counts = self._index_video(case_id)
        stats.update(video_counts)
        all_documents.extend(self._video_text_docs)
        all_metadatas.extend(self._video_text_metas)
        all_ids.extend(self._video_text_ids)
        
        total_docs = len(all_documents)
        if total_docs == 0 and not self._image_embeddings:
            logger.warning(f"No data found for case '{case_id}'")
            return stats
        
        # Index into ChromaDB
        current_step += 1
        if progress_callback:
            progress_callback(current_step, total_steps, f"Embedding {total_docs} documents...")
        
        try:
            self._store.add_documents(
                case_id=case_id,
                documents=all_documents,
                metadatas=all_metadatas,
                ids=all_ids,
                batch_size=100
            )
        except Exception as e:
            logger.error(f"ChromaDB indexing failed: {e}")
            raise
        
        # Index Images (CLIP Embeddings)
        if self._image_embeddings:
            logger.info(f"Indexing {len(self._image_embeddings)} images for case '{case_id}'")
            try:
                self._store.add_documents(
                    case_id=case_id,
                    documents=self._image_docs, # Descriptions or empty
                    metadatas=self._image_metas,
                    ids=self._image_ids,
                    modality="image",
                    embeddings=self._image_embeddings
                )
            except Exception as e:
                logger.error(f"ChromaDB image indexing failed: {e}")

        
        # Build BM25 index
        current_step += 1
        if progress_callback:
            progress_callback(current_step, total_steps, "Building keyword index...")
        
        try:
            bm25 = BM25Index(case_id)
            bm25.build(all_documents, all_ids, all_metadatas)
        except Exception as e:
            logger.error(f"BM25 indexing failed: {e}")
            # Non-fatal — ChromaDB still works
        
        logger.info(
            f"Indexed case '{case_id}': {total_docs} total documents "
            f"({', '.join(f'{k}={v}' for k, v in stats.items() if v > 0)})"
        )
        
        if progress_callback:
            progress_callback(total_steps, total_steps, "Indexing complete!")
        
        return stats
    
    def is_case_indexed(self, case_id: str) -> bool:
        """Check if a case has been indexed."""
        return self._store.get_case_doc_count(case_id) > 0
    
    def reindex_case(
        self,
        case_id: str,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> dict:
        """Delete existing index and re-index a case."""
        self._store.delete_case(case_id)
        return self.index_case(case_id, progress_callback)
    
    def delete_case_index(self, case_id: str) -> bool:
        """Delete a case's search index."""
        return self._store.delete_case(case_id)

    def _index_audio(self, case_id: str) -> int:
        """Read ASR JSONL and prepare docs."""
        self._audio_docs = []
        self._audio_metas = []
        self._audio_ids = []
        
        asr_file = Path("data/asr_output") / f"{case_id}_asr_results.jsonl"
        if not asr_file.exists():
            return 0
            
        count = 0
        with open(asr_file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if not data.get("transcript"):
                        continue
                        
                    # Index full transcript
                    text = f"Audio Transcript ({data['language']}): {data['transcript']}"
                    doc_id = f"audio_{data['sha256_hash'][:8]}"
                    
                    self._audio_docs.append(text)
                    self._audio_ids.append(doc_id)
                    self._audio_metas.append({
                        "case_id": case_id,
                        "data_type": "audio",
                        "source": data["audio_path"],
                        "timestamp": data["metadata"].get("modified", ""),
                        "confidence": data["confidence"]
                    })
                    count += 1
                except Exception as e:
                    logger.warning(f"Error parsing ASR line: {e}")
        return count

    def _index_video(self, case_id: str) -> dict:
        """Read Video JSONL and prepare docs (text & image)."""
        self._video_text_docs = []
        self._video_text_metas = []
        self._video_text_ids = []
        
        self._image_docs = []
        self._image_metas = []
        self._image_ids = []
        self._image_embeddings = []
        
        stats = {"video_transcripts": 0, "video_images": 0}
        
        video_file = Path("data/video_output") / f"{case_id}_video_results.jsonl"
        if not video_file.exists():
            return stats
            
        with open(video_file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    
                    # 1. Video Audio Transcript
                    if data.get("audio_transcript"):
                        text = f"Video Transcript: {data['audio_transcript']}"
                        doc_id = f"vid_audio_{data['sha256_hash'][:8]}"
                        
                        self._video_text_docs.append(text)
                        self._video_text_ids.append(doc_id)
                        self._video_text_metas.append({
                            "case_id": case_id,
                            "data_type": "video_transcript",
                            "source": data["video_path"],
                            "timestamp": data["metadata"].get("modified", "")
                        })
                        stats["video_transcripts"] += 1
                    
                    # 2. Keyframes (OCR & Embeddings)
                    for frame in data.get("keyframes", []):
                        frame_id = f"frame_{data['sha256_hash'][:8]}_{frame['frame_index']}"
                        
                        # OCR Content (Text Index)
                        if frame.get("ocr_text") and len(frame["ocr_text"]) > 5:
                            text = f"Video Frame OCR ({frame['timestamp']}s): {frame['ocr_text']}"
                            # Add detections to text
                            if frame.get("detections"):
                                text += f" | Detected: {', '.join(frame['detections'])}"
                            
                            self._video_text_docs.append(text)
                            self._video_text_ids.append(f"{frame_id}_ocr")
                            self._video_text_metas.append({
                                "case_id": case_id,
                                "data_type": "video_ocr",
                                "source": data["video_path"],
                                "timestamp": str(frame["timestamp"]),
                                "detections": str(frame.get("detections", []))
                            })
                        
                        # CLIP Embedding (Image Index)
                        if frame.get("embedding"):
                            # Document text describes the image for potential hybrid search or just identification
                            desc = f"Frame at {frame['timestamp']}s"
                            if frame.get("detections"):
                                desc += f" containing {', '.join(frame['detections'])}"
                            
                            self._image_docs.append(desc)
                            self._image_ids.append(f"{frame_id}_img")
                            self._image_embeddings.append(frame["embedding"])
                            self._image_metas.append({
                                "case_id": case_id,
                                "data_type": "video_frame",
                                "source": data["video_path"],
                                "timestamp": str(frame["timestamp"]),
                                "detections": str(frame.get("detections", []))
                            })
                            stats["video_images"] += 1
                            
                except Exception as e:
                    logger.warning(f"Error parsing Video line: {e}")
                    
        return stats
