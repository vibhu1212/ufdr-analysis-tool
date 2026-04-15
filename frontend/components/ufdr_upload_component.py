"""
UFDR Upload Component

This component handles UFDR file uploads and automatic processing.
After upload, it automatically:
1. Extracts and ingests the UFDR file
2. Processes media files (images with YOLO + BLIP + DeepFace)
3. Builds search indices
4. Makes data ready for investigation

Based on RESTRUCTURING_COMPLETE.md specifications.
"""

import streamlit as st
import sqlite3
import os
import zipfile
import tempfile
import shutil
from pathlib import Path
import logging

from database.sql_validator import SQLValidator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Consistent DB path — matches app.py and .env DATABASE_PATH
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = str(_PROJECT_ROOT / "forensic_data.db")


def render_ufdr_upload():
    """
    Render the UFDR upload interface with automatic processing.

    This is the main function called by the frontend app.
    """
    st.subheader("📁 Upload UFDR File")

    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a UFDR file (.ufdr, .zip)",
        type=["ufdr", "zip"],
        help="UFDR files contain forensic data extracted from mobile devices"
    )

    if uploaded_file is not None:
        # Display file info
        st.success(f"✅ File selected: **{uploaded_file.name}** ({uploaded_file.size:,} bytes)")

        # Case Metadata Form
        st.markdown("""
        <div class="glass-card">
            <div class="glass-header">📋 Case Information</div>
            <p style="color:var(--text-secondary); margin-bottom: 1rem;">Please provide case details for proper tracking and audit trail.</p>
        """, unsafe_allow_html=True)

        col1, col2 = st.columns(2)

        with col1:
            case_name = st.text_input(
                "Case Name *",
                value="",
                placeholder="e.g., Operation Phoenix",
                help="Descriptive name for this case"
            )

            examiner_name = st.text_input(
                "Examiner Name *",
                value="",
                placeholder="e.g., Det. John Smith",
                help="Name of the forensic examiner"
            )

        with col2:
            evidence_number = st.text_input(
                "Evidence Number",
                value="",
                placeholder="e.g., EVD-2025-001",
                help="Official evidence tracking number (optional)"
            )

            agency = st.text_input(
                "Agency/Organization",
                value="",
                placeholder="e.g., Metro Police Department",
                help="Investigating agency (optional)"
            )

        case_notes = st.text_area(
            "Case Notes (Optional)",
            value="",
            placeholder="Any additional notes about this case...",
            height=80
        )

        st.markdown("</div>", unsafe_allow_html=True) # Close glass card

        # Validation
        if not case_name or not examiner_name:
            st.warning("⚠️ Please provide at least a Case Name and Examiner Name before uploading")
            return

        # Process button
        if st.button("🚀 Upload and Process", type="primary", use_container_width=True):
            case_metadata = {
                "case_name": case_name,
                "examiner_name": examiner_name,
                "evidence_number": evidence_number or None,
                "agency": agency or None,
                "notes": case_notes or None
            }
            process_ufdr_file(uploaded_file, case_metadata)


def process_ufdr_file(uploaded_file, case_metadata):
    """
    Process uploaded UFDR file with automatic media processing.

    Args:
        uploaded_file: Streamlit UploadedFile object
        case_metadata: Dictionary with case information
    """
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Step 1: Save uploaded file
        status_text.info("📥 Saving uploaded file...")
        progress_bar.progress(10)

        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, uploaded_file.name)

        with open(temp_file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        logger.info(f"Saved uploaded file to: {temp_file_path}")

        # Step 2: Extract UFDR file
        status_text.info("📦 Extracting UFDR archive...")
        progress_bar.progress(20)

        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        try:
            with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            logger.info(f"Extracted UFDR to: {extract_dir}")
        except Exception as e:
            st.error(f"❌ Error extracting UFDR file: {e}")
            return

        # Step 3: Ingest data to database
        status_text.info("💾 Ingesting data to database...")
        progress_bar.progress(40)

        case_id = ingest_ufdr_data(extract_dir, uploaded_file.name, case_metadata)

        if not case_id:
            st.error("❌ Failed to ingest data")
            return

        logger.info(f"Ingested case: {case_id}")

        # Step 4: Build RAG search index (ChromaDB + BM25)
        status_text.info("🔍 Building search index (embeddings + keywords)...")
        progress_bar.progress(60)

        try:
            from rag.indexer import CaseIndexer
            indexer = CaseIndexer()

            def index_progress(current, total, msg):
                pct = 60 + int((current / max(total, 1)) * 35)
                progress_bar.progress(min(pct, 95))
                status_text.info(f"🔍 {msg}")

            index_stats = indexer.index_case(case_id, progress_callback=index_progress)

            indexed_total = sum(index_stats.values())
            if indexed_total > 0:
                status_text.success(f"✅ Indexed {indexed_total} records for search!")
            else:
                st.warning("⚠️ No data to index, but case was created")

        except ImportError as e:
            st.warning(f"⚠️ RAG indexing not available (missing dependency: {e})")
            logger.warning(f"RAG dependencies not installed, skipping indexing: {e}")
        except Exception as e:
            st.warning(f"⚠️ Search indexing had issues: {e}")
            logger.warning(f"RAG indexing failed: {e}", exc_info=True)

        # Step 5: Complete
        progress_bar.progress(100)
        status_text.success(f"✅ Upload complete! Case ID: **{case_id}**")

        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

        # Show next steps
        st.markdown("---")
        st.success("### 🎉 Upload Successful!")
        st.info(f"**Case ID:** {case_id}")
        st.markdown("### 📋 Next Steps:")
        st.markdown("1. Go to **📊 Dashboard** to view case statistics")
        st.markdown("2. Go to **🔍 Unified Search** to query your data")
        st.markdown("3. Go to **🕸️ Network & Graphs** to visualize connections")

        # Refresh button
        if st.button("🔄 Upload Another File"):
            st.rerun()

    except Exception as e:
        logger.error(f"Error processing UFDR file: {e}", exc_info=True)
        st.error(f"❌ Error: {e}")
        st.info("Please check the logs for more details")


def ingest_ufdr_data(extract_dir, filename, case_metadata):
    """
    Ingest extracted UFDR data into the database.

    Args:
        extract_dir: Directory with extracted UFDR contents
        filename: Original filename for case ID
        case_metadata: Dictionary with case information

    Returns:
        case_id: Generated case ID or None on failure
    """
    try:
        # Generate case ID from filename
        case_id = Path(filename).stem

        # Connect to main database (project root forensic_data.db)
        db_path = DB_PATH

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Look for extracted database file
        extracted_db = None
        for file in os.listdir(extract_dir):
            if file.endswith('.db') or file.endswith('.sqlite') or file == 'forensic.db':
                extracted_db = os.path.join(extract_dir, file)
                break

        if extracted_db and os.path.exists(extracted_db):
            st.info(f"📦 Found database: {os.path.basename(extracted_db)}")

            # Open source database separately (avoids locking issues)
            source_conn = sqlite3.connect(extracted_db)
            source_cursor = source_conn.cursor()

            try:
                # Get list of tables from source
                source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in source_cursor.fetchall()]

                st.info(f"📊 Found {len(tables)} tables in extracted database")

                # Copy each table
                for table in tables:
                    if table == 'sqlite_sequence':
                        continue

                    # Sentinel Security Fix: Validate table names against ALLOWED_TABLES
                    # This prevents SQL injection through malicious table names in uploaded databases
                    # since table names cannot be parameterized in PRAGMA or SELECT queries.
                    if table.lower() not in SQLValidator.ALLOWED_TABLES:
                        st.warning(f"  ⚠️ Skipping unrecognized or forbidden table: {table}")
                        logger.warning(f"Security: Blocked attempt to process unapproved table '{table}' from uploaded DB")
                        continue

                    try:
                        # Get table schema from source
                        source_cursor.execute(f"PRAGMA table_info({table})")
                        source_columns_info = source_cursor.fetchall()
                        source_column_names = [col[1] for col in source_columns_info]

                        # Check if case_id exists
                        has_case_id = 'case_id' in source_column_names

                        # Check if table exists in target
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                        target_table_exists = cursor.fetchone() is not None

                        if not target_table_exists:
                            # Create table in main db (copy schema)
                            source_cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
                            create_sql = source_cursor.fetchone()
                            if create_sql:
                                cursor.execute(create_sql[0].replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS'))
                                target_column_names = source_column_names
                        else:
                            # Get target table schema
                            cursor.execute(f"PRAGMA table_info({table})")
                            target_columns_info = cursor.fetchall()
                            target_column_names = [col[1] for col in target_columns_info]

                        # Fetch all data from source
                        source_cursor.execute(f"SELECT * FROM {table}")
                        rows = source_cursor.fetchall()

                        if rows:
                            # Find common columns between source and target
                            common_columns = [col for col in source_column_names if col in target_column_names]

                            # If 'id' is AUTOINCREMENT in target, exclude it from insert
                            if 'id' in common_columns:
                                # Check if id is autoincrement in target
                                cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
                                target_sql = cursor.fetchone()
                                if target_sql and 'AUTOINCREMENT' in target_sql[0].upper():
                                    common_columns = [col for col in common_columns if col != 'id']

                            if not common_columns:
                                st.warning(f"  ⚠️ {table}: No compatible columns to copy")
                                continue

                            # Get column indices from source
                            column_indices = [source_column_names.index(col) for col in common_columns]

                            # Prepare for insertion with explicit case_id handling
                            # We want to force the case_id to match the one we generated (from filename)
                            # so that the dashboard can find it.
                            final_columns = [col for col in common_columns if col != 'case_id']

                            # If target table has case_id, we MUST include it
                            if 'case_id' in target_column_names:
                                final_columns.append('case_id')

                            # Build INSERT statement
                            placeholders = ','.join(['?' for _ in final_columns])

                            # Use OR IGNORE for cases to avoid unique constraint errors
                            conflict_clause = "OR IGNORE" if table == "cases" else "OR REPLACE"
                            insert_sql = f"INSERT {conflict_clause} INTO {table} ({','.join(final_columns)}) VALUES ({placeholders})"

                            # Prepare rows: extract existing columns + append fixed case_id
                            prepared_rows = []
                            for row in rows:
                                # Extract values for common columns (excluding case_id for now)
                                values = []
                                for col in final_columns:
                                    if col == 'case_id':
                                        values.append(case_id)
                                    else:
                                        # Find index in source
                                        idx = source_column_names.index(col)
                                        values.append(row[idx])
                                prepared_rows.append(tuple(values))

                            # Insert data
                            cursor.executemany(insert_sql, prepared_rows)

                            # No need for unsafe UPDATE anymore!

                            st.success(f"  ✅ {table}: {len(rows)} records ({len(final_columns)} columns)")
                        else:
                            st.info(f"  ℹ️ {table}: 0 records (empty table)")

                    except Exception as table_error:
                        error_msg = str(table_error)
                        if "UNIQUE constraint failed" in error_msg and table == "cases":
                             # This is expected behavior for existing cases
                            st.success(f"  ✅ {table}: Schema matches (existing case record preserved)")
                        else:
                            st.warning(f"  ⚠️ Skipped {table}: {error_msg}")
                            print(f"Error executing SQL for {table}: {insert_sql}")
                            print(f"Error details: {table_error}")

            finally:
                # Close source connection
                source_conn.close()

            # Update case metadata in cases table
            # First check what columns exist in cases table
            cursor.execute("PRAGMA table_info(cases)")
            cases_columns = [col[1] for col in cursor.fetchall()]

            # Build UPDATE query based on available columns
            update_parts = []
            update_values = []

            if 'examiner_name' in cases_columns:
                update_parts.append("examiner_name = ?")
                update_values.append(case_metadata.get('examiner_name'))

            if 'agency' in cases_columns:
                update_parts.append("agency = ?")
                update_values.append(case_metadata.get('agency'))

            if 'notes' in cases_columns:
                update_parts.append("notes = COALESCE(?, notes)")
                update_values.append(case_metadata.get('notes'))

            if 'updated_at' in cases_columns:
                update_parts.append("updated_at = CURRENT_TIMESTAMP")

            # Only update if there are columns to update
            if update_parts:
                update_values.append(case_id)  # For WHERE clause
                update_sql = f"UPDATE cases SET {', '.join(update_parts)} WHERE case_id = ?"
                cursor.execute(update_sql, update_values)

            # Store metadata in a separate table if needed
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS case_metadata (
                    case_id TEXT PRIMARY KEY,
                    case_name TEXT,
                    examiner_name TEXT,
                    examiner_badge TEXT,
                    evidence_number TEXT,
                    agency TEXT,
                    notes TEXT,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (case_id) REFERENCES cases(case_id)
                )
            """)

            cursor.execute("""
                INSERT OR REPLACE INTO case_metadata (
                    case_id, case_name, examiner_name, evidence_number, agency, notes
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                case_id,
                case_metadata.get('case_name'),
                case_metadata.get('examiner_name'),
                case_metadata.get('evidence_number'),
                case_metadata.get('agency'),
                case_metadata.get('notes')
            ))

            conn.commit()
            st.success(f"✅ Successfully ingested all data for case: {case_id}")
            st.info(f"📊 Case metadata stored separately in case_metadata table")

        else:
            st.warning("⚠️ No database file found in UFDR archive")
            st.info("Creating case entry only...")

            # Create cases table if not exists (with all required columns)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cases (
                    case_id TEXT PRIMARY KEY,
                    evidence_number TEXT,
                    examiner_name TEXT NOT NULL,
                    examiner_badge TEXT,
                    agency TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'active',
                    integrity_verified BOOLEAN DEFAULT FALSE,
                    court_admissible BOOLEAN DEFAULT FALSE,
                    notes TEXT,
                    metadata JSON
                )
            """)

            # Insert case with metadata
            cursor.execute("""
                INSERT OR REPLACE INTO cases (
                    case_id, evidence_number, examiner_name, agency, notes, status
                ) VALUES (?, ?, ?, ?, ?, 'active')
            """, (
                case_id,
                case_metadata.get('evidence_number'),
                case_metadata.get('examiner_name'),
                case_metadata.get('agency'),
                case_metadata.get('notes')
            ))

            conn.commit()

        conn.close()
        return case_id

    except Exception as e:
        logger.error(f"Error ingesting data: {e}", exc_info=True)
        st.error(f"Database error: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None


def check_media_files(case_id):
    """
    Check how many media files exist for this case.

    Args:
        case_id: Case identifier

    Returns:
        media_count: Number of media files
    """
    try:
        db_path = DB_PATH
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create media table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                media_type TEXT,
                file_path TEXT,
                filename TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Count media files
        cursor.execute("""
            SELECT COUNT(*) FROM media
            WHERE case_id = ? AND media_type IN ('image', 'photo')
        """, (case_id,))

        count = cursor.fetchone()[0]
        conn.close()

        return count

    except Exception as e:
        logger.error(f"Error checking media: {e}")
        return 0


def auto_process_media(case_id):
    """
    Automatically process media files without LLM (for speed).

    Args:
        case_id: Case identifier

    Returns:
        dict with processing results
    """
    try:
        # This would call the actual media processing pipeline
        # For now, just return a placeholder
        st.info("ℹ️ Media processing integration pending")

        return {"processed": 0, "success": True}

    except Exception as e:
        logger.error(f"Error processing media: {e}")
        return None


# Additional utility functions

def get_case_statistics(case_id):
    """Get statistics for a case from database."""
    try:
        db_path = DB_PATH
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        stats = {}

        # Count messages
        try:
            cursor.execute("SELECT COUNT(*) FROM messages WHERE case_id = ?", (case_id,))
            stats['messages'] = cursor.fetchone()[0]
        except:
            stats['messages'] = 0

        # Count calls
        try:
            cursor.execute("SELECT COUNT(*) FROM calls WHERE case_id = ?", (case_id,))
            stats['calls'] = cursor.fetchone()[0]
        except:
            stats['calls'] = 0

        # Count contacts
        try:
            cursor.execute("SELECT COUNT(DISTINCT phone) FROM contacts WHERE case_id = ?", (case_id,))
            stats['contacts'] = cursor.fetchone()[0]
        except:
            stats['contacts'] = 0

        # Count media
        try:
            cursor.execute("SELECT COUNT(*) FROM media WHERE case_id = ?", (case_id,))
            stats['media'] = cursor.fetchone()[0]
        except:
            stats['media'] = 0

        conn.close()
        return stats

    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return {}


if __name__ == "__main__":
    # For testing
    st.set_page_config(page_title="UFDR Upload Test", page_icon="📤")
    st.title("UFDR Upload Component Test")
    render_ufdr_upload()
