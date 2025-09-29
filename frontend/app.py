"""
UFDR Analysis Tool - Streamlit Frontend
MVP interface for investigators to upload and analyze UFDR files
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import json
import sys
import os
from datetime import datetime
import time

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Import our modules
from parser.ingest_cli import UFDRIngestor
from vector.index_builder import VectorIndexBuilder
from vector.retriever import VectorRetriever
from nlp.rag_engine import RAGEngine
from backend.cloud_storage import create_storage_manager

# Configure Streamlit
st.set_page_config(
    page_title="UFDR Analysis Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for government-style interface
st.markdown("""
<style>
    .main {
        padding-top: 2rem;
    }
    .stButton>button {
        background-color: #1e3a8a;
        color: white;
        border-radius: 5px;
    }
    .stButton>button:hover {
        background-color: #1e40af;
    }
    .success-box {
        background-color: #10b981;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .warning-box {
        background-color: #f59e0b;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .metric-card {
        background-color: #f3f4f6;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'case_id' not in st.session_state:
    st.session_state.case_id = None
if 'ingestion_complete' not in st.session_state:
    st.session_state.ingestion_complete = False
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'current_results' not in st.session_state:
    st.session_state.current_results = None


def render_header():
    """Render application header"""
    col1, col2, col3 = st.columns([1, 3, 1])
    with col2:
        st.title("🔍 AI-Based UFDR Analysis Tool")
        st.caption("Universal Forensic Extraction Device Report Analyzer - SIH 2025")
    
    # Show connection status
    with col3:
        storage_manager = create_storage_manager()
        if storage_manager.provider == 'local':
            st.info("📁 Local Storage")
        elif storage_manager.provider == 'azure':
            st.success("☁️ Azure Connected")
        elif storage_manager.provider == 's3':
            st.success("☁️ AWS S3 Connected")


def upload_page():
    """UFDR Upload and Ingestion Page"""
    st.header("📤 Upload UFDR File")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### Upload Evidence File")
        
        # Upload form
        with st.form("upload_form"):
            uploaded_file = st.file_uploader(
                "Select UFDR file (.ufdr or .zip)",
                type=['ufdr', 'zip'],
                help="Upload the UFDR forensic extraction file"
            )
            
            case_id = st.text_input(
                "Case ID",
                placeholder="CASE001",
                help="Unique identifier for this case"
            )
            
            operator = st.text_input(
                "Operator Name",
                placeholder="Inspector Name",
                help="Name of the investigating officer"
            )
            
            cloud_storage = st.checkbox(
                "Upload to Cloud Storage",
                value=True,
                help="Store processed data in configured cloud storage"
            )
            
            submit_button = st.form_submit_button("🚀 Process UFDR")
        
        if submit_button and uploaded_file:
            with st.spinner("Processing UFDR file..."):
                # Save uploaded file temporarily
                temp_path = Path("temp") / uploaded_file.name
                temp_path.parent.mkdir(exist_ok=True)
                
                with open(temp_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                # Initialize progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    # Step 1: Ingest UFDR
                    status_text.text("Extracting and parsing UFDR file...")
                    progress_bar.progress(20)
                    
                    ingestor = UFDRIngestor()
                    manifest = ingestor.ingest(
                        str(temp_path),
                        case_id=case_id,
                        operator=operator
                    )
                    
                    st.session_state.case_id = manifest['case_id']
                    progress_bar.progress(50)
                    
                    # Step 2: Build vector index
                    status_text.text("Building semantic search index...")
                    builder = VectorIndexBuilder()
                    indexed_count = builder.index_case_artifacts(manifest['case_id'])
                    progress_bar.progress(80)
                    
                    # Step 3: Upload to cloud if enabled
                    if cloud_storage:
                        status_text.text("Uploading to cloud storage...")
                        storage = create_storage_manager()
                        
                        # Upload parsed data
                        parsed_dir = Path("data/parsed") / manifest['case_id']
                        for file_path in parsed_dir.glob("**/*.json"):
                            remote_path = f"cases/{manifest['case_id']}/{file_path.name}"
                            storage.upload_file(str(file_path), remote_path)
                    
                    progress_bar.progress(100)
                    status_text.text("Processing complete!")
                    
                    # Show success message
                    st.success(f"✅ Successfully processed UFDR for case: {manifest['case_id']}")
                    
                    # Display statistics
                    st.markdown("### Processing Statistics")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    stats = manifest['parsing']['statistics']
                    with col1:
                        st.metric("Messages", f"{stats.get('messages', 0):,}")
                    with col2:
                        st.metric("Calls", f"{stats.get('calls', 0):,}")
                    with col3:
                        st.metric("Contacts", f"{stats.get('contacts', 0):,}")
                    with col4:
                        st.metric("Locations", f"{stats.get('locations', 0):,}")
                    
                    # Store in session state
                    st.session_state.ingestion_complete = True
                    st.session_state.ingestion_manifest = manifest
                    
                    # Show SHA256 hash for forensic integrity
                    st.info(f"**SHA256 Hash:** {manifest['source_file']['sha256']}")
                    
                except Exception as e:
                    st.error(f"Error processing UFDR: {str(e)}")
                finally:
                    # Cleanup temp file
                    if temp_path.exists():
                        temp_path.unlink()
                    progress_bar.empty()
                    status_text.empty()
    
    with col2:
        st.markdown("### Recent Cases")
        
        # List recent cases
        parsed_dir = Path("data/parsed")
        if parsed_dir.exists():
            cases = [d.name for d in parsed_dir.iterdir() if d.is_dir()]
            
            if cases:
                for case in cases[-5:]:  # Show last 5 cases
                    if st.button(f"📁 {case}", key=f"case_{case}"):
                        st.session_state.case_id = case
                        st.session_state.ingestion_complete = True
                        st.rerun()
            else:
                st.info("No cases processed yet")


def query_page():
    """Natural Language Query Page"""
    st.header("🔎 Query Analysis")
    
    if not st.session_state.case_id:
        st.warning("Please upload a UFDR file first")
        return
    
    st.info(f"**Active Case:** {st.session_state.case_id}")
    
    # Query interface
    col1, col2 = st.columns([3, 1])
    
    with col1:
        query = st.text_input(
            "Enter your question",
            placeholder="e.g., Show me all messages containing crypto addresses",
            help="Ask questions in natural language"
        )
    
    with col2:
        st.markdown("### Quick Queries")
        if st.button("🪙 Find Crypto"):
            query = "Show all messages containing cryptocurrency addresses"
        if st.button("🌍 Foreign Numbers"):
            query = "List all communications with foreign phone numbers"
        if st.button("🕐 Timeline"):
            query = "Show timeline of events"
    
    if query:
        with st.spinner("Analyzing..."):
            # Initialize RAG engine
            rag_engine = RAGEngine()
            
            # Process query
            response = rag_engine.query(
                query,
                case_ids=[st.session_state.case_id],
                top_k=10
            )
            
            # Store in session state
            st.session_state.current_results = response
            st.session_state.query_history.append({
                'query': query,
                'timestamp': datetime.now().isoformat(),
                'confidence': response.confidence
            })
            
            # Display answer
            st.markdown("### Answer")
            st.write(response.answer)
            
            # Show confidence
            confidence_color = "green" if response.confidence > 0.7 else "orange" if response.confidence > 0.4 else "red"
            st.markdown(f"**Confidence:** :{confidence_color}[{response.confidence:.1%}]")
            
            # Show citations
            if response.citations:
                with st.expander("📚 View Sources"):
                    for citation in response.citations:
                        st.markdown(f"**[{citation['reference_id']}]** {citation['source_file']}")
                        st.text(citation['content_preview'][:200] + "...")
                        st.divider()
            
            # Show relevant snippets
            if response.snippets:
                with st.expander("📄 Relevant Snippets"):
                    for i, snippet in enumerate(response.snippets, 1):
                        col1, col2 = st.columns([4, 1])
                        with col1:
                            st.text(snippet.get('content', '')[:300] + "...")
                        with col2:
                            st.metric("Score", f"{snippet.get('score', 0):.3f}")
                        st.divider()
            
            # Processing metadata
            with st.expander("⚙️ Processing Details"):
                st.json(response.query_metadata)
    
    # Query history
    if st.session_state.query_history:
        st.markdown("### Query History")
        history_df = pd.DataFrame(st.session_state.query_history)
        st.dataframe(history_df, use_container_width=True)


def graph_page():
    """Knowledge Graph Visualization Page"""
    st.header("🕸️ Relationship Graph")
    
    if not st.session_state.case_id:
        st.warning("Please upload a UFDR file first")
        return
    
    st.info("Graph visualization coming soon - will show entity relationships using Neo4j")
    
    # Placeholder for graph visualization
    # This would integrate with Neo4j and use Plotly/Cytoscape for visualization
    
    # Sample visualization
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=["Person A", "Person B", "Person C", "WhatsApp", "Telegram", "SMS"],
            color="blue"
        ),
        link=dict(
            source=[0, 0, 1, 2, 3, 4],
            target=[3, 4, 3, 4, 5, 5],
            value=[10, 5, 8, 3, 15, 7]
        )
    )])
    
    fig.update_layout(title_text="Communication Patterns", font_size=10)
    st.plotly_chart(fig, use_container_width=True)


def export_page():
    """Export and Report Generation Page"""
    st.header("📊 Export Reports")
    
    if not st.session_state.case_id:
        st.warning("Please upload a UFDR file first")
        return
    
    st.markdown(f"### Export Options for Case: {st.session_state.case_id}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Investigation Report")
        
        report_type = st.selectbox(
            "Report Type",
            ["Executive Summary", "Detailed Analysis", "Timeline Report", "Entity Report"]
        )
        
        include_graphs = st.checkbox("Include visualizations", value=True)
        include_sources = st.checkbox("Include source citations", value=True)
        
        if st.button("📄 Generate PDF Report"):
            with st.spinner("Generating report..."):
                # This would generate actual PDF using reportlab
                time.sleep(2)
                st.success("Report generated successfully!")
                st.download_button(
                    label="Download PDF",
                    data=b"Sample PDF content",
                    file_name=f"UFDR_Report_{st.session_state.case_id}.pdf",
                    mime="application/pdf"
                )
    
    with col2:
        st.markdown("#### Data Export")
        
        export_format = st.selectbox(
            "Export Format",
            ["JSON", "CSV", "Excel", "XML"]
        )
        
        data_types = st.multiselect(
            "Data Types to Export",
            ["Messages", "Calls", "Contacts", "Locations", "Media"],
            default=["Messages", "Calls"]
        )
        
        if st.button("💾 Export Data"):
            with st.spinner("Preparing export..."):
                time.sleep(1)
                st.success("Data exported successfully!")
                
                # Create sample export data
                export_data = {
                    "case_id": st.session_state.case_id,
                    "export_time": datetime.now().isoformat(),
                    "data_types": data_types
                }
                
                st.download_button(
                    label=f"Download {export_format}",
                    data=json.dumps(export_data, indent=2),
                    file_name=f"UFDR_Export_{st.session_state.case_id}.{export_format.lower()}",
                    mime="application/json"
                )


def settings_page():
    """Settings and Configuration Page"""
    st.header("⚙️ Settings")
    
    tab1, tab2, tab3 = st.tabs(["Storage", "Models", "Security"])
    
    with tab1:
        st.markdown("### Cloud Storage Configuration")
        
        provider = st.selectbox(
            "Storage Provider",
            ["Local", "Azure Blob Storage", "AWS S3"]
        )
        
        if provider == "Azure Blob Storage":
            connection_string = st.text_input(
                "Connection String",
                type="password",
                help="Azure Storage connection string"
            )
            container = st.text_input(
                "Container Name",
                value="ufdr-storage"
            )
        
        elif provider == "AWS S3":
            access_key = st.text_input(
                "Access Key ID",
                type="password"
            )
            secret_key = st.text_input(
                "Secret Access Key",
                type="password"
            )
            bucket = st.text_input(
                "Bucket Name",
                value="ufdr-storage"
            )
            region = st.text_input(
                "Region",
                value="us-east-1"
            )
        
        if st.button("Save Storage Settings"):
            st.success("Storage settings saved!")
    
    with tab2:
        st.markdown("### AI Model Configuration")
        
        st.info("All models run locally for security")
        
        llm_model = st.selectbox(
            "LLM Model",
            ["Mistral 7B (Q4)", "LLaMA 2 7B (Q4)", "Phi-2", "Mock Mode (No LLM)"]
        )
        
        embedding_model = st.selectbox(
            "Embedding Model",
            ["paraphrase-multilingual-MiniLM-L12-v2", "all-MiniLM-L6-v2"]
        )
        
        st.slider("Temperature", 0.0, 1.0, 0.3)
        st.slider("Max Tokens", 128, 1024, 512)
        
        if st.button("Download Models"):
            st.info("Model download functionality would be implemented here")
    
    with tab3:
        st.markdown("### Security Settings")
        
        enable_encryption = st.checkbox("Enable at-rest encryption", value=True)
        enable_audit = st.checkbox("Enable audit logging", value=True)
        require_auth = st.checkbox("Require authentication", value=False)
        
        if require_auth:
            st.text_input("Username")
            st.text_input("Password", type="password")
        
        st.markdown("### Forensic Integrity")
        st.info("SHA256 hashing is always enabled for chain-of-custody")
        
        if st.button("Export Audit Log"):
            st.download_button(
                label="Download Audit Log",
                data="Sample audit log",
                file_name="audit_log.jsonl",
                mime="text/plain"
            )


def main():
    """Main application"""
    render_header()
    
    # Sidebar navigation
    with st.sidebar:
        st.markdown("## Navigation")
        
        page = st.radio(
            "Select Page",
            ["📤 Upload", "🔎 Query", "🕸️ Graph", "📊 Export", "⚙️ Settings"],
            label_visibility="collapsed"
        )
        
        st.divider()
        
        # Case info
        if st.session_state.case_id:
            st.markdown("### Active Case")
            st.info(st.session_state.case_id)
            
            if st.button("🔄 Clear Case"):
                st.session_state.case_id = None
                st.session_state.ingestion_complete = False
                st.rerun()
        
        st.divider()
        
        # System info
        st.markdown("### System Status")
        st.success("✅ All systems operational")
        st.caption("Local LLM: Ready")
        st.caption("Vector Index: Ready")
        st.caption("Graph DB: Ready")
    
    # Route to selected page
    if "Upload" in page:
        upload_page()
    elif "Query" in page:
        query_page()
    elif "Graph" in page:
        graph_page()
    elif "Export" in page:
        export_page()
    elif "Settings" in page:
        settings_page()


if __name__ == "__main__":
    main()