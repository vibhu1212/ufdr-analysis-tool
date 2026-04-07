"""
UFDR Analysis Tool - Production Frontend
Complete forensic analysis interface with all features integrated

Features:
- Dashboard with case overview and statistics
- UFDR Upload with automatic image processing  
- Unified Query Search with text, image, and hybrid queries
- Network & Timeline Analysis with 8 visualization tabs
- Export capabilities (CSV, Excel, JSON)
- AI-powered reports generation
"""

import streamlit as st
import sys
import os
# Disable ChromaDB telemetry immediately
os.environ["ANONYMIZED_TELEMETRY"] = "False"
from pathlib import Path

# CRITICAL: Add project root to Python path FIRST (before any other imports)
# This ensures backend and visualization modules can be imported

# Get absolute path to project root - works even if Streamlit changes working directory
try:
    # Try to get the absolute path of the current file
    current_file = Path(__file__).resolve()
    # Frontend folder
    frontend_dir = current_file.parent
    # Project root is parent of frontend
    project_root = frontend_dir.parent
except:
    # Fallback: use current working directory
    project_root = Path(os.getcwd()).resolve()

# Add to path if not already there
project_root_str = str(project_root)
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)
else:
    sys.path.remove(project_root_str)
    sys.path.insert(0, project_root_str)

# Also add current working directory as a fallback
cwd = os.getcwd()
if cwd not in sys.path:
    sys.path.insert(1, cwd)


from datetime import datetime
import json
import logging
import sqlite3
from dotenv import load_dotenv

# Load .env file BEFORE anything checks env vars
load_dotenv(project_root / ".env")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress verbose underlying logs and warnings
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("sentence_transformers").setLevel(logging.WARNING)

import warnings
warnings.filterwarnings("ignore")

# ============================================================================
# Main Helper Functions
# ============================================================================

def show_hero_header():
    """Render the premium hero header."""
    st.markdown("""
    <div class="hero-container">
        <div class="hero-glow"></div>
        <div class="hero-content">
            <h1 class="hero-title">UFDR FORENSIC SUITE <span style="color:var(--primary-500)">PRO</span></h1>
            <p class="hero-subtitle">Advanced mobile forensic extraction, analysis, and intelligence platform.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

def show_home_page():
    """Render the modern home dashboard."""
    show_hero_header()
    
    # Feature Grid
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="glass-card">
            <div class="glass-header">📁 Case Management</div>
            <p style="color:var(--text-secondary)">Upload and manage forensic UFDR exports. Automatic parsing and indexing.</p>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown("""
        <div class="glass-card">
            <div class="glass-header">🔍 Deep Search</div>
            <p style="color:var(--text-secondary)">AI-powered semantic search across messages, calls, and media evidence.</p>
        </div>
        """, unsafe_allow_html=True)
        
    with col3:
        st.markdown("""
        <div class="glass-card">
            <div class="glass-header">🕸️ Intelligence Graph</div>
            <p style="color:var(--text-secondary)">Visualize communication networks, communities, and key influencers.</p>
        </div>
        """, unsafe_allow_html=True)

    # Quick Stats (Placeholder or Real)
    st.markdown("### 📊 System Status")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Active Cases", "3", "+1")
    m2.metric("Indexed Items", "12,450", "+540")
    m3.metric("AI Status", "Online",delta="Ready", delta_color="normal") 
    m4.metric("Storage", "45.2 GB", "-1.2%")

# ============================================================================
# Environment Variable Validation
# ============================================================================
# Validate environment configuration on startup
try:
    from utils.env_validator import validate_environment, check_env_file_exists
    
    # Check if .env file exists
    env_exists, env_message = check_env_file_exists()
    if not env_exists and env_message:
        logger.warning(env_message)
    
    # Validate environment variables
    env_valid, env_validation_message = validate_environment()
    
    if not env_valid:
        logger.error("Environment validation failed!")
        logger.error(env_validation_message)
        # Display error in Streamlit UI
        st.error("Configuration Error")
        st.error(env_validation_message)
        st.stop()
    elif env_validation_message:
        # Show warnings if any
        logger.warning(env_validation_message)
        
except ImportError as e:
    logger.warning(f"Could not import environment validator: {e}")
except Exception as e:
    logger.error(f"Error during environment validation: {e}")

# Import visualization modules at top level (after path config)
vis_import_errors = []
try:
    from visualization.network_viz import NetworkVisualizer
    NETWORK_VIZ_AVAILABLE = True
except ImportError as e:
    NetworkVisualizer = None
    NETWORK_VIZ_AVAILABLE = False
    vis_import_errors.append(f"NetworkVisualizer: {e}")

try:
    from visualization.timeline_viz import TimelineVisualizer
    TIMELINE_VIZ_AVAILABLE = True
except ImportError as e:
    TimelineVisualizer = None
    TIMELINE_VIZ_AVAILABLE = False
    vis_import_errors.append(f"TimelineVisualizer: {e}")

try:
    from visualization.geo_viz import GeoVisualizer
    GEO_VIZ_AVAILABLE = True
except ImportError as e:
    GeoVisualizer = None
    GEO_VIZ_AVAILABLE = False
    vis_import_errors.append(f"GeoVisualizer: {e}")

try:
    from visualization.advanced_network_viz import AdvancedNetworkAnalyzer
    ADVANCED_VIZ_AVAILABLE = True
except ImportError as e:
    AdvancedNetworkAnalyzer = None
    ADVANCED_VIZ_AVAILABLE = False
    vis_import_errors.append(f"AdvancedNetworkAnalyzer: {e}")

try:
    from visualization.communication_patterns_viz import CommunicationPatternAnalyzer
    PATTERNS_VIZ_AVAILABLE = True
except ImportError as e:
    CommunicationPatternAnalyzer = None
    PATTERNS_VIZ_AVAILABLE = False
    vis_import_errors.append(f"CommunicationPatternAnalyzer: {e}")

try:
    from visualization.anomaly_detection_viz import AnomalyDetector
    ANOMALY_VIZ_AVAILABLE = True
except ImportError as e:
    AnomalyDetector = None
    ANOMALY_VIZ_AVAILABLE = False
    vis_import_errors.append(f"AnomalyDetector: {e}")

try:
    from visualization.centrality_dashboard_viz import CentralityDashboard
    CENTRALITY_VIZ_AVAILABLE = True
except ImportError as e:
    CentralityDashboard = None
    CENTRALITY_VIZ_AVAILABLE = False
    vis_import_errors.append(f"CentralityDashboard: {e}")

try:
    from visualization.graph_export import GraphExporter
    EXPORT_AVAILABLE = True
except ImportError as e:
    GraphExporter = None
    EXPORT_AVAILABLE = False
    vis_import_errors.append(f"GraphExporter: {e}")

# Only log viz import issues at DEBUG level — these are optional modules
if vis_import_errors:
    logger.debug(f"Optional visualization modules not available: {vis_import_errors}")

# Load Custom CSS - Premium Theme
def load_css():
    css_file = project_root / "frontend" / "assets" / "premium_theme.css"
    if css_file.exists():
        with open(css_file, "r") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    else:
        st.warning("⚠️ Theme file not found")

# Initialize Session State
if 'first_load' not in st.session_state:
    st.session_state.first_load = True

# Page Configuration
st.set_page_config(
    page_title="UFDR Analysis Pro",
    page_icon="🕵️‍♂️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/yourusername/ufdr-analysis-tool',
        'Report a bug': "https://github.com/yourusername/ufdr-analysis-tool/issues",
        'About': "# UFDR Forensic Analysis Tool\nAdvanced extraction and analysis suite."
    }
)

# Load Theme
load_css()

# Loading Animation (Simulated for UX)
if st.session_state.first_load:
    placeholder = st.empty()
    with placeholder.container():
        st.markdown("""
        <div class="loader-container">
            <div class="loader-ring"></div>
            <div class="loading-text">INITIALIZING FORENSIC CORE...</div>
        </div>
        """, unsafe_allow_html=True)
        import time
        time.sleep(1.5)  # UX pause
    placeholder.empty()
    st.session_state.first_load = False

# Main App Container
st.markdown('<div class="main-fade-in">', unsafe_allow_html=True)


# Initialize session state
if 'case_id' not in st.session_state:
    st.session_state.case_id = None
if 'selected_cases' not in st.session_state:
    st.session_state.selected_cases = []
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Dashboard"


def get_db_connection(db_path=None):
    """Get database connection"""
    if db_path is None:
        db_path = project_root / "forensic_data.db"
    try:
        conn = sqlite3.connect(str(db_path))
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


def ensure_db_schema(conn):
    """Create required tables if they don't exist (first-run initialization)."""
    cursor = conn.cursor()
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS cases (
            case_id TEXT PRIMARY KEY,
            ingest_time TEXT,
            source_file TEXT,
            sha256 TEXT,
            examiner TEXT,
            agency TEXT,
            notes TEXT
        );
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            imei TEXT,
            serial_number TEXT,
            manufacturer TEXT,
            model TEXT,
            os_type TEXT,
            os_version TEXT,
            owner TEXT
        );
        CREATE TABLE IF NOT EXISTS contacts (
            contact_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            name TEXT,
            phone_raw TEXT,
            phone_digits TEXT,
            phone_e164 TEXT,
            phone_suffix_2 TEXT,
            phone_suffix_4 TEXT,
            email TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            msg_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            device_id TEXT,
            app TEXT,
            sender_raw TEXT,
            sender_digits TEXT,
            sender_suffix_2 TEXT,
            sender_suffix_4 TEXT,
            receiver_raw TEXT,
            receiver_digits TEXT,
            receiver_suffix_2 TEXT,
            receiver_suffix_4 TEXT,
            text TEXT,
            message_type TEXT,
            timestamp TEXT,
            encrypted INTEGER DEFAULT 0,
            is_deleted INTEGER DEFAULT 0,
            source_path TEXT
        );
        CREATE TABLE IF NOT EXISTS calls (
            call_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            device_id TEXT,
            caller_raw TEXT,
            caller_digits TEXT,
            caller_suffix_2 TEXT,
            caller_suffix_4 TEXT,
            receiver_raw TEXT,
            receiver_digits TEXT,
            receiver_suffix_2 TEXT,
            receiver_suffix_4 TEXT,
            timestamp TEXT,
            duration_seconds INTEGER,
            direction TEXT,
            source_path TEXT
        );
        CREATE TABLE IF NOT EXISTS media (
            media_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            device_id TEXT,
            filename TEXT,
            media_type TEXT,
            sha256 TEXT,
            phash TEXT,
            ocr_text TEXT,
            caption TEXT,
            timestamp TEXT,
            file_size INTEGER,
            source_path TEXT
        );
        CREATE TABLE IF NOT EXISTS locations (
            location_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            device_id TEXT,
            latitude REAL,
            longitude REAL,
            accuracy REAL,
            altitude REAL,
            timestamp TEXT,
            source_path TEXT
        );
    """)
    conn.commit()


def get_case_list():
    """Get list of all cases from database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        ensure_db_schema(conn)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT case_id FROM cases ORDER BY case_id")
        cases = [row[0] for row in cursor.fetchall()]
        return cases
    except Exception as e:
        logger.debug(f"No cases yet: {e}")
        return []
    finally:
        conn.close()


def get_case_statistics(case_id):
    """Get statistics for a specific case"""
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cursor = conn.cursor()
        stats = {}
        
        # Count messages
        cursor.execute("SELECT COUNT(*) FROM messages WHERE case_id = ?", (case_id,))
        stats['messages'] = cursor.fetchone()[0]
        
        # Count calls
        cursor.execute("SELECT COUNT(*) FROM calls WHERE case_id = ?", (case_id,))
        stats['calls'] = cursor.fetchone()[0]
        
        # Count contacts
        cursor.execute("SELECT COUNT(*) FROM contacts WHERE case_id = ?", (case_id,))
        stats['contacts'] = cursor.fetchone()[0]
        
        # Count media
        cursor.execute("SELECT COUNT(*) FROM media WHERE case_id = ?", (case_id,))
        stats['media'] = cursor.fetchone()[0]
        
        # Get date range
        cursor.execute("""
            SELECT MIN(timestamp), MAX(timestamp) 
            FROM (
                SELECT timestamp FROM messages WHERE case_id = ?
                UNION ALL
                SELECT timestamp FROM calls WHERE case_id = ?
            )
        """, (case_id, case_id))
        date_range = cursor.fetchone()
        stats['date_range'] = date_range if date_range[0] else (None, None)
        
        return stats
    except Exception as e:
        logger.error(f"Error fetching case stats: {e}")
        return {}
    finally:
        conn.close()



def render_header():
    """Render main application header - deprecated in favor of hero header"""
    pass


def page_dashboard():
    """Dashboard page with overview and statistics"""
    
    # Get case list
    cases = get_case_list()
    
    if not cases:
        # Show premium home page if no cases
        show_home_page()
        st.info("👋 Upload a UFDR file to get started.")
        return
    
    # Custom dashboard header for active session
    st.markdown("""
    <div class="glass-card">
        <div class="glass-header">📊 Dashboard</div>
        <p style="color:var(--text-secondary)">Case overview, statistics, and platform status.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Case selection
    st.subheader("📁 Case Selection")
    selected_case = st.selectbox(
        "Select a case to view details",
        options=["-- Select a case --"] + cases,
        index=0  # Default to placeholder
    )
    
    # Check if placeholder is selected
    if selected_case == "-- Select a case --":
        show_home_page()
        return
    

    if selected_case:
        st.session_state.case_id = selected_case
        
        # Get statistics
        stats = get_case_statistics(selected_case)
        
        # Display statistics in columns
        st.subheader(f"📈 Case Statistics: {selected_case}")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📱 Messages", f"{stats.get('messages', 0):,}")
        with col2:
            st.metric("📞 Calls", f"{stats.get('calls', 0):,}")
        with col3:
            st.metric("👥 Contacts", f"{stats.get('contacts', 0):,}")
        with col4:
            st.metric("🖼️ Media Files", f"{stats.get('media', 0):,}")
        
        # Date range
        if stats.get('date_range') and stats['date_range'][0]:
            st.info(f"📅 Date Range: {stats['date_range'][0]} to {stats['date_range'][1]}")
        
        # Quick actions
        st.subheader("🚀 Quick Actions")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔍 Start Search", use_container_width=True, help="Go to Unified Search to query evidence using AI"):
                st.session_state.current_page = "Unified Search"
                st.rerun()
        
        with col2:
            if st.button("🕸️ View Network", use_container_width=True, help="Explore communication networks and timeline graphs"):
                st.session_state.current_page = "Network & Graphs"
                st.rerun()
        
        with col3:
            if st.button("📥 Export Data", use_container_width=True, help="Export case data to various formats"):
                st.session_state.current_page = "Network & Graphs"
                st.rerun()
        
        # Recent activity
        st.subheader("📝 Recent Query History")
        if st.session_state.query_history:
            for i, query in enumerate(st.session_state.query_history[-5:]):
                st.markdown(f"{i+1}. {query}")
        else:
            st.info("No queries yet. Go to Unified Search to start investigating!")


def page_upload():
    """UFDR Upload page with automatic processing"""
    st.title("📤 UFDR Upload")
    
    st.markdown("""
    Upload UFDR files for analysis. The system will automatically:
    - Extract and parse the UFDR file
    - Process all media files (images with YOLO, BLIP, DeepFace)
    - Build search indices
    - Make everything ready for investigation
    """)
    
    # Import upload component
    try:
        sys.path.append(str(Path(__file__).parent / "components"))
        from ufdr_upload_component import render_ufdr_upload
        
        # Render the upload component
        render_ufdr_upload()
        
    except ImportError as e:
        st.error(f"Upload component not available: {e}")
        st.info("Please ensure frontend/components/ufdr_upload_component.py exists")
        
        # Fallback basic upload
        st.subheader("📁 Upload UFDR File")
        uploaded_file = st.file_uploader("Choose a UFDR file", type=["ufdr"])
        
        if uploaded_file:
            st.success(f"File uploaded: {uploaded_file.name}")
            st.info("Processing functionality requires the upload component module")


def page_unified_search():
    """Unified Query Search page — ChatGPT-style Interface"""
    # st.title("🔍 Unified Query Search") # Removed title for cleaner look
    
    # Import chat component
    try:
        sys.path.append(str(Path(__file__).parent / "components"))
        from chat_interface import render_chat_interface
    except ImportError:
        st.error("Chat component not found. Please verify frontend/components/chat_interface.py")
        return

    # Top Bar: Context Switcher (Floating style via CSS or just top container)
    with st.container():
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown("### 💬 Forensic Chat Assistant")
        with col2:
            # Case selection
            cases = get_case_list()
            if not cases:
                st.warning("No cases available.")
                return
            
            st.session_state.selected_cases = st.multiselect(
                "Active Cases",
                options=cases,
                default=st.session_state.selected_cases if st.session_state.selected_cases else [],
                label_visibility="collapsed",
                placeholder="Select Case Context..."
            )

    if not st.session_state.selected_cases:
        st.info("👈 Please select a case context to start the chat session.")
        # Optional: Show a "clean slate" or welcome message
        st.markdown("""
        <div style="text-align: center; padding: 4rem; color: var(--text-tertiary);">
            <h2>👋 Welcome to Forensic Chat</h2>
            <p>Select a case above to begin analyzing evidence.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    # Render Chat Interface
    render_chat_interface(st.session_state.selected_cases)




def page_network_graphs():
    """Network & Timeline Analysis page with 8 tabs"""
    # Premium Hero Header
    st.markdown("""
    <div class="page-hero">
        <h1>Network & Timeline Intelligence</h1>
        <p>Visualize communication patterns, relationships, and temporal flows using advanced graph analytics.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Case selection
    cases = get_case_list()
    if not cases:
        st.warning("No cases available. Please upload a UFDR file first.")
        return
    
    # Styled Config Panel for Case Selection
    st.markdown("""
    <div class="config-panel" style="margin-bottom: 2rem;">
        <div class="panel-title">Active Case Context</div>
    </div>
    """, unsafe_allow_html=True)
    
    selected_case = st.selectbox(
        "Select case for analysis",
        options=["-- Select a case --"] + cases,
        index=0,
        label_visibility="collapsed"
    )
    
    # Check if placeholder is selected
    if not selected_case or selected_case == "-- Select a case --":
        st.info("👉 Please select a case from the dropdown above to start analysis")
        return
    
    st.session_state.case_id = selected_case
    
    # Create 8 tabs with scrollable support
    tabs = st.tabs([
        "🕸️ Network",
        "📅 Timeline", 
        "🎯 Ego Net",
        "🗺️ Geo Maps",
        "🔬 Advanced",
        "📞 Patterns",
        "🚨 Anomaly",
        "🎯 Centrality"
    ])
    
    st.markdown('<div class="tab-description">Select a tab above to access specific forensic analysis tools.</div>', unsafe_allow_html=True)
    
    # Check if visualization modules are available (imported at top level)
    if vis_import_errors:
        st.warning("⚠️ Some visualization modules could not be loaded. Check logs for details.")
        with st.expander("See import errors"):
            for error in vis_import_errors:
                st.code(error)
    
    # Tab 1: Network Graph
    with tabs[0]:
        st.markdown("""
        <div class="section-header">
            <div class="icon">
                <svg viewBox="0 0 24 24"><path d="M4 6h16M4 12h16M4 18h16"></path></svg>
            </div>
            <h3>Communication Network</h3>
        </div>
        """, unsafe_allow_html=True)

        if NETWORK_VIZ_AVAILABLE:
            st.markdown("""
            <div class="tab-description">
                <strong>Global Network Analysis:</strong> Visualize relationships between all contacts in the case.
                <div class="feature-pills">
                    <span class="feature-pill blue">Communities</span>
                    <span class="feature-pill purple">Centrality</span>
                    <span class="feature-pill orange">Strength</span>
                    <span class="feature-pill green">Interactive</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Config Panel
            st.markdown('<div class="config-panel"><div class="panel-title">Visualization Parameters</div>', unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            with col1:
                min_interactions = st.slider(
                    "Connection Threshold",
                    min_value=1,
                    max_value=50,
                    value=1,
                    help="Filter edges with fewer interactions. Increase for cleaner network.",
                    key="net_min_inter"
                )
            with col2:
                physics = st.checkbox(
                    "Enable Physics Simulation",
                    value=True,
                    help="Turn OFF for large networks (faster). Turn ON for animated layout.",
                    key="net_physics"
                )
            
            col1, col2 = st.columns(2)
            with col1:
                color_by = st.selectbox(
                    "Color Nodes By",
                    ["community", "centrality", "tier"],
                    help="community = detected groups, centrality = importance (red=high)",
                    key="net_color"
                )
            with col2:
                size_by = st.selectbox(
                    "Size Nodes By",
                    ["degree", "pagerank", "betweenness"],
                    help="Note: Backend currently uses in-degree (incoming connections)",
                    key="net_size"
                )
            st.markdown('</div>', unsafe_allow_html=True) # End Config Panel
            
            # Info about what will happen
            if min_interactions > 10:
                st.info(f"💡 High filter ({min_interactions}) = cleaner network showing only strong connections")
            elif min_interactions <= 2:
                st.warning(f"⚠️ Low filter ({min_interactions}) = may show many nodes. Consider using higher value for large cases.")
            
            if st.button("🎨 Generate Visualization", type="primary", key="net_gen_btn", use_container_width=True):
                with st.spinner("🎨 Generating network graph... This may take 10-30 seconds..."):
                    try:
                        # Use absolute database path
                        db_path = str(project_root / "forensic_data.db")
                        viz = NetworkVisualizer(db_path=db_path)
                        
                        # Call with correct parameters (as per backend signature)
                        html_path = viz.create_communication_network(
                            case_id=selected_case,
                            min_interactions=min_interactions,
                            color_by=color_by,
                            size_by=size_by,  # Note: Backend uses in-degree, but accepts parameter
                            physics=physics,
                            width='100%',
                            height='800px'
                        )
                        
                        if html_path and os.path.exists(html_path):
                            with open(html_path, 'r', encoding='utf-8') as f:
                                html_content = f.read()
                            
                            st.markdown('<div class="analysis-card accent-blue">', unsafe_allow_html=True)
                            st.components.v1.html(html_content, height=800, scrolling=True)
                            st.markdown('</div>', unsafe_allow_html=True)
                            
                            st.success("✅ Network visualization generated!")
                            
                            # Show tips
                            with st.expander("💡 Interpretation Guide"):
                                st.markdown("""
                                **Navigation:**
                                - **Zoom**: Mouse wheel or pinch
                                - **Pan**: Click and drag background
                                - **Select**: Click nodes/edges for details
                                
                                **Visual Coding:**
                                - **Colors**: Different communities (groups that talk frequently)
                                - **Edges**: Red = Strong (50+), Orange = Medium (20-50), Gray = Weak (<20)
                                - **Size**: Larger nodes have more incoming messages/calls
                                """)
                        else:
                            st.error("❌ Failed to generate visualization")
                            st.info("💡 Check if the case has sufficient data (messages/calls)")
                            
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
                        logger.error(f"Network viz error: {e}", exc_info=True)
                        with st.expander("🔧 Debug Info"):
                            st.code(f"Error details: {e}")
                            st.code(f"Case ID: {selected_case}")
                            st.code(f"Min interactions: {min_interactions}")
                            st.code(f"Physics: {physics}")
        else:
            st.warning("⚠️ Network visualizer not available")
            st.info("Check if visualization/network_viz.py exists and imports correctly")
    
    # Tab 2: Timeline
    with tabs[1]:
        st.markdown("""
        <div class="section-header">
            <div class="icon">
                <svg viewBox="0 0 24 24"><path d="M19 4h-1V2h-2v2H8V2H6v2H5c-1.11 0-1.99.9-1.99 2L3 20a2 2 0 0 0 2 2h14c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 16H5V10h14v10zm0-12H5V6h14v2zm-7 5h5v5h-5v-5z"></path></svg>
            </div>
            <h3>Temporal Analysis</h3>
        </div>
        """, unsafe_allow_html=True)
        
        if TIMELINE_VIZ_AVAILABLE:
            st.markdown("""
            <div class="tab-description">
                <strong>Time-Series Analysis:</strong> Analyze patterns of communication and activity over time.
                <div class="feature-pills">
                    <span class="feature-pill blue">Activity Heatmap</span>
                    <span class="feature-pill green">Event Timeline</span>
                    <span class="feature-pill orange">Call Durations</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Config Panel
            st.markdown('<div class="config-panel"><div class="panel-title">Analysis Configuration</div>', unsafe_allow_html=True)
            
            viz_type = st.selectbox(
                "Select visualization type",
                ["Activity Heatmap", "Activity Timeline", "Call Duration Timeline"],
                help="Choose the temporal analysis type",
                key="timeline_type"
            )
            
            # Additional options for Activity Timeline
            if viz_type == "Activity Timeline":
                time_window = st.selectbox(
                    "Time Window",
                    ["day", "hour", "week", "month"],
                    help="Aggregation window for the timeline",
                    key="timeline_window"
                )
            else:
                time_window = "day"
            
            st.markdown('</div>', unsafe_allow_html=True) # End Config Panel
            
            if st.button("📆 Generate Timeline", type="primary", key="timeline_gen_btn", use_container_width=True):
                with st.spinner(f"📊 Generating {viz_type}... Please wait..."):
                    try:
                        # Use absolute database path
                        db_path = str(project_root / "forensic_data.db")
                        viz = TimelineVisualizer(db_path=db_path)
                        
                        # Call correct backend methods
                        if viz_type == "Activity Heatmap":
                            # Backend method: create_heatmap_timeline(case_id)
                            html_path = viz.create_heatmap_timeline(case_id=selected_case)
                        elif viz_type == "Activity Timeline":
                            # Backend method: create_activity_timeline(case_id, time_window)
                            html_path = viz.create_activity_timeline(
                                case_id=selected_case,
                                time_window=time_window
                            )
                        elif viz_type == "Call Duration Timeline":
                            # Backend method: create_call_duration_timeline(case_id)
                            html_path = viz.create_call_duration_timeline(case_id=selected_case)
                        else:
                            html_path = None
                        
                        if html_path and os.path.exists(html_path):
                            with open(html_path, 'r', encoding='utf-8') as f:
                                html_content = f.read()
                            
                            st.markdown('<div class="analysis-card accent-purple">', unsafe_allow_html=True)
                            st.components.v1.html(html_content, height=800, scrolling=True)
                            st.markdown('</div>', unsafe_allow_html=True)
                            
                            st.success("✅ Timeline visualization generated!")
                            
                            # Show interpretation tips
                            with st.expander("💡 Interpretation Guide"):
                                if viz_type == "Activity Heatmap":
                                    st.markdown("""
                                    **Heatmap Interpretation:**
                                    - **Darker colors** = More activity during that day/hour
                                    - **Patterns**: Look for late-night activity or regular schedules
                                    - **Interaction**: Hover over cells for counts
                                    """)
                                elif viz_type == "Activity Timeline":
                                    st.markdown("""
                                    **Timeline Interpretation:**
                                    - **Blue** = Messages, **Red** = Calls, **Green** = Locations
                                    - **Spikes**: Periods of high activity
                                    - **Gaps**: Periods of silence
                                    """)
                                elif viz_type == "Call Duration Timeline":
                                    st.markdown("""
                                    **Call Duration Interpretation:**
                                    - **Bubble size** = Duration (larger = longer)
                                    - **Y-axis** = Duration in minutes
                                    - **Goal**: Identify unusually long calls
                                    """)
                        else:
                            st.error("❌ Failed to generate visualization")
                            st.info("💡 Check if the case has messages/calls with timestamps")
                            
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
                        logger.error(f"Timeline viz error: {e}", exc_info=True)
                        with st.expander("🔧 Debug Info"):
                            st.code(f"Error: {e}")
        else:
            st.warning("⚠️ Timeline visualizer not available")
            st.info("Check if visualization/timeline_viz.py exists and imports correctly")
    
    # Tab 3: Ego Network
    with tabs[2]:
        st.markdown("""
        <div class="section-header">
            <div class="icon">
                <svg viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 3c1.66 0 3 1.34 3 3s-1.34 3-3 3-3-1.34-3-3 1.34-3 3-3zm0 14.2c-2.5 0-4.71-1.28-6-3.22.03-1.99 4-3.08 6-3.08 1.99 0 5.97 1.09 6 3.08-1.29 1.94-3.5 3.22-6 3.22z"></path></svg>
            </div>
            <h3>Ego Network Analysis</h3>
        </div>
        """, unsafe_allow_html=True)

        if NETWORK_VIZ_AVAILABLE:  # Ego network uses NetworkVisualizer
            st.markdown("""
            <div class="tab-description">
                <strong>Individual Contact Analysis:</strong> Explore the direct and indirect connections of a specific target.
                <div class="feature-pills">
                    <span class="feature-pill red">Target-Centric</span>
                    <span class="feature-pill blue">Multi-Hop</span>
                    <span class="feature-pill orange">Hierarchical</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Get contacts for selection - query from messages and calls since contacts may not have phone numbers
            conn = get_db_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    # Get unique phone numbers from messages and calls, with names from contacts if available
                    cursor.execute("""
                        WITH all_phones AS (
                            SELECT DISTINCT sender_digits as phone FROM messages WHERE case_id = ? AND sender_digits IS NOT NULL AND sender_digits != ''
                            UNION
                            SELECT DISTINCT receiver_digits as phone FROM messages WHERE case_id = ? AND receiver_digits IS NOT NULL AND receiver_digits != ''
                            UNION
                            SELECT DISTINCT caller_digits as phone FROM calls WHERE case_id = ? AND caller_digits IS NOT NULL AND caller_digits != ''
                            UNION
                            SELECT DISTINCT receiver_digits as phone FROM calls WHERE case_id = ? AND receiver_digits IS NOT NULL AND receiver_digits != ''
                        )
                        SELECT DISTINCT ap.phone, COALESCE(c.name, 'Unknown') as name
                        FROM all_phones ap
                        LEFT JOIN contacts c ON ap.phone = c.phone_digits AND c.case_id = ?
                        ORDER BY name
                        LIMIT 200
                    """, (selected_case, selected_case, selected_case, selected_case, selected_case))
                    contacts = cursor.fetchall()
                    conn.close()
                    
                    if contacts:
                        # Config Panel
                        st.markdown('<div class="config-panel"><div class="panel-title">Target Selection</div>', unsafe_allow_html=True)
                        
                        # Create display options with name and phone
                        contact_options = [f"{row[1] or 'Unknown'} ({row[0]})" for row in contacts]
                        selected_contact = st.selectbox(
                            "Select contact to analyze",
                            contact_options,
                            help="Choose a contact to see their network connections",
                            key="ego_contact"
                        )
                        
                        # Extract phone number from selection
                        import re
                        phone_match = re.search(r'\(([^)]+)\)$', selected_contact)
                        contact_phone = phone_match.group(1) if phone_match else contacts[0][0]
                        
                        # Only radius parameter (no min_weight in backend)
                        radius = st.slider(
                            "Network Radius (hops)",
                            min_value=1,
                            max_value=3,
                            value=2,
                            help="1 = direct connections only, 2 = friends of friends, 3 = extended network",
                            key="ego_radius"
                        )
                        
                        st.markdown('</div>', unsafe_allow_html=True) # End Config Panel
                        
                        # Info about what radius means
                        if radius == 1:
                            st.info("👥 Radius 1: Shows only direct contacts (1 hop away)")
                        elif radius == 2:
                            st.info("👥 Radius 2: Shows contacts + their contacts (2 hops away)")
                        else:
                            st.info("👥 Radius 3: Shows extended network (3 hops away) - may be large!")
                        
                        if st.button("🎯 Generate Ego Network", type="primary", key="ego_gen_btn", use_container_width=True):
                            with st.spinner(f"🎯 Generating ego network for {contact_phone}... This may take a moment..."):
                                try:
                                    # Use absolute database path
                                    db_path = str(project_root / "forensic_data.db")
                                    viz = NetworkVisualizer(db_path=db_path)
                                    
                                    # Call with CORRECT parameters
                                    html_path = viz.create_ego_network(
                                        case_id=selected_case,
                                        target_phone=contact_phone,
                                        radius=radius
                                    )
                                    
                                    if html_path and os.path.exists(html_path):
                                        with open(html_path, 'r', encoding='utf-8') as f:
                                            html_content = f.read()
                                        
                                        st.markdown('<div class="analysis-card accent-red">', unsafe_allow_html=True)
                                        st.components.v1.html(html_content, height=800, scrolling=True)
                                        st.markdown('</div>', unsafe_allow_html=True)
                                        
                                        st.success("✅ Ego network generated!")
                                        
                                        # Show interpretation tips
                                        with st.expander("💡 Interpretation Guide"):
                                            st.markdown("""
                                            **Nodes:**
                                            - ⭐ **Red Star**: Target contact
                                            - 🔵 **Blue**: Connected contacts
                                            
                                            **Edges:**
                                            - 🔴 **Red**: Strong connection (20+ interactions)
                                            - 🟠 **Orange**: Medium (10-20)
                                            - ⚪ **Gray**: Weak (<10)
                                            """)
                                    else:
                                        st.error("❌ Failed to generate ego network")
                                        st.info("💡 This could mean the contact has no connections in the network")
                                        
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
                                    logger.error(f"Ego network error: {e}", exc_info=True)
                                    with st.expander("🔧 Debug Info"):
                                        st.code(f"Error: {e}")
                    else:
                        st.warning("⚠️ No contacts found in this case")
                        st.info("💡 Make sure the case has contacts with phone numbers")
                except Exception as e:
                    st.error(f"❌ Error loading contacts: {str(e)}")
                    logger.error(f"Contact loading error: {e}", exc_info=True)
            else:
                st.error("❌ Database connection failed")
        else:
            st.warning("⚠️ Ego network visualizer not available")
            st.info("Check if visualization/network_viz.py exists and imports correctly")
    
    # Tab 4: Geographic Maps
    with tabs[3]:
        st.markdown("""
        <div class="section-header">
            <div class="icon">
                <svg viewBox="0 0 24 24"><path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5s1.12-2.5 2.5-2.5 2.5 1.12 2.5 2.5-1.12 2.5-2.5 2.5z"></path></svg>
            </div>
            <h3>Geospatial Intelligence</h3>
        </div>
        """, unsafe_allow_html=True)
        
        if GEO_VIZ_AVAILABLE:
            st.markdown("""
            <div class="tab-description">
                <strong>Location Analysis:</strong> Map movement patterns and identify key locations from metadata.
                <div class="feature-pills">
                    <span class="feature-pill blue">Movement Paths</span>
                    <span class="feature-pill red">Hotspots</span>
                    <span class="feature-pill green">Location Clusters</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Config Panel
            st.markdown('<div class="config-panel"><div class="panel-title">Map Configuration</div>', unsafe_allow_html=True)
            
            # Use same variable names as original to avoid breakage
            viz_type = st.selectbox(
                "Select map type",
                ["Location Points", "Movement Paths", "Location Heatmap"],
                key="geo_type"
            )
            
            st.markdown('</div>', unsafe_allow_html=True) # End Config Panel
            
            if st.button("🗺️ Generate Map", type="primary", key="geo_gen_btn", use_container_width=True):
                with st.spinner(f"🌍 Generating {viz_type}... This may take a moment..."):
                    try:
                        # Use absolute database path
                        db_path = str(project_root / "forensic_data.db")
                        viz = GeoVisualizer(db_path=db_path)
                        
                        html_path = None
                        if viz_type == "Location Points":
                            html_path = viz.create_location_map(case_id=selected_case)
                        elif viz_type == "Movement Paths":
                            html_path = viz.create_movement_paths(case_id=selected_case)
                        elif viz_type == "Location Heatmap":
                            html_path = viz.create_location_heatmap(case_id=selected_case)
                        
                        if html_path and os.path.exists(html_path):
                            with open(html_path, 'r', encoding='utf-8') as f:
                                html_content = f.read()
                            
                            st.markdown('<div class="analysis-card accent-green">', unsafe_allow_html=True)
                            st.components.v1.html(html_content, height=600, scrolling=True)
                            st.markdown('</div>', unsafe_allow_html=True)
                            
                            st.success("✅ Geographic visualization generated!")
                            
                            # Show description
                            with st.expander("💡 Map Description"):
                                if viz_type == "Location Heatmap":
                                    st.markdown("**Heatmap**: Shows density of location points. Red areas indicate high frequency of visits.")
                                elif viz_type == "Location Points": # Was Marker Cluster in my plan, but original code uses "Location Points" -> create_location_map
                                    st.markdown("**Points**: Individual location markers. Click clusters to zoom in.")
                                elif viz_type == "Movement Paths":
                                    st.markdown("**Paths**: Connects location points in chronological order to show movement trajectory.")
                        else:
                            st.error("❌ Failed to generate map")
                            st.info("💡 Check if the case has location data (latitude/longitude columns)")
                            
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
                        logger.error(f"Geo viz error: {e}", exc_info=True)
                        with st.expander("🔧 Debug Info"):
                            st.code(f"Error: {e}")
        else:
            st.warning("⚠️ Geographic visualizer not available")
            st.info("Check if visualization/geo_viz.py exists and imports correctly")
    
    # Tab 5: Advanced Analysis
    with tabs[4]:
        st.markdown("""
        <div class="section-header">
            <div class="icon">
                <svg viewBox="0 0 24 24"><path d="M20 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm-5 14H9v-2h6v2zm0-5H9V9h6v4z"></path></svg>
            </div>
            <h3>Advanced Network Metrics</h3>
        </div>
        """, unsafe_allow_html=True)
        
        if ADVANCED_VIZ_AVAILABLE:
            st.markdown("""
            <div class="tab-description">
                <strong>Structural analysis:</strong> Detect key players and hidden connections.
                <div class="feature-pills">
                    <span class="feature-pill purple">Shortest Path</span>
                    <span class="feature-pill red">Graph Density</span>
                    <span class="feature-pill blue">Clustering Coeff</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Config Panel
            st.markdown('<div class="config-panel"><div class="panel-title">Metric Configuration</div>', unsafe_allow_html=True)
            
            viz_type = st.selectbox(
                "Select analysis type",
                ["Hierarchical Structure", "Network Bridges", "Shortest Paths", "Network Evolution"],
                key="adv_type",
                help="Choose the type of advanced network analysis to perform"
            )
            
            st.markdown('</div>', unsafe_allow_html=True) # End Config Panel
            
            # Show description based on selection in styled cards
            if viz_type == "Hierarchical Structure":
                st.info("🏛️ **Hierarchical Structure**: Identifies leaders (high outbound) vs receivers (high inbound).")
            elif viz_type == "Network Bridges":
                st.info("🌉 **Network Bridges**: Nodes that connect different communities. Critical for information flow.")
            elif viz_type == "Shortest Paths":
                st.info("🛤️ **Shortest Paths**: Most efficient route between central nodes.")
            elif viz_type == "Network Evolution":
                st.info("⏳ **Network Evolution**: How the network grew over time.")
            
            if st.button("🔬 Generate Analysis", type="primary", key="adv_gen_btn", use_container_width=True):
                with st.spinner(f"Analyzing {viz_type.lower()}..."):
                    try:
                        # Use absolute database path
                        db_path = str(project_root / "forensic_data.db")
                        # Original code used AdvancedNetworkAnalyzer? No, likely NetworkVisualizer or specific class.
                        # Wait, Step 5300 line 1151 says: `viz = AdvancedNetworkAnalyzer(db_path=db_path)`
                        # I must use that class name.
                        viz = AdvancedNetworkAnalyzer(db_path=db_path)
                        
                        html_path = None
                        if viz_type == "Hierarchical Structure":
                            html_path = viz.create_hierarchical_visualization(case_id=selected_case)
                        elif viz_type == "Network Bridges":
                            html_path = viz.identify_bridges(case_id=selected_case)
                        elif viz_type == "Shortest Paths":
                            html_path = viz.visualize_shortest_paths(case_id=selected_case)
                        elif viz_type == "Network Evolution":
                            html_path = viz.create_network_evolution(case_id=selected_case)
                        
                        if html_path and os.path.exists(html_path):
                            with open(html_path, 'r', encoding='utf-8') as f:
                                html_content = f.read()
                            
                            st.markdown('<div class="analysis-card accent-purple">', unsafe_allow_html=True)
                            st.components.v1.html(html_content, height=850, scrolling=True)
                            st.markdown('</div>', unsafe_allow_html=True)
                            
                            st.success(f"✅ {viz_type} analysis complete!")
                            
                            # Add interpretation guide (preserving original content)
                            with st.expander("💡 Interpretation Guide"):
                                if viz_type == "Hierarchical Structure":
                                    st.markdown("""
                                    **Understanding the Hierarchy:**
                                    - **Top Layer (Red)**: Leaders/influencers
                                    - **Middle Layer (Orange)**: Coordinators
                                    - **Bottom Layer (Blue)**: Receivers
                                    """)
                                elif viz_type == "Network Bridges":
                                    st.markdown("""
                                    **Understanding Bridges:**
                                    - **Bridge Nodes**: Connect different communities
                                    - **Removal**: Removing these nodes fragments the network
                                    """)
                                elif viz_type == "Shortest Paths":
                                    st.markdown("""
                                    **Understanding Paths:**
                                    - **Red Node**: Source
                                    - **Orange Nodes**: Targets
                                    - **Lines**: Communication hops
                                    """)
                                elif viz_type == "Network Evolution":
                                    st.markdown("""
                                    **Understanding Evolution:**
                                    - **Growth**: How nodes were added over time
                                    - **Activity**: Volume changes
                                    """)
                        else:
                            st.error("❌ Failed to generate analysis.")
                            st.info("💡 This might happen if there's insufficient data.")
                            
                    except Exception as e:
                        st.error(f"❌ Error generating analysis: {e}")
                        logger.error(f"Advanced viz error: {e}", exc_info=True)
        else:
            st.warning("⚠️ Advanced visualizer not available")
            st.info("Check imports")
    
    # Tab 6: Communication Patterns
    with tabs[5]:
        st.markdown("""
        <div class="section-header">
            <div class="icon">
                <svg viewBox="0 0 24 24"><path d="M16 11c1.66 0 2.99-1.34 2.99-3S17.66 5 16 5c-1.66 0-3 1.34-3 3s1.34 3 3 3zm-8 0c1.66 0 2.99-1.34 2.99-3S9.66 5 8 5C6.34 5 5 6.34 5 8s1.34 3 3 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5c0-2.33-4.67-3.5-7-3.5zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z"></path></svg>
            </div>
            <h3>Communication Behavior</h3>
        </div>
        """, unsafe_allow_html=True)
        
        if PATTERNS_VIZ_AVAILABLE:
            st.markdown("""
            <div class="tab-description">
                <strong>Behavior Profiling:</strong> Identify communication habits and unusual patterns.
                <div class="feature-pills">
                    <span class="feature-pill blue">Hourly Heatmap</span>
                    <span class="feature-pill green">Response Time</span>
                    <span class="feature-pill orange">Flow Analysis</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Config Panel
            st.markdown('<div class="config-panel"><div class="panel-title">Pattern Configuration</div>', unsafe_allow_html=True)
            
            viz_type = st.selectbox(
                "Select pattern type",
                ["Peak Hours Heatmap", "Frequency Chart", "Response Times", "Communication Flow"],
                key="pattern_type"
            )
            
            st.markdown('</div>', unsafe_allow_html=True) # End Config Panel
            
            if st.button("📊 Generate Pattern Analysis", type="primary", key="pattern_gen_btn", use_container_width=True):
                with st.spinner(f"Generating {viz_type}..."):
                    try:
                        # Use absolute database path
                        db_path = str(project_root / "forensic_data.db")
                        viz = CommunicationPatternAnalyzer(db_path=db_path)
                        
                        html_path = None
                        if viz_type == "Peak Hours Heatmap":
                            html_path = viz.create_peak_hours_heatmap(case_id=selected_case)
                        elif viz_type == "Frequency Chart":
                            html_path = viz.create_frequency_chart(case_id=selected_case, time_window='day')
                        elif viz_type == "Response Times":
                            html_path = viz.create_response_time_analysis(case_id=selected_case)
                        elif viz_type == "Communication Flow":
                            html_path = viz.create_sankey_diagram(case_id=selected_case, top_n=15)
                        
                        if html_path and os.path.exists(html_path):
                            # Check file size
                            file_size = os.path.getsize(html_path)
                            file_size_mb = file_size / (1024 * 1024)
                            
                            # Provide download button
                            with open(html_path, 'rb') as f:
                                file_data = f.read()
                            
                            # Provide download button for all visualizations
                            st.download_button(
                                label="📥 Download Visualization (HTML)",
                                data=file_data,
                                file_name=os.path.basename(html_path),
                                mime="text/html",
                                key="pattern_download",
                                help="Download to view in browser or save for later"
                            )
                            
                            # Try inline rendering
                            if viz_type == "Communication Flow":
                                st.info("🌊 **Communication Flow (Sankey Diagram)**: Top contacts and their communication patterns")
                            
                            try:
                                with open(html_path, 'r', encoding='utf-8') as f:
                                    html_content = f.read()
                                
                                # Try to render inline
                                st.markdown('<div class="analysis-card accent-orange">', unsafe_allow_html=True)
                                st.components.v1.html(html_content, height=800, scrolling=True)
                                st.markdown('</div>', unsafe_allow_html=True)
                                st.success(f"✅ Pattern analysis complete! (File: {file_size_mb:.2f} MB)")
                                
                            except Exception as render_error:
                                st.warning(f"⚠️ Could not render inline: {render_error}")
                                st.info("💡 Please use the download button above to view the visualization in your browser.")
                                st.success(f"✅ Visualization generated successfully! File: {html_path}")
                            
                            # Add interpretation guide for Sankey
                            if viz_type == "Communication Flow":
                                with st.expander("💡 Understanding the Sankey Diagram"):
                                    st.markdown("""
                                    **What it shows:**
                                    - **Nodes**: Individual contacts (phone numbers/names)
                                    - **Flows**: Communication volume between contacts  
                                    - **Width**: Thicker flows = more communications
                                    - **Direction**: Shows who communicated with whom
                                    
                                    **How to interact:**
                                    - **Hover**: See exact communication counts
                                    - **Drag**: Rearrange nodes for better view
                                    - **Scroll/Pinch**: Zoom in/out
                                    
                                    **Use cases:**
                                    - Identify hub contacts (many connections)
                                    - Find communication clusters
                                    - Discover key intermediaries
                                    - Visualize information flow patterns
                                    
                                    **Note**: If the diagram doesn't display above, use the download button to open in your browser.
                                    """)
                            
                        else:
                            st.error("❌ Failed to generate analysis")
                            
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        with st.expander("🔍 Debug Information"):
                            st.code(str(e))
                        logger.error(f"Pattern viz error: {e}", exc_info=True)
        else:
            st.warning("Pattern visualizer not available")
    
    # Tab 7: Anomaly Detection
    with tabs[6]:
        st.markdown("""
        <div class="section-header">
            <div class="icon">
                <svg viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"></path></svg>
            </div>
            <h3>Anomaly Detection</h3>
        </div>
        """, unsafe_allow_html=True)
        
        if ANOMALY_VIZ_AVAILABLE:
            st.markdown("""
            <div class="tab-description">
                <strong>Threat Analysis:</strong> Detect unusual patterns and suspicious activity automatically.
                <div class="feature-pills">
                    <span class="feature-pill red">Communication Spikes</span>
                    <span class="feature-pill orange">Unusual Contacts</span>
                    <span class="feature-pill purple">Behavior Changes</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Config Panel
            st.markdown('<div class="config-panel"><div class="panel-title">Detection Parameters</div>', unsafe_allow_html=True)
            
            analysis_type = st.selectbox(
                "Select analysis type",
                ["Communication Spikes", "Unusual Contacts", "Behavioral Changes", "Anomaly Dashboard"],
                key="anom_type"
            )
            
            # Interactive parameters based on selection
            threshold_std = 2.0
            min_interactions = 5
            window_days = 7
            
            if analysis_type == "Communication Spikes":
                 threshold_std = st.slider("Sensitivity (Z-Score Threshold)", 1.0, 5.0, 2.0, 0.5, help="Lower value = more sensitive (more alerts)")
            elif analysis_type == "Unusual Contacts":
                 min_interactions = st.slider("Min Interactions Reference", 1, 50, 5, help="Minimum interactions to consider a contact 'normal'")
            elif analysis_type == "Behavioral Changes":
                 window_days = st.slider("Analysis Window (Days)", 3, 30, 7, help="Number of days to compare against history")
            
            st.markdown('</div>', unsafe_allow_html=True) # End Config Panel
            
            if st.button("🔍 Run Detection", type="primary", key="anom_gen_btn", use_container_width=True):
                with st.spinner(f"Running detection for {analysis_type}..."):
                    try:
                        # Use absolute database path
                        db_path = str(project_root / "forensic_data.db")
                        detector = AnomalyDetector(db_path=db_path)
                        
                        html_path = None
                        if analysis_type == "Communication Spikes":
                            html_path = detector.detect_communication_spikes(
                                case_id=selected_case,
                                time_window='day',
                                threshold_std=threshold_std
                            )
                        elif analysis_type == "Unusual Contacts":
                            html_path = detector.detect_unusual_contacts(
                                case_id=selected_case,
                                min_interactions=min_interactions
                            )
                        elif analysis_type == "Behavioral Changes":
                            html_path = detector.detect_behavioral_changes(
                                case_id=selected_case,
                                window_days=window_days
                            )
                        elif analysis_type == "Anomaly Dashboard":
                            html_path = detector.create_anomaly_dashboard(
                                case_id=selected_case
                            )
                        
                        if html_path and os.path.exists(html_path):
                            with open(html_path, 'r', encoding='utf-8') as f:
                                html_content = f.read()
                            
                            st.markdown('<div class="analysis-card accent-red">', unsafe_allow_html=True)
                            st.components.v1.html(html_content, height=1000, scrolling=True)
                            st.markdown('</div>', unsafe_allow_html=True)
                            
                            st.success("✅ Anomaly detection complete!")
                        else:
                            st.error("❌ Failed to generate analysis")
                            st.info("💡 Insufficient data or no anomalies found matching criteria.")
                            
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        logger.error(f"Anomaly detection error: {e}", exc_info=True)
                        with st.expander("🔧 Debug Info"):
                            st.code(f"Error: {str(e)}")
        else:
            st.warning("⚠️ Anomaly detector not available")
            st.info("Check if analysis/anomaly_detection.py exists and imports correctly")
    
    # Tab 8: Centrality Dashboard
    with tabs[7]:
        st.markdown("""
        <div class="section-header">
            <div class="icon">
                <svg viewBox="0 0 24 24"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zM9 17H7v-7h2v7zm4 0h-2V7h2v10zm4 0h-2v-4h2v4z"></path></svg>
            </div>
            <h3>Centrality Dashboard</h3>
        </div>
        """, unsafe_allow_html=True)
        
        if CENTRALITY_VIZ_AVAILABLE:
            st.markdown("""
            <div class="tab-description">
                <strong>Key Player Identification:</strong> Rank entities by influence, control, and connectivity.
                <div class="feature-pills">
                    <span class="feature-pill purple">Influencers</span>
                    <span class="feature-pill red">Brokers</span>
                    <span class="feature-pill blue">Connectors</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Add explanation expander
            with st.expander("💡 Understanding Centrality Metrics", expanded=False):
                st.markdown("""
                ### 📊 Composite Score (Purple Bar)
                
                **What it shows:** Overall importance combining all 5 metrics
                
                **Formula:** Average of (Degree + Betweenness + Closeness + PageRank + Eigenvector)
                
                **Range:** 0.0 (unimportant) to 1.0 (critical)
                
                **Interpretation:**
                - 🔴 **0.80-1.00**: Extremely important → Top priority investigation
                - 🟠 **0.60-0.79**: Very important → Investigate thoroughly
                - 🟡 **0.40-0.59**: Moderately important → Monitor regularly
                - 🟢 **0.20-0.39**: Somewhat important → Background check
                - ⚪ **0.00-0.19**: Peripheral → Minor player
                
                ---
                
                ### 📈 The Five Metrics Explained:
                
                1. **📍 Degree Centrality** (Blue)
                   - Measures: Number of direct connections
                   - High = "Well-connected" or "Popular"
                   - Example: Person who talks to 50 people vs 5 people
                
                2. **🌉 Betweenness Centrality** (Red)
                   - Measures: How often person lies on shortest path between others
                   - High = "Broker" or "Bridge" connecting different groups
                   - Example: Only link between two separate groups
                
                3. **🎯 Closeness Centrality**
                   - Measures: Average distance to all other people
                   - High = "Central" or "Accessible" to everyone
                   - Example: Can reach anyone in 1-2 hops vs 5-6 hops
                
                4. **📊 PageRank** (Green)
                   - Measures: Importance based on who connects to you (Google's algorithm)
                   - High = "Influential" with quality connections
                   - Example: Connected by "bosses" vs "foot soldiers"
                
                5. **⭐ Eigenvector Centrality**
                   - Measures: Connections to well-connected people
                   - High = "Elite" or "Inner circle"
                   - Example: Few connections but to highly-connected people
                
                ---
                
                ### 🎯 Investigation Strategy:
                
                **Start with Top 3 Composite Score contacts:**
                1. Monitor their communications closely
                2. Map their connections (use Ego Network tab)
                3. Analyze temporal patterns (use Timeline tab)
                4. Cross-reference with other cases
                
                **High composite score person is likely:**
                - ✅ Key player / coordinator
                - ✅ Has influence over others
                - ✅ Connects different groups
                - ✅ Can spread information quickly
                - ✅ **Priority target for investigation**
                
                ---
                
                📚 **Full documentation:** See `CENTRALITY_METRICS_EXPLAINED.md`
                """)
            
            st.markdown('<div class="config-panel"><div class="panel-title">Analysis Configuration</div>', unsafe_allow_html=True)
            centrality_type = st.selectbox(
                "Select analysis type",
                ["Centrality Overview", "Metric Comparison Heatmap", "Individual Contact Profile"],
                key="cent_type"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                top_n = st.slider("Top N Contacts", 10, 50, 20, key="cent_topn")
            with col2:
                if centrality_type == "Individual Contact Profile":
                    # Get contact list for selector
                    conn = get_db_connection()
                    if conn:
                        try:
                            cursor = conn.cursor()
                            # Get phone_digits (not phone) and names for display
                            cursor.execute("""
                                SELECT phone_digits, COALESCE(name, phone_raw, phone_digits) as display_name
                                FROM contacts 
                                WHERE case_id = ? AND phone_digits IS NOT NULL AND phone_digits != ''
                                ORDER BY name
                                LIMIT 100
                            """, (selected_case,))
                            contacts_data = cursor.fetchall()
                            conn.close()
                            if contacts_data:
                                # Create display options with name
                                contact_options = {f"{row[1]} ({row[0]})": row[0] for row in contacts_data}
                                selected_display = st.selectbox(
                                    "Select Contact", 
                                    list(contact_options.keys()), 
                                    key="cent_contact"
                                )
                                contact_phone = contact_options[selected_display]
                            else:
                                st.warning("No contacts found with phone numbers")
                                contact_phone = None
                        except Exception as e:
                            st.error(f"Error loading contacts: {e}")
                            logger.error(f"Contact loading error: {e}", exc_info=True)
                            contact_phone = None
                    else:
                        contact_phone = None
            
            st.markdown('</div>', unsafe_allow_html=True) # End Config Panel
            
            if st.button("📊 Generate Analysis", type="primary", key="cent_gen_btn", use_container_width=True):
                with st.spinner(f"Generating {centrality_type}..."):
                    try:
                        # Use absolute database path
                        db_path = str(project_root / "forensic_data.db")
                        
                        # Check if database exists
                        if not os.path.exists(db_path):
                            st.error(f"❌ Database not found: {db_path}")
                            st.info("💡 Please ensure you have uploaded at least one UFDR file")
                        else:
                            dashboard = CentralityDashboard(db_path=db_path)
                            
                            # Validate inputs
                            if centrality_type == "Individual Contact Profile" and not contact_phone:
                                st.error("❌ Please select a contact first")
                                html_path = None
                            else:
                                # Generate visualization
                                if centrality_type == "Centrality Overview":
                                    html_path = dashboard.create_centrality_overview(
                                        case_id=selected_case,
                                        top_n=top_n
                                    )
                                elif centrality_type == "Metric Comparison Heatmap":
                                    html_path = dashboard.create_metric_comparison_heatmap(
                                        case_id=selected_case,
                                        top_n=top_n
                                    )
                                elif centrality_type == "Individual Contact Profile":
                                    html_path = dashboard.create_individual_profile(
                                        case_id=selected_case,
                                        contact_digits=contact_phone  # ✅ Correct parameter name
                                    )
                                else:
                                    html_path = None
                            
                            if html_path and os.path.exists(html_path):
                                with open(html_path, 'r', encoding='utf-8') as f:
                                    html_content = f.read()
                                st.markdown('<div class="analysis-card accent-purple">', unsafe_allow_html=True)
                                st.components.v1.html(html_content, height=1200, scrolling=True)
                                st.markdown('</div>', unsafe_allow_html=True)
                                st.success("✅ Centrality analysis complete!")
                            elif html_path is None:
                                st.error("❌ Failed to generate analysis - No data returned")
                                st.info("💡 **Possible causes:**")
                                st.markdown("""
                                - Case has insufficient network data (needs messages or calls)
                                - Selected contact not found in network
                                - Graph analysis module may have issues
                                """)
                            else:
                                st.error(f"❌ Generated file not found: {html_path}")
                                st.info("💡 Check console logs for detailed error information")
                            
                    except ImportError as e:
                        st.error(f"❌ Required module not available: {e}")
                        st.info("💡 Ensure visualization/centrality_dashboard_viz.py and graph_analytics.py exist")
                        logger.error(f"Centrality import error: {e}", exc_info=True)
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        logger.error(f"Centrality dashboard error: {e}", exc_info=True)
                        with st.expander("🔍 Debug Information"):
                            st.code(f"Error type: {type(e).__name__}")
                            st.code(f"Error message: {str(e)}")
                            st.code(f"Case ID: {selected_case}")
                            st.code(f"Analysis type: {centrality_type}")
                            if centrality_type == "Individual Contact Profile":
                                st.code(f"Selected contact: {contact_phone if 'contact_phone' in locals() else 'None'}")
            
            # Export section
            st.markdown('<div class="config-panel"><div class="panel-title">Export Results</div>', unsafe_allow_html=True)
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("📄 Export CSV", key="cent_export_csv"):
                    try:
                        from visualization.graph_export import GraphExporter
                        db_path = str(project_root / "forensic_data.db")
                        exporter = GraphExporter(db_path=db_path)
                        csv_path = exporter.export_centrality_scores(
                            case_id=selected_case,
                            format='csv',
                            top_n=top_n
                        )
                        if csv_path and os.path.exists(csv_path):
                            with open(csv_path, 'rb') as f:
                                st.download_button(
                                    label="Download CSV",
                                    data=f,
                                    file_name=os.path.basename(csv_path),
                                    mime="text/csv",
                                    key="cent_dl_csv"
                                )
                    except Exception as e:
                        st.error(f"Export failed: {e}")
            
            with col2:
                if st.button("📊 Export Excel", key="cent_export_excel"):
                    try:
                        from visualization.graph_export import GraphExporter
                        db_path = str(project_root / "forensic_data.db")
                        exporter = GraphExporter(db_path=db_path)
                        excel_path = exporter.export_centrality_scores(
                            case_id=selected_case,
                            format='excel',
                            top_n=top_n
                        )
                        if excel_path and os.path.exists(excel_path):
                            with open(excel_path, 'rb') as f:
                                st.download_button(
                                    label="Download Excel",
                                    data=f,
                                    file_name=os.path.basename(excel_path),
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="cent_dl_excel"
                                )
                    except Exception as e:
                        st.error(f"Export failed: {e}")
            
            with col3:
                if st.button("📋 Export JSON", key="cent_export_json"):
                    try:
                        from visualization.graph_export import GraphExporter
                        db_path = str(project_root / "forensic_data.db")
                        exporter = GraphExporter(db_path=db_path)
                        json_path = exporter.export_centrality_scores(
                            case_id=selected_case,
                            format='json',
                            top_n=top_n
                        )
                        if json_path and os.path.exists(json_path):
                            with open(json_path, 'rb') as f:
                                st.download_button(
                                    label="Download JSON",
                                    data=f,
                                    file_name=os.path.basename(json_path),
                                    mime="application/json",
                                    key="cent_dl_json"
                                )
                    except Exception as e:
                        st.error(f"Export failed: {e}")
            
            with col4:
                if st.button("📁 Full Report", key="cent_full_report"):
                    with st.spinner("Generating full investigation report..."):
                        try:
                            from visualization.graph_export import GraphExporter
                            exporter = GraphExporter()
                            report_paths = exporter.create_investigation_report(case_id=selected_case)
                            if report_paths:
                                st.success(f"✅ Generated {len(report_paths)} files!")
                                st.info("Check the exports/ directory for all generated files")
                        except Exception as e:
                            st.error(f"Report generation failed: {e}")
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.warning("Centrality dashboard not available")


def page_cross_case_analysis():
    """Cross-Case Analysis page"""
    # Premium Hero Header
    st.markdown("""
    <div class="page-hero">
        <h1>Cross-Case Intelligence</h1>
        <p>Discover hidden connections, shared entities, and behavioral patterns across multiple cases.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Feature capabilities
    st.markdown("""
    <div class="tab-description">
        <strong>Advanced Analysis:</strong> Detect shared phone numbers, emails, crypto wallets, and behavioral overlaps.
    </div>
    """, unsafe_allow_html=True)
    
    # Multi-case selection
    st.markdown('<div class="config-panel"><div class="panel-title">Case Selection</div>', unsafe_allow_html=True)
    
    all_cases = get_case_list()
    if not all_cases or len(all_cases) < 2:
        st.warning("⚠️ You need at least 2 cases in the database for cross-case analysis")
        st.info("📁 Upload more UFDR files to analyze connections between cases")
    else:
        # Multi-select for cross-case analysis
        cross_case_selection = st.multiselect(
            "Select 2 or more cases to analyze connections",
            options=all_cases,
            default=[],
            help="Select multiple cases to find shared entities and connections",
            key="cross_case_select"
        )
        
        if len(cross_case_selection) < 2:
            st.info("👉 Please select at least 2 cases to find connections")
        else:
            st.success(f"✅ Selected {len(cross_case_selection)} cases for analysis")
            
            # Show selected cases
            with st.expander("📊 View Selected Cases", expanded=False):
                for i, case_id in enumerate(cross_case_selection, 1):
                    stats = get_case_statistics(case_id)
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric(f"Case {i}", case_id)
                    with col2:
                        st.metric("Messages", stats.get('messages', 0))
                    with col3:
                        st.metric("Calls", stats.get('calls', 0))
                    with col4:
                        st.metric("Contacts", stats.get('contacts', 0))
                    st.markdown("---")
            
            # Analysis settings
            st.markdown('</div>', unsafe_allow_html=True) # End Case Selection Panel
            

            
            if st.button("🔍 Analyze Connections", type="primary", use_container_width=True, key="cross_case_analyze_btn"):
                with st.spinner("🧠 AI is analyzing connections between cases... This may take 10-30 seconds..."):
                    try:
                        # Import cross-case analyzer
                        from rag.cross_case_analyzer import get_cross_case_analyzer
                        
                        # Progress bar and status
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        def progress_callback(current, total, message):
                            progress_bar.progress(current / total)
                            status_text.text(f"🔍 {message}")
                        
                        # Initialize analyzer
                        db_path = str(project_root / "forensic_data.db")
                        analyzer = get_cross_case_analyzer(db_path=db_path)
                        
                        # Run analysis
                        result = analyzer.analyze_cross_case_links(
                            case_ids=cross_case_selection,
                            progress_callback=progress_callback
                        )
                        
                        # Clear progress indicators
                        progress_bar.empty()
                        status_text.empty()
                        
                        # Display results
                        if result['success']:
                            st.success(f"✅ Analysis Complete! Found {result['total_connections']} connection(s)")
                            st.info(f"⏱️ Processing time: {result['processing_time']:.1f}s")
                            
                            # AI Summary
                            if result.get('analysis_summary'):
                                st.markdown('<div class="analysis-card accent-purple">', unsafe_allow_html=True)
                                st.markdown("### 🧠 AI Analysis Summary")
                                st.markdown(result['analysis_summary'])
                                st.markdown('</div>', unsafe_allow_html=True)
                            
                            # Connections
                            if result['connections']:
                                st.markdown("---")
                                st.subheader(f"🔗 Connections Found ({len(result['connections'])})")
                                
                                for i, conn in enumerate(result['connections'], 1):
                                    with st.expander(
                                        f"🔗 Connection {i}: {conn['case_1']} ↔️ {conn['case_2']} "
                                        f"(Strength: {conn['connection_strength']:.0%})",
                                        expanded=True
                                    ):
                                        st.markdown('<div class="connection-card">', unsafe_allow_html=True)
                                        st.markdown(f"**Summary:** {conn['summary']}")
                                        st.markdown(f"**Connection Strength:** {conn['connection_strength']:.0%}")
                                        st.markdown(f"**Shared Entities:** {len(conn['shared_entities'])}")
                                        
                                        # Display shared entities
                                        if conn['shared_entities']:
                                            st.markdown("#### 📦 Shared Entities:")
                                            
                                            for entity in conn['shared_entities']:
                                                entity_type = entity['entity_type']
                                                entity_value = entity['entity_value']
                                                confidence = entity['confidence']
                                                context = entity['context']
                                                cases_list = entity['cases']
                                                
                                                # Icon based on entity type
                                                icon = {
                                                    'phone': '📞',
                                                    'email': '📧',
                                                    'crypto_wallet': '💰',
                                                    'name': '👤',
                                                    'device': '📱',
                                                    'location': '📍'
                                                }.get(entity_type, '🔸')
                                                
                                                # Color based on confidence
                                                if confidence >= 0.8:
                                                    confidence_color = '🟢'  # Green
                                                elif confidence >= 0.5:
                                                    confidence_color = '🟡'  # Yellow
                                                else:
                                                    confidence_color = '🔴'  # Red
                                                
                                                st.markdown(
                                                    f"{icon} **{entity_type.replace('_', ' ').title()}:** `{entity_value}`  "
                                                    f"{confidence_color} {confidence:.0%} confidence  "
                                                    f"\n   📁 Found in: {', '.join(cases_list)}  "
                                                    f"\n   📝 Context: {context}"
                                                )
                                                st.markdown("---")
                                        st.markdown('</div>', unsafe_allow_html=True)
                            # Export results
                            st.markdown('<div class="config-panel"><div class="panel-title">Export Results</div>', unsafe_allow_html=True)
                            
                            # Prepare export data
                            export_json = json.dumps(result, indent=2)
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.download_button(
                                    label="💾 Download JSON",
                                    data=export_json,
                                    file_name=f"cross_case_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                    mime="application/json",
                                    use_container_width=True
                                )
                            
                            with col2:
                                # Create markdown report
                                md_report = f"# Cross-Case Analysis Report\n\n"
                                md_report += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                                md_report += f"**Cases Analyzed:** {', '.join(cross_case_selection)}\n\n"
                                md_report += f"**Connections Found:** {result['total_connections']}\n\n"
                                md_report += f"**Processing Time:** {result['processing_time']:.1f}s\n\n"
                                
                                if result.get('analysis_summary'):
                                    md_report += f"## AI Summary\n\n{result['analysis_summary']}\n\n"
                                
                                md_report += f"## Connections\n\n"
                                for i, conn in enumerate(result['connections'], 1):
                                    md_report += f"### Connection {i}: {conn['case_1']} ↔ {conn['case_2']}\n\n"
                                    md_report += f"- **Summary:** {conn['summary']}\n"
                                    md_report += f"- **Strength:** {conn['connection_strength']:.0%}\n"
                                    md_report += f"- **Shared Entities:** {len(conn['shared_entities'])}\n\n"
                                
                                st.download_button(
                                    label="📝 Download Markdown",
                                    data=md_report,
                                    file_name=f"cross_case_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
                                    mime="text/markdown",
                                    use_container_width=True
                                )
                            st.markdown('</div>', unsafe_allow_html=True)
                        else:
                            st.error(f"❌ Analysis failed: {result.get('error', 'Unknown error')}")
                            st.info("💡 **Tips:**")
                            st.markdown("""
                            - Ensure DeepSeek is installed: `ollama pull deepseek-v3.1:671b-cloud`
                            - Or set OPENAI_API_KEY environment variable
                            - Check logs for detailed error information
                            - Ensure cases have overlapping data (contacts, messages, etc.)
                            """)
                    
                    except ImportError as e:
                        st.error(f"❌ Cross-case analysis module not available: {e}")
                        st.info("💡 Ensure backend modules are properly installed")
                    except Exception as e:
                        st.error(f"❌ Error during analysis: {e}")
                        logger.error(f"Cross-case analysis error: {e}", exc_info=True)
                        st.info("📝 Check the console logs for detailed error information")


def main():
    """Main application"""
    render_header()
    
    # Sidebar navigation
    with st.sidebar:
        st.title("📋 Navigation")
        
        pages = {
            "📊 Dashboard": page_dashboard,
            "📤 UFDR Upload": page_upload,
            "🔍 Unified Search": page_unified_search,
            "🕸️ Network & Graphs": page_network_graphs,
            "🔗 Cross-Case Analysis": page_cross_case_analysis
        }
        
        # Page selection
        selected_page = st.radio(
            "Go to",
            options=list(pages.keys()),
            key="page_selector"
        )
        
        st.session_state.current_page = selected_page
        
        st.markdown("---")
        
        # Case info in sidebar
        if st.session_state.case_id:
            st.info(f"**Current Case:** {st.session_state.case_id}")
        

    
    # Render selected page
    if selected_page in pages:
        pages[selected_page]()


if __name__ == "__main__":
    main()
