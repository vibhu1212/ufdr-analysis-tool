"""
UFDR Analysis Tool - Visualization Package

This package contains all visualization modules for the UFDR forensic analysis tool:
- Network visualizations (force-directed layouts, ego networks)
- Timeline analysis (activity heatmaps, communication patterns)
- Geographic visualizations (location mapping)
- Advanced network analysis (hierarchy, evolution)
- Communication pattern analysis
- Anomaly detection visualizations (4 methods)
- Centrality dashboard (key player identification)
- Graph export functionality (CSV, Excel, JSON, reports)

All visualizations generate interactive HTML files using Plotly.
"""

__version__ = "2.0.0"
__author__ = "UFDR Analysis Team"

# Export main classes for easier imports
try:
    from .network_viz import NetworkVisualizer
except ImportError:
    NetworkVisualizer = None

try:
    from .timeline_viz import TimelineVisualizer
except ImportError:
    TimelineVisualizer = None

try:
    from .geo_viz import GeoVisualizer
except ImportError:
    GeoVisualizer = None

try:
    from .advanced_network_viz import AdvancedNetworkAnalyzer
except ImportError:
    AdvancedNetworkAnalyzer = None

try:
    from .communication_patterns_viz import CommunicationPatternAnalyzer
except ImportError:
    CommunicationPatternAnalyzer = None

try:
    from .anomaly_detection_viz import AnomalyDetector
except ImportError:
    AnomalyDetector = None

try:
    from .centrality_dashboard_viz import CentralityDashboard
except ImportError:
    CentralityDashboard = None

try:
    from .graph_export import GraphExporter
except ImportError:
    GraphExporter = None

__all__ = [
    'NetworkVisualizer',
    'TimelineVisualizer',
    'GeoVisualizer',
    'AdvancedNetworkAnalyzer',
    'CommunicationPatternAnalyzer',
    'AnomalyDetector',
    'CentralityDashboard',
    'GraphExporter'
]
