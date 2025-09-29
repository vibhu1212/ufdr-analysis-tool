"""
UFDR Analysis Tool - Main Entry Point
Run this script to start the application
"""

import sys
import os
import subprocess
from pathlib import Path
import argparse
import webbrowser
import time

def check_dependencies():
    """Check if required dependencies are installed"""
    missing = []
    
    # Check Python packages
    required_packages = [
        'streamlit',
        'pandas',
        'faker',
        'lxml',
        'faiss-cpu',
        'sentence_transformers'
    ]
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing.append(package)
    
    if missing:
        print("❌ Missing dependencies detected:")
        for pkg in missing:
            print(f"   - {pkg}")
        print("\n📦 Install with: pip install -r requirements.txt")
        return False
    
    print("✅ All dependencies installed")
    return True

def generate_samples():
    """Generate sample UFDR files for testing"""
    print("📝 Generating sample UFDR files...")
    
    # Run sample generator
    result = subprocess.run(
        [sys.executable, "data/generate_samples.py"],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✅ Sample files generated successfully")
    else:
        print(f"⚠️ Sample generation failed: {result.stderr}")

def start_streamlit():
    """Start the Streamlit application"""
    print("🚀 Starting UFDR Analysis Tool...")
    
    # Set environment variables
    os.environ['STREAMLIT_THEME_PRIMARY_COLOR'] = '#1e3a8a'
    os.environ['STREAMLIT_THEME_BACKGROUND_COLOR'] = '#ffffff'
    os.environ['STREAMLIT_THEME_SECONDARY_BACKGROUND_COLOR'] = '#f0f2f6'
    
    # Start Streamlit
    cmd = [
        sys.executable, "-m", "streamlit", "run",
        "frontend/app.py",
        "--server.port", "8501",
        "--server.headless", "true",
        "--browser.gatherUsageStats", "false"
    ]
    
    process = subprocess.Popen(cmd)
    
    # Wait and open browser
    time.sleep(3)
    webbrowser.open('http://localhost:8501')
    
    print("✅ Application started at http://localhost:8501")
    print("Press Ctrl+C to stop the server")
    
    try:
        process.wait()
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        process.terminate()

def test_modules():
    """Test individual modules"""
    print("🧪 Testing modules...")
    
    tests = []
    
    # Test parser
    try:
        from parser import UFDRExtractor
        tests.append(("Parser", True))
    except Exception as e:
        tests.append(("Parser", False, str(e)))
    
    # Test vector indexing
    try:
        from vector import VectorIndexBuilder
        tests.append(("Vector Index", True))
    except Exception as e:
        tests.append(("Vector Index", False, str(e)))
    
    # Test RAG
    try:
        from nlp.rag_engine import RAGEngine
        tests.append(("RAG Engine", True))
    except Exception as e:
        tests.append(("RAG Engine", False, str(e)))
    
    # Test cloud storage
    try:
        from backend.cloud_storage import create_storage_manager
        storage = create_storage_manager()
        tests.append(("Cloud Storage", True, f"Provider: {storage.provider}"))
    except Exception as e:
        tests.append(("Cloud Storage", False, str(e)))
    
    # Display results
    print("\nModule Test Results:")
    print("-" * 40)
    for test in tests:
        if test[1]:
            status = "✅ PASS"
            info = test[2] if len(test) > 2 else ""
            print(f"{test[0]:<20} {status} {info}")
        else:
            status = "❌ FAIL"
            error = test[2] if len(test) > 2 else ""
            print(f"{test[0]:<20} {status}")
            if error:
                print(f"  Error: {error}")

def setup_environment():
    """Set up environment and directories"""
    print("🔧 Setting up environment...")
    
    # Create necessary directories
    dirs = [
        "data/samples",
        "data/raw",
        "data/parsed",
        "data/indices",
        "data/cache",
        "data/storage",
        "logs",
        "temp"
    ]
    
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    # Check for .env file
    if not Path(".env").exists() and Path(".env.template").exists():
        print("📋 Creating .env from template...")
        import shutil
        shutil.copy(".env.template", ".env")
        print("✅ .env file created - please update with your configuration")
    
    print("✅ Environment setup complete")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="UFDR Analysis Tool - SIH 2025",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=["run", "test", "setup", "samples"],
        help="Command to execute (default: run)"
    )
    
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Don't open browser automatically"
    )
    
    args = parser.parse_args()
    
    print("="*50)
    print(" UFDR Analysis Tool - SIH 2025")
    print(" AI-Based Forensic Analysis System")
    print("="*50)
    print()
    
    if args.command == "setup":
        setup_environment()
        check_dependencies()
    
    elif args.command == "test":
        test_modules()
    
    elif args.command == "samples":
        generate_samples()
    
    elif args.command == "run":
        # Setup and check before running
        setup_environment()
        
        if not check_dependencies():
            print("\n⚠️ Please install missing dependencies before running")
            return 1
        
        # Generate samples if none exist
        samples_dir = Path("data/samples")
        if not list(samples_dir.glob("*.ufdr")):
            generate_samples()
        
        # Start application
        start_streamlit()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())