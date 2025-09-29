# 📚 UFDR Analysis Tool - Complete Setup & Development Guide

## 🚀 Quick Start (For Windows Users)

### Step 1: Install Dependencies
```bash
# Option A: Use the setup script (Recommended for Windows)
setup.bat

# Option B: Install manually
pip install -r requirements-simple.txt
```

### Step 2: Generate Test Data
```bash
python run.py setup
python run.py samples
```

### Step 3: Run the Application
```bash
python run.py
```

The application will open in your browser at `http://localhost:8501`

---

## 📋 Detailed Setup Instructions

### 1️⃣ Core Dependencies Installation

```bash
# Upgrade pip first
python -m pip install --upgrade pip

# Install core packages
pip install streamlit pandas numpy faker python-dotenv

# Install data processing
pip install lxml xmltodict phonenumbers python-dateutil

# Install cloud storage support
pip install boto3 azure-storage-blob
```

### 2️⃣ Optional Components

#### FAISS (for fast vector search)
```bash
# Try pip first
pip install faiss-cpu

# If that fails, use conda
conda install -c pytorch faiss-cpu

# Note: The app will work without FAISS using fallback methods
```

#### Neo4j (for knowledge graph)
```bash
# Install Python driver
pip install neo4j

# Download Neo4j Desktop from: https://neo4j.com/download/
# Or use Docker:
docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password123 neo4j:latest
```

### 3️⃣ Environment Configuration

1. Copy the environment template:
```bash
copy .env.template .env
```

2. Edit `.env` file with your settings:
```ini
# For local storage only
STORAGE_PROVIDER=local

# For cloud storage (optional)
STORAGE_PROVIDER=azure
AZURE_CONNECTION_STRING=your_connection_string
```

---

## 🔄 Complete Project Workflow

### Phase 1: Data Preparation ✅
```bash
# Generate sample UFDR files
python data/generate_samples.py

# This creates 3 sample UFDR files:
# - case_crypto_investigation.ufdr
# - case_foreign_contacts.ufdr
# - case_local_fraud.ufdr
```

### Phase 2: Process UFDR File
```bash
# Option A: Use the Web UI
python run.py
# Then upload file through the interface

# Option B: Use CLI
python parser/ingest_cli.py data/samples/case_crypto_investigation.ufdr --case-id CASE001 --operator "Your Name"
```

### Phase 3: Build Search Index
```bash
# Index the processed data
python vector/index_builder.py --case-id CASE001
```

### Phase 4: Query the Data

#### Through Web UI:
1. Open `http://localhost:8501`
2. Go to Query tab
3. Ask questions like:
   - "Show all messages containing cryptocurrency addresses"
   - "List communications with foreign numbers"
   - "Find suspicious activity patterns"

#### Through CLI:
```bash
# Search for crypto addresses
python vector/retriever.py "cryptocurrency bitcoin" --crypto --case-id CASE001

# Use RAG for natural language queries
python nlp/rag_engine.py "What crypto addresses were mentioned?" --case-id CASE001
```

### Phase 5: Knowledge Graph (Optional)
```bash
# Initialize Neo4j schema
python graph/ingest_to_neo4j.py --case-id CASE001 --init-schema

# Ingest data into graph
python graph/ingest_to_neo4j.py --case-id CASE001
```

---

## 🏗️ Project Structure Overview

```
ufdr-analysis-tool/
│
├── 📁 parser/          # UFDR extraction & parsing
│   ├── ufdr_unzip.py   # Extracts with SHA256 verification
│   ├── ufdr_parser.py  # XML streaming parser
│   └── ingest_cli.py   # Unified ingestion pipeline
│
├── 📁 vector/          # Semantic search
│   ├── index_builder.py # Creates search index
│   └── retriever.py    # Searches indexed data
│
├── 📁 nlp/            # AI & Language Processing
│   └── rag_engine.py  # RAG with local LLM support
│
├── 📁 frontend/       # User Interface
│   └── app.py        # Streamlit web application
│
├── 📁 graph/          # Knowledge Graph
│   ├── schema.cypher    # Neo4j schema
│   └── ingest_to_neo4j.py # Graph population
│
└── 📁 backend/        # Infrastructure
    └── cloud_storage.py # Azure/S3 support
```

---

## 🧪 Testing the System

### 1. Test Individual Modules
```bash
python run.py test
```

### 2. Test Data Processing Pipeline
```bash
# Process a sample file
python parser/ingest_cli.py data/samples/case_crypto_investigation.ufdr --case-id TEST001

# Check output
dir data\parsed\TEST001
```

### 3. Test Search Functionality
```bash
# Build index
python vector/index_builder.py --case-id TEST001

# Search
python vector/retriever.py "bitcoin" --case-id TEST001
```

---

## 🔧 Troubleshooting

### Common Issues & Solutions

#### 1. "pandas installation failed"
```bash
# Use pre-built wheel
pip install --only-binary :all: pandas

# Or use conda
conda install pandas
```

#### 2. "FAISS not found"
```bash
# The app works without FAISS
# For better performance, install via conda:
conda install -c pytorch faiss-cpu
```

#### 3. "Neo4j connection failed"
```bash
# Check if Neo4j is running
# Start Neo4j Desktop or Docker container
docker start neo4j
```

#### 4. "Port 8501 already in use"
```bash
# Kill existing Streamlit process
taskkill /F /IM streamlit.exe

# Or use different port
streamlit run frontend/app.py --server.port 8502
```

---

## 📈 Next Development Steps

### Priority 1: Complete Core Features
- [ ] Download and integrate a local LLM model (Mistral/LLaMA)
- [ ] Implement OCR for image processing
- [ ] Add audio transcription

### Priority 2: Enhance Security
- [ ] Add user authentication
- [ ] Implement data encryption
- [ ] Create audit reports

### Priority 3: Performance Optimization
- [ ] Add caching layer
- [ ] Optimize search queries
- [ ] Implement batch processing

### Priority 4: Production Deployment
- [ ] Create Docker containers
- [ ] Set up monitoring
- [ ] Write API documentation

---

## 💡 Tips for Development

1. **Start Small**: Test with sample data first
2. **Use Mock Mode**: RAG engine works without LLM for testing
3. **Check Logs**: Enable verbose mode with `--verbose` flag
4. **Monitor Resources**: This tool can be memory-intensive

---

## 📞 Getting Help

1. Check `PROJECT_PROGRESS.md` for current status
2. Review error logs in `logs/` directory
3. Run diagnostics: `python run.py test`
4. Check documentation in `docs/` folder

---

## 🎯 For SIH 2025 Submission

### Key Points to Highlight:
1. **100% Offline Operation** - No external API calls
2. **Government-Ready Security** - SHA256, audit trails
3. **Fast Processing** - Streaming parsers, optimized search
4. **User-Friendly** - Natural language queries
5. **Scalable** - Cloud storage support

### Demo Preparation:
1. Prepare 2-3 sample UFDR files
2. Show ingestion with SHA256 verification
3. Demonstrate natural language queries
4. Show crypto detection and foreign contact flagging
5. Export forensic report

---

## 🚦 Ready to Go?

Once setup is complete, you should be able to:
- ✅ Upload and process UFDR files
- ✅ Search with natural language
- ✅ Detect patterns (crypto, foreign contacts)
- ✅ Generate reports
- ✅ Maintain forensic integrity

**Good luck with your SIH 2025 project!** 🏆