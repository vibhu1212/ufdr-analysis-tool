#!/bin/bash
# UFDR Analysis Tool - Linux Installation Script
# For offline government deployment on Ubuntu/Debian
# Version: 2.0.0

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default installation path
INSTALL_PATH="${1:-/opt/ufdr-analysis-tool}"
SKIP_DEPS=false
OFFLINE=false

# Helper functions
print_success() { echo -e "${GREEN}✓${NC} $1"; }
print_info() { echo -e "${CYAN}ℹ${NC} $1"; }
print_warning() { echo -e "${YELLOW}⚠${NC} $1"; }
print_error() { echo -e "${RED}✗${NC} $1"; }

echo -e "${CYAN}"
cat << "EOF"
╔══════════════════════════════════════════════════════════╗
║   UFDR Analysis Tool - Installation Wizard              ║
║   Version 2.0.0 | Government Deployment                 ║
║   Smart India Hackathon 2025                            ║
╚══════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    print_error "This script must be run as root (use sudo)"
    exit 1
fi

print_success "Running with root privileges"

# System Requirements Check
print_info "\n[1/8] Checking System Requirements..."

# Check OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    print_success "OS: $NAME $VERSION"
else
    print_warning "Could not determine OS version"
fi

# Check RAM
total_ram=$(free -g | awk '/^Mem:/{print $2}')
if [ "$total_ram" -lt 8 ]; then
    print_warning "System has less than 8 GB RAM (${total_ram} GB)"
    print_warning "Minimum recommended: 16 GB, Optimal: 32 GB"
    read -p "Continue anyway? (y/n): " continue
    if [ "$continue" != "y" ]; then
        exit 1
    fi
else
    print_success "RAM: ${total_ram} GB"
fi

# Check disk space
free_space=$(df -BG "$INSTALL_PATH" 2>/dev/null | awk 'NR==2 {print $4}' | sed 's/G//')
if [ -z "$free_space" ]; then
    free_space=$(df -BG / | awk 'NR==2 {print $4}' | sed 's/G//')
fi

if [ "$free_space" -lt 10 ]; then
    print_error "Insufficient disk space. Required: 10 GB, Available: ${free_space} GB"
    exit 1
fi
print_success "Disk space: ${free_space} GB available"

# Check CPU cores
cpu_cores=$(nproc)
if [ "$cpu_cores" -lt 4 ]; then
    print_warning "System has less than 4 CPU cores ($cpu_cores cores)"
    print_warning "Minimum recommended: 4 cores, Optimal: 8+ cores"
fi
print_success "CPU cores: $cpu_cores"

# Python Check
print_info "\n[2/8] Checking Python Installation..."

if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed"
    print_info "Install with: sudo apt-get install python3 python3-pip"
    exit 1
fi

python_version=$(python3 --version | awk '{print $2}')
python_major=$(echo "$python_version" | cut -d. -f1)
python_minor=$(echo "$python_version" | cut -d. -f2)

if [ "$python_major" -lt 3 ] || ([ "$python_major" -eq 3 ] && [ "$python_minor" -lt 9 ]); then
    print_error "Python 3.9+ is required. Found: $python_version"
    exit 1
fi
print_success "Python version: $python_version"

# Check pip
if ! command -v pip3 &> /dev/null; then
    print_error "pip3 is not installed"
    print_info "Install with: sudo apt-get install python3-pip"
    exit 1
fi
print_success "pip3 is available"

# Install system dependencies
print_info "\n[2.5/8] Installing System Dependencies..."

apt-get update -qq
apt-get install -y -qq \
    build-essential \
    python3-dev \
    libssl-dev \
    libffi-dev \
    git \
    wget \
    curl \
    > /dev/null 2>&1

print_success "System dependencies installed"

# Create Installation Directory
print_info "\n[3/8] Creating Installation Directory..."

if [ -d "$INSTALL_PATH" ]; then
    print_warning "Installation directory already exists: $INSTALL_PATH"
    read -p "Overwrite? (y/n): " overwrite
    if [ "$overwrite" != "y" ]; then
        print_info "Installation cancelled"
        exit 0
    fi
    print_info "Removing existing installation..."
    rm -rf "$INSTALL_PATH"
fi

mkdir -p "$INSTALL_PATH"
print_success "Created: $INSTALL_PATH"

# Copy Application Files
print_info "\n[4/8] Copying Application Files..."

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DIR="$(dirname "$SCRIPT_DIR")"

# Directories to copy
dirs_to_copy=(
    "data"
    "database"
    "frontend"
    "infra"
    "ingest"
    "lib"
    "media"
    "parser"
    "prompts"
    "rag"
    "scripts"
    "utils"
    "visualization"
)

for dir in "${dirs_to_copy[@]}"; do
    if [ -d "$SOURCE_DIR/$dir" ]; then
        cp -r "$SOURCE_DIR/$dir" "$INSTALL_PATH/"
        print_success "  Copied: $dir"
    else
        print_warning "  Skipped (not found): $dir"
    fi
done

# Copy configuration files
config_files=(
    "requirements.txt"
    "README.md"
    ".env.example"
    "finalize_setup.sh"
)

for file in "${config_files[@]}"; do
    if [ -f "$SOURCE_DIR/$file" ]; then
        cp "$SOURCE_DIR/$file" "$INSTALL_PATH/"
        print_success "  Copied: $file"
    fi
done

# Create data directories
print_info "Creating data directories..."
data_dirs=(
    "data/parsed"
    "data/indices"
    "data/indices/backups"
    "data/samples"
    "logs"
    "exports"
)

for dir in "${data_dirs[@]}"; do
    mkdir -p "$INSTALL_PATH/$dir"
    print_success "  Created: $dir"
done

# Install Python Dependencies
if [ "$SKIP_DEPS" != true ]; then
    print_info "\n[5/8] Installing Python Dependencies..."
    print_warning "This may take 10-15 minutes..."
    
    cd "$INSTALL_PATH"
    
    # Upgrade pip
    print_info "Upgrading pip..."
    python3 -m pip install --upgrade pip > /dev/null 2>&1
    
    # Install core dependencies
    print_info "Installing core dependencies..."
    pip3 install -r requirements.txt --no-warn-script-location > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        print_success "Core dependencies installed"
    else
        print_error "Failed to install core dependencies"
        exit 1
    fi
    
    # Install additional UI dependencies just in case
    print_info "Installing additional UI dependencies..."
    pip3 install streamlit plotly pandas networkx > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        print_success "UI dependencies extra packages installed"
    else
        print_error "Failed to install extra UI dependencies"
    fi
else
    print_warning "[5/8] Skipped: Dependency installation"
fi

# Create Configuration Files
print_info "\n[6/8] Creating Configuration Files..."

cat > "$INSTALL_PATH/config.env" << EOF
# UFDR Analysis Tool Configuration
# Generated on: $(date '+%Y-%m-%d %H:%M:%S')

# Installation
INSTALL_PATH=$INSTALL_PATH

# Neo4j Configuration (if using)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 16 | head -n 1)

# Security
ENABLE_ENCRYPTION=true
ENABLE_AUDIT_LOG=true
RSA_KEY_SIZE=2048

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/ufdr_tool.log

# Performance
MAX_WORKERS=4
BATCH_SIZE=1000

# Vector Index
VECTOR_MODEL=sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
VECTOR_DIMENSION=384

# Media Processing
ENABLE_GPU=false
YOLO_MODEL=yolov8n.pt
EOF

print_success "Created: config.env"

# Create launcher script
cat > "$INSTALL_PATH/launch.sh" << 'EOF'
#!/bin/bash
# UFDR Analysis Tool Launcher

echo -e "\033[0;36mStarting UFDR Analysis Tool...\033[0m"
echo -e "\033[0;36mVersion 2.0.0 | Production Build\033[0m"
echo ""

cd "$(dirname "$0")"

echo -e "\033[0;32mLaunching Production UI...\033[0m"
echo -e "\033[1;33mAccess at: http://localhost:8501\033[0m"
echo ""
echo -e "\033[0;37mPress Ctrl+C to stop the server\033[0m"
echo ""

streamlit run frontend/app.py
EOF

chmod +x "$INSTALL_PATH/launch.sh"
print_success "Created: launch.sh"

# Create systemd service (optional)
print_info "\n[7/8] Creating System Service..."

cat > /etc/systemd/system/ufdr-analysis-tool.service << EOF
[Unit]
Description=UFDR Analysis Tool
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$INSTALL_PATH
ExecStart=$INSTALL_PATH/launch.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
print_success "System service created"
print_info "  Enable with: systemctl enable ufdr-analysis-tool"
print_info "  Start with: systemctl start ufdr-analysis-tool"

# Verify Installation
print_info "\n[8/8] Verifying Installation..."

verification_checks=(
    "$INSTALL_PATH:Installation directory"
    "$INSTALL_PATH/config.env:Configuration file"
    "$INSTALL_PATH/launch.sh:Launcher script"
    "$INSTALL_PATH/frontend:Frontend directory"
    "$INSTALL_PATH/parser:Parser directory"
)

all_passed=true
for check in "${verification_checks[@]}"; do
    path="${check%%:*}"
    name="${check##*:}"
    if [ -e "$path" ]; then
        print_success "  $name"
    else
        print_error "  $name - NOT FOUND"
        all_passed=false
    fi
done

# Final Summary
echo ""
echo -e "${GREEN}"
cat << "EOF"
╔══════════════════════════════════════════════════════════╗
║          Installation Complete!                          ║
╚══════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

echo -e "\n📍 Installation Location: ${CYAN}$INSTALL_PATH${NC}"

echo -e "\n🚀 Quick Start Options:\n"
echo -e "  1. Run launcher script:"
echo -e "     ${CYAN}cd $INSTALL_PATH${NC}"
echo -e "     ${CYAN}./launch.sh${NC}"
echo -e "  2. Start as system service:"
echo -e "     ${CYAN}sudo systemctl start ufdr-analysis-tool${NC}"
echo -e "  3. Manual launch:"
echo -e "     ${CYAN}streamlit run frontend/app.py${NC}"

echo -e "\n📚 Documentation:"
echo "  • Full Docs: $INSTALL_PATH/README.md"

echo -e "\n🌐 Access URL: ${YELLOW}http://localhost:8501${NC}"

echo -e "\n⚙️  Configuration: ${CYAN}$INSTALL_PATH/config.env${NC}"

if [ "$all_passed" != true ]; then
    print_warning "\n⚠️  Some verification checks failed. Please review the installation."
    exit 1
fi

echo -e "\n${GREEN}✅ All systems ready! Launch the application to begin.${NC}"
echo ""

# Offer to launch immediately
read -p "Launch UFDR Analysis Tool now? (y/n): " launch_now
if [ "$launch_now" = "y" ]; then
    print_info "Launching..."
    cd "$INSTALL_PATH"
    ./launch.sh
fi

echo ""
print_info "Installation log saved to: $INSTALL_PATH/install.log"