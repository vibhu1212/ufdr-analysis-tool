#!/bin/bash
# UFDR Analysis Tool - Quick Start Script (macOS)

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Starting UFDR Analysis Tool Setup...${NC}"

# Check for Python 3
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 is not installed or not in PATH.${NC}"
    echo "Please install Python 3.9+ (e.g. using Homebrew: brew install python) and try again."
    exit 1
fi

# Check Python version
python_ver=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
major=$(echo $python_ver | cut -d. -f1)
minor=$(echo $python_ver | cut -d. -f2)

if [ "$major" -lt 3 ] || ([ "$major" -eq 3 ] && [ "$minor" -lt 9 ]); then
    echo -e "${RED}Error: Python 3.9+ is required. Found: $python_ver${NC}"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv venv
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to create virtual environment.${NC}"
        exit 1
    fi
fi

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
echo -e "${YELLOW}Upgrading pip...${NC}"
pip install --upgrade pip

# Install requirements
echo -e "${YELLOW}Installing dependencies... (this may take a few minutes)${NC}"
pip install -r requirements.txt

# Setup .env
if [ ! -f ".env" ] && [ -f ".env.example" ]; then
    echo -e "${YELLOW}Creating default .env file...${NC}"
    cp .env.example .env
fi

# Run the app
echo -e "${GREEN}Starting application...${NC}"
streamlit run frontend/app.py
