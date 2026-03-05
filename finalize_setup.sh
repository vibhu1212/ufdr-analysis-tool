#!/bin/bash
# ============================================================================
# Finalize Project Setup and Cleanup
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}============================================================================${NC}"
echo -e "${BLUE}Finalizing Project Setup & Cleanup${NC}"
echo -e "${BLUE}============================================================================${NC}"

# 1. Clean __pycache__
echo -e "\n${YELLOW}[1/4] Cleaning compilation artifacts...${NC}"
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type d -name ".pytest_cache" -exec rm -rf {} +
find . -type d -name ".hypothesis" -exec rm -rf {} +
find . -type d -name ".mypy_cache" -exec rm -rf {} +
echo -e "${GREEN}✓ Removed __pycache__ and test caches${NC}"

# 2. Consolidate Environment Config
echo -e "\n${YELLOW}[2/4] Consolidating environment settings...${NC}"
if [ -f ".env.template" ] && [ -f ".env.example" ]; then
    # Merge unique keys from template to example if needed, or just remove template
    # For now, we assume .env.example is the source of truth
    if cmp -s .env.template .env.example; then
        rm .env.template
        echo -e "${GREEN}✓ Removed redundant .env.template${NC}"
    else
        # Keep both if different, but suggest consolidation
        echo -e "${YELLOW}⚠ .env.template differs from .env.example. Keeping both for review.${NC}"
    fi
fi
# Ensure .env exists
if [ ! -f ".env" ] && [ -f ".env.example" ]; then
    cp .env.example .env
    echo -e "${GREEN}✓ Created .env from .env.example${NC}"
fi

# 3. Verify Documentation Consolidation
echo -e "\n${YELLOW}[3/4] Verifying documentation...${NC}"
if [ -d "docs/ingest" ] && [ ! -f "ingest/README.md" ]; then
    echo -e "${GREEN}✓ Documentation already consolidated in docs/ingest/${NC}"
else
    # Create docs/ingest if it doesn't exist
    mkdir -p docs/ingest
    # Move files if they exist in source
    for file in ingest/*.md; do
        if [ -f "$file" ]; then
            mv "$file" docs/ingest/
            echo -e "${GREEN}✓ Moved $file to docs/ingest/${NC}"
        fi
    done
fi

# 4. Install Dependencies
echo -e "\n${YELLOW}[4/4] Installing dependencies...${NC}"
if [ -f "requirements-all.txt" ]; then
    pip install -r requirements-all.txt
    echo -e "${GREEN}✓ Dependencies installed${NC}"
else
    echo -e "${YELLOW}⚠ requirements-all.txt not found!${NC}"
fi

echo -e "\n${BLUE}============================================================================${NC}"
echo -e "${GREEN}Cleanup Complete!${NC}"
echo -e "${BLUE}============================================================================${NC}"
echo -e "You can now start the application with:"
echo -e "  ${GREEN}./start.sh${NC}"
echo -e ""
