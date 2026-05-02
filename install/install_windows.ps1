# UFDR Analysis Tool - Windows Installation Script
# For offline government deployment
# Version: 2.0.0

param(
    [string]$InstallPath = "C:\UFDR-Analysis-Tool",
    [switch]$SkipDependencies = $false,
    [switch]$Offline = $false,
    [switch]$DevMode = $false
)

# Colors for output
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }

Write-Host @"
╔══════════════════════════════════════════════════════════╗
║   UFDR Analysis Tool - Installation Wizard              ║
║   Version 2.0.0 | Government Deployment                 ║
║   Smart India Hackathon 2025                            ║
╚══════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

# Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error "This script must be run as Administrator!"
    Write-Info "Please right-click PowerShell and select 'Run as Administrator'"
    exit 1
}

Write-Success "✓ Running with Administrator privileges"

# System Requirements Check
Write-Info "`n[1/8] Checking System Requirements..."

# Check Windows version
$osVersion = [System.Environment]::OSVersion.Version
if ($osVersion.Major -lt 10) {
    Write-Error "Windows 10 or later is required"
    exit 1
}
Write-Success "✓ Windows version: $($osVersion.Major).$($osVersion.Minor)"

# Check available memory
$totalRAM = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB
if ($totalRAM -lt 8) {
    Write-Warning "! Warning: System has less than 8 GB RAM ($([math]::Round($totalRAM, 2)) GB)"
    Write-Warning "  Minimum recommended: 16 GB, Optimal: 32 GB"
    $continue = Read-Host "Continue anyway? (y/n)"
    if ($continue -ne 'y') { exit 1 }
} else {
    Write-Success "✓ RAM: $([math]::Round($totalRAM, 2)) GB"
}

# Check available disk space
$installDrive = Split-Path -Qualifier $InstallPath
$disk = Get-PSDrive $installDrive.Trim(':')
$freeSpaceGB = $disk.Free / 1GB
if ($freeSpaceGB -lt 10) {
    Write-Error "Insufficient disk space. Required: 10 GB, Available: $([math]::Round($freeSpaceGB, 2)) GB"
    exit 1
}
Write-Success "✓ Disk space: $([math]::Round($freeSpaceGB, 2)) GB available"

# Check CPU cores
$cpuCores = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
if ($cpuCores -lt 4) {
    Write-Warning "! Warning: System has less than 4 CPU cores ($cpuCores cores)"
    Write-Warning "  Minimum recommended: 4 cores, Optimal: 8+ cores"
}
Write-Success "✓ CPU cores: $cpuCores"

# Python Check
Write-Info "`n[2/8] Checking Python Installation..."

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    Write-Error "Python 3.9+ is not installed or not in PATH"
    Write-Info "Please install Python from: https://www.python.org/downloads/"
    Write-Info "Make sure to check 'Add Python to PATH' during installation"
    exit 1
}

$pythonVersion = python --version 2>&1
if ($pythonVersion -match "Python (\d+)\.(\d+)") {
    $majorVersion = [int]$matches[1]
    $minorVersion = [int]$matches[2]
    
    if ($majorVersion -lt 3 -or ($majorVersion -eq 3 -and $minorVersion -lt 9)) {
        Write-Error "Python 3.9+ is required. Found: $pythonVersion"
        exit 1
    }
    Write-Success "✓ Python version: $pythonVersion"
} else {
    Write-Warning "! Could not determine Python version"
}

# Check pip
$pipVersion = pip --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "pip is not installed"
    exit 1
}
Write-Success "✓ pip is available"

# Create Installation Directory
Write-Info "`n[3/8] Creating Installation Directory..."

if (Test-Path $InstallPath) {
    Write-Warning "Installation directory already exists: $InstallPath"
    $overwrite = Read-Host "Overwrite? (y/n)"
    if ($overwrite -ne 'y') {
        Write-Info "Installation cancelled"
        exit 0
    }
    Write-Info "Removing existing installation..."
    Remove-Item -Path $InstallPath -Recurse -Force -ErrorAction SilentlyContinue
}

New-Item -ItemType Directory -Path $InstallPath -Force | Out-Null
Write-Success "✓ Created: $InstallPath"

# Copy Application Files
Write-Info "`n[4/8] Copying Application Files..."

$currentDir = $PSScriptRoot
if (-not $currentDir) {
    $currentDir = Get-Location
}

$sourceDir = Split-Path -Parent $currentDir

# List of directories to copy
$dirsToTopy = @(
    "data",
    "database",
    "frontend",
    "infra",
    "ingest",
    "lib",
    "media",
    "parser",
    "prompts",
    "rag",
    "scripts",
    "utils",
    "visualization"
)

foreach ($dir in $dirsToTopy) {
    $sourcePath = Join-Path $sourceDir $dir
    if (Test-Path $sourcePath) {
        $destPath = Join-Path $InstallPath $dir
        Copy-Item -Path $sourcePath -Destination $destPath -Recurse -Force
        Write-Success "  ✓ Copied: $dir"
    } else {
        Write-Warning "  ! Skipped (not found): $dir"
    }
}

# Copy configuration files
$configFiles = @(
    "requirements.txt",
    "README.md",
    ".env.example",
    "finalize_setup.sh"
)

foreach ($file in $configFiles) {
    $sourcePath = Join-Path $sourceDir $file
    if (Test-Path $sourcePath) {
        Copy-Item -Path $sourcePath -Destination $InstallPath -Force
        Write-Success "  ✓ Copied: $file"
    }
}

# Create data directories
Write-Info "Creating data directories..."
$dataDirs = @(
    "data/parsed",
    "data/indices",
    "data/indices/backups",
    "data/samples",
    "logs",
    "exports"
)

foreach ($dir in $dataDirs) {
    $dirPath = Join-Path $InstallPath $dir
    New-Item -ItemType Directory -Path $dirPath -Force | Out-Null
    Write-Success "  ✓ Created: $dir"
}

# Install Python Dependencies
if (-not $SkipDependencies) {
    Write-Info "`n[5/8] Installing Python Dependencies..."
    Write-Warning "This may take 10-15 minutes..."
    
    Set-Location $InstallPath
    
    # Upgrade pip
    Write-Info "Upgrading pip..."
    python -m pip install --upgrade pip 2>&1 | Out-Null
    
    # Install core dependencies
    Write-Info "Installing core dependencies..."
    pip install -r requirements.txt --no-warn-script-location 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Success "✓ Core dependencies installed"
    } else {
        Write-Error "Failed to install core dependencies"
        exit 1
    }
    
    # Install additional UI dependencies just in case
    Write-Info "Installing additional UI dependencies..."
    pip install streamlit plotly pandas networkx --no-warn-script-location 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Success "✓ UI dependencies extra packages installed"
    } else {
        Write-Error "Failed to install extra UI dependencies"
    }
} else {
    Write-Warning "[5/8] Skipped: Dependency installation"
}

# Create Configuration Files
Write-Info "`n[6/8] Creating Configuration Files..."

$configContent = @"
# UFDR Analysis Tool Configuration
# Generated on: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")

# Installation
INSTALL_PATH=$InstallPath

# Neo4j Configuration (if using)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=$(-join ((48..57) + (65..90) + (97..122) | Get-Random -Count 16 | % {[char]$_}))

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
"@

$configPath = Join-Path $InstallPath "config.env"
$configContent | Out-File -FilePath $configPath -Encoding UTF8
Write-Success "✓ Created: config.env"

# Create launcher script
$launcherContent = @"
# UFDR Analysis Tool Launcher
# Quick start script

Write-Host "Starting UFDR Analysis Tool..." -ForegroundColor Cyan
Write-Host "Version 2.0.0 | Production Build" -ForegroundColor Cyan
Write-Host ""

Set-Location "$InstallPath"

Write-Host "Launching Production UI..." -ForegroundColor Green
Write-Host "Access at: http://localhost:8501" -ForegroundColor Yellow
Write-Host ""
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Gray
Write-Host ""

streamlit run frontend/app.py
"@

$launcherPath = Join-Path $InstallPath "launch.ps1"
$launcherContent | Out-File -FilePath $launcherPath -Encoding UTF8
Write-Success "✓ Created: launch.ps1"

# Create desktop shortcut
Write-Info "`n[7/8] Creating Desktop Shortcut..."

$desktopPath = [Environment]::GetFolderPath("Desktop")
$shortcutPath = Join-Path $desktopPath "UFDR Analysis Tool.lnk"

$WScriptShell = New-Object -ComObject WScript.Shell
$shortcut = $WScriptShell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = "powershell.exe"
$shortcut.Arguments = "-ExecutionPolicy Bypass -File `"$launcherPath`""
$shortcut.WorkingDirectory = $InstallPath
$shortcut.Description = "UFDR Forensic Analysis Tool"
$shortcut.Save()

Write-Success "✓ Desktop shortcut created"

# Verify Installation
Write-Info "`n[8/8] Verifying Installation..."

$verificationChecks = @(
    @{ Name = "Installation directory"; Path = $InstallPath },
    @{ Name = "Configuration file"; Path = $configPath },
    @{ Name = "Launcher script"; Path = $launcherPath },
    @{ Name = "Frontend directory"; Path = (Join-Path $InstallPath "frontend") },
    @{ Name = "Parser directory"; Path = (Join-Path $InstallPath "parser") }
)

$allPassed = $true
foreach ($check in $verificationChecks) {
    if (Test-Path $check.Path) {
        Write-Success "  ✓ $($check.Name)"
    } else {
        Write-Error "  ✗ $($check.Name) - NOT FOUND"
        $allPassed = $false
    }
}

# Final Summary
Write-Host "`n" -NoNewline
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║          Installation Complete!                          ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green

Write-Host "`n📍 Installation Location: " -NoNewline
Write-Host $InstallPath -ForegroundColor Cyan

Write-Host "`n🚀 Quick Start Options:`n"
Write-Host "  1. Double-click the desktop shortcut: " -NoNewline
Write-Host "'UFDR Analysis Tool'" -ForegroundColor Yellow
Write-Host "  2. Run from PowerShell:`n" -NoNewline
Write-Host "     cd $InstallPath`n" -NoNewline -ForegroundColor Cyan
Write-Host "     .\launch.ps1" -ForegroundColor Cyan
Write-Host "  3. Manual launch:`n" -NoNewline
Write-Host "     streamlit run frontend/app.py" -ForegroundColor Cyan

Write-Host "`n📚 Documentation:"
Write-Host "  • Full Docs: $InstallPath\README.md"

Write-Host "`n🌐 Access URL: " -NoNewline
Write-Host "http://localhost:8501" -ForegroundColor Yellow

Write-Host "`n⚙️  Configuration: " -NoNewline
Write-Host "$configPath" -ForegroundColor Cyan

if (-not $allPassed) {
    Write-Warning "`n⚠️  Some verification checks failed. Please review the installation."
    exit 1
}

Write-Host "`n✅ All systems ready! Launch the application to begin." -ForegroundColor Green
Write-Host ""

# Offer to launch immediately
$launchNow = Read-Host "Launch UFDR Analysis Tool now? (y/n)"
if ($launchNow -eq 'y') {
    Write-Info "`nLaunching..."
    Set-Location $InstallPath
    Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -File `"$launcherPath`""
}

Write-Host "`nInstallation log saved to: $InstallPath\install.log" -ForegroundColor Gray