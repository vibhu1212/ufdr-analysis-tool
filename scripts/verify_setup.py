#!/usr/bin/env python3
"""
Installation verification script for UFDR Analysis Tool.
Checks all components and generates a comprehensive report.
"""

import sys
import subprocess
import sqlite3
from pathlib import Path
from typing import List, Tuple, Dict

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Note: Install 'rich' for better output: pip install rich")


class SetupVerifier:
    """Verifies UFDR Analysis Tool installation."""
    
    def __init__(self):
        self.console = Console() if RICH_AVAILABLE else None
        self.checks: List[Tuple[str, bool, str, str]] = []
        self.warnings: List[str] = []
    
    def print_header(self):
        """Print verification header."""
        if self.console:
            self.console.print("\n")
            self.console.print(Panel.fit(
                "[bold cyan]UFDR Analysis Tool - Installation Verification[/bold cyan]",
                border_style="cyan"
            ))
            self.console.print("\n")
        else:
            print("\n" + "="*70)
            print("UFDR Analysis Tool - Installation Verification")
            print("="*70 + "\n")
    
    def add_check(self, name: str, passed: bool, message: str, details: str = ""):
        """Add a check result."""
        self.checks.append((name, passed, message, details))
    
    def check_python_version(self) -> bool:
        """Check if Python version is 3.9 or higher."""
        version = sys.version_info
        version_str = f"{version.major}.{version.minor}.{version.micro}"
        
        if version.major == 3 and version.minor >= 9:
            self.add_check(
                "Python Version",
                True,
                f"Python {version_str}",
                "✓ Compatible version"
            )
            return True
        elif version.major > 3:
            self.add_check(
                "Python Version",
                True,
                f"Python {version_str}",
                "✓ Compatible version"
            )
            return True
        else:
            self.add_check(
                "Python Version",
                False,
                f"Python {version_str}",
                "✗ Requires Python 3.9 or higher"
            )
            return False
    
    def check_dependencies(self) -> bool:
        """Check if required packages are installed."""
        required_packages = [
            'streamlit',
            'pandas',
            'numpy',
            'neo4j',
            'sentence_transformers',
            'torch',
            'opencv-cv2',
            'pytest',
        ]
        
        missing = []
        installed = []
        
        for package in required_packages:
            try:
                # Try to import the package
                if package == 'opencv-cv2':
                    __import__('cv2')
                elif package == 'sentence_transformers':
                    __import__('sentence_transformers')
                else:
                    __import__(package)
                installed.append(package)
            except ImportError:
                missing.append(package)
        
        if not missing:
            self.add_check(
                "Dependencies",
                True,
                f"{len(installed)}/{len(required_packages)} packages installed",
                "✓ All required packages available"
            )
            return True
        else:
            self.add_check(
                "Dependencies",
                False,
                f"{len(installed)}/{len(required_packages)} packages installed",
                f"✗ Missing: {', '.join(missing[:3])}{'...' if len(missing) > 3 else ''}"
            )
            return False
    
    def check_ollama(self) -> bool:
        """Check if Ollama is installed and running."""
        # Check if Ollama is installed
        try:
            result = subprocess.run(
                ['ollama', '--version'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode != 0:
                self.add_check(
                    "Ollama Installation",
                    False,
                    "Not installed",
                    "✗ Ollama not found in PATH"
                )
                return False
        except (FileNotFoundError, subprocess.TimeoutExpired):
            self.add_check(
                "Ollama Installation",
                False,
                "Not installed",
                "✗ Ollama not found"
            )
            return False
        
        # Check if Ollama is running
        try:
            result = subprocess.run(
                ['ollama', 'list'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                self.add_check(
                    "Ollama Service",
                    True,
                    "Running",
                    "✓ Ollama is accessible"
                )
                return True
            else:
                self.add_check(
                    "Ollama Service",
                    False,
                    "Not running",
                    "✗ Run: ollama serve"
                )
                self.warnings.append("Start Ollama with: ollama serve")
                return False
        except subprocess.TimeoutExpired:
            self.add_check(
                "Ollama Service",
                False,
                "Timeout",
                "✗ Ollama not responding"
            )
            return False
    
    def check_models(self) -> Dict[str, bool]:
        """Check if required AI models are available."""
        required_models = {
            'llama3.1:8b': False,
            'nomic-embed-text': False,
        }
        
        try:
            result = subprocess.run(
                ['ollama', 'list'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                output = result.stdout.lower()
                for model in required_models.keys():
                    if model.lower() in output:
                        required_models[model] = True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        
        available = sum(required_models.values())
        total = len(required_models)
        
        if available == total:
            self.add_check(
                "AI Models",
                True,
                f"{available}/{total} models available",
                "✓ All required models downloaded"
            )
            return required_models
        else:
            missing = [m for m, avail in required_models.items() if not avail]
            self.add_check(
                "AI Models",
                False,
                f"{available}/{total} models available",
                f"✗ Missing: {', '.join(missing)}"
            )
            self.warnings.append("Download models with: setup_models.bat (Windows) or ./setup_models.sh (Linux/Mac)")
            return required_models
    
    def check_database(self) -> bool:
        """Check if database file exists and has valid schema."""
        db_paths = [
            Path("forensic_data.db"),
            Path("data/forensic.db"),
        ]
        
        db_found = None
        for db_path in db_paths:
            if db_path.exists():
                db_found = db_path
                break
        
        if not db_found:
            self.add_check(
                "Database",
                True,
                "Not found (will be created)",
                "ℹ Database will be created on first use"
            )
            self.warnings.append("Database will be created automatically on first use")
            return True
        
        # Check if database is valid SQLite
        try:
            conn = sqlite3.connect(str(db_found))
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            conn.close()
            
            if tables:
                self.add_check(
                    "Database",
                    True,
                    f"Found at {db_found}",
                    f"✓ Valid database with {len(tables)} tables"
                )
            else:
                self.add_check(
                    "Database",
                    True,
                    f"Found at {db_found}",
                    "ℹ Empty database (will be populated)"
                )
            return True
        except sqlite3.Error as e:
            self.add_check(
                "Database",
                False,
                f"Found at {db_found}",
                f"✗ Invalid database: {str(e)}"
            )
            return False
    
    def check_directory_structure(self) -> bool:
        """Check if required directories exist."""
        required_dirs = [
            "data",
            "logs",
            "exports",
            "uploads/ufdr",
            "frontend",
            "backend",
            "scripts",
        ]
        
        missing = []
        for dir_name in required_dirs:
            if not Path(dir_name).exists():
                missing.append(dir_name)
        
        if not missing:
            self.add_check(
                "Directory Structure",
                True,
                f"{len(required_dirs)}/{len(required_dirs)} directories found",
                "✓ All required directories exist"
            )
            return True
        else:
            self.add_check(
                "Directory Structure",
                False,
                f"{len(required_dirs)-len(missing)}/{len(required_dirs)} directories found",
                f"✗ Missing: {', '.join(missing[:3])}"
            )
            return False
    
    def generate_report(self) -> str:
        """Generate a summary report."""
        total = len(self.checks)
        passed = sum(1 for _, status, _, _ in self.checks if status)
        failed = total - passed
        
        if self.console:
            # Rich formatted output
            table = Table(title="Verification Results", box=box.ROUNDED)
            table.add_column("Component", style="cyan", no_wrap=True)
            table.add_column("Status", style="bold")
            table.add_column("Details", style="dim")
            
            for name, status, message, details in self.checks:
                status_str = "[green]✓ PASS[/green]" if status else "[red]✗ FAIL[/red]"
                table.add_row(name, status_str, f"{message}\n{details}")
            
            self.console.print(table)
            self.console.print("\n")
            
            # Summary
            if failed == 0:
                self.console.print(Panel(
                    f"[bold green]All {total} checks passed![/bold green]\n\n"
                    "Your installation is ready to use.\n"
                    "Run: [cyan]start.bat[/cyan] (Windows) or [cyan]./start.sh[/cyan] (Linux/Mac)",
                    title="✓ Success",
                    border_style="green"
                ))
            else:
                self.console.print(Panel(
                    f"[bold yellow]{passed}/{total} checks passed[/bold yellow]\n"
                    f"[bold red]{failed} checks failed[/bold red]\n\n"
                    "Please address the failed checks before starting the application.",
                    title="⚠ Issues Found",
                    border_style="yellow"
                ))
            
            # Warnings
            if self.warnings:
                self.console.print("\n[bold yellow]Warnings:[/bold yellow]")
                for warning in self.warnings:
                    self.console.print(f"  • {warning}")
            
            return ""
        else:
            # Plain text output
            report = "\n" + "="*70 + "\n"
            report += "VERIFICATION REPORT\n"
            report += "="*70 + "\n\n"
            
            for name, status, message, details in self.checks:
                status_str = "✓ PASS" if status else "✗ FAIL"
                report += f"{status_str:10} {name:25} {message}\n"
                if details:
                    report += f"           {' '*25} {details}\n"
            
            report += "\n" + "="*70 + "\n"
            report += f"Total checks: {total}\n"
            report += f"Passed: {passed}\n"
            report += f"Failed: {failed}\n"
            report += "="*70 + "\n"
            
            if failed == 0:
                report += "\n✓ All checks passed! Your installation is ready to use.\n"
                report += "Run: start.bat (Windows) or ./start.sh (Linux/Mac)\n"
            else:
                report += f"\n⚠ {failed} checks failed. Please address the issues above.\n"
            
            if self.warnings:
                report += "\nWarnings:\n"
                for warning in self.warnings:
                    report += f"  • {warning}\n"
            
            return report
    
    def run_all_checks(self) -> bool:
        """Run all verification checks."""
        self.print_header()
        
        if self.console:
            self.console.print("[bold]Running verification checks...[/bold]\n")
        else:
            print("Running verification checks...\n")
        
        # Run all checks
        python_ok = self.check_python_version()
        deps_ok = self.check_dependencies()
        self.check_ollama()
        self.check_models()
        self.check_database()
        dirs_ok = self.check_directory_structure()
        
        # Generate and display report
        report = self.generate_report()
        if report:
            print(report)
        
        # Return overall success
        return all([python_ok, deps_ok, dirs_ok])


def main():
    """Main entry point."""
    verifier = SetupVerifier()
    success = verifier.run_all_checks()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
