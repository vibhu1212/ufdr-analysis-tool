#!/usr/bin/env python3
"""
Content-based verification for .gitignore configuration.
Checks that required patterns are present in the .gitignore file.
"""

from pathlib import Path
from typing import List, Tuple


class GitignoreContentVerifier:
    """Verifies .gitignore file contains required patterns."""
    
    def __init__(self, gitignore_path: str = ".gitignore"):
        self.gitignore_path = Path(gitignore_path)
        self.content = self._read_gitignore()
        self.results: List[Tuple[str, bool, str]] = []
    
    def _read_gitignore(self) -> str:
        """Read the .gitignore file."""
        try:
            with open(self.gitignore_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            print(f"Error: .gitignore file not found at {self.gitignore_path}")
            return ""
    
    def check_pattern_present(self, pattern: str) -> bool:
        """Check if a pattern is present in .gitignore."""
        # Check for exact line match or pattern match
        lines = self.content.split('\n')
        for line in lines:
            line = line.strip()
            if line == pattern or line.startswith(pattern):
                return True
        return False
    
    def verify_exclusion_patterns(self):
        """Verify that required exclusion patterns are present."""
        print("\n=== Verifying Exclusion Patterns ===")
        
        required_patterns = [
            # Python cache (Requirement 4.1)
            ("__pycache__/", "Python cache directories"),
            ("*.pyc", "Python bytecode files"),
            
            # Virtual environments (Requirement 4.2)
            ("venv/", "venv/ directory"),
            ("env/", "env/ directory"),
            (".venv/", ".venv/ directory"),
            
            # Sensitive files (Requirement 4.4)
            (".env", "Environment variables"),
            ("*.key", "Key files"),
            ("*.pem", "PEM files"),
            ("*.cert", "Certificate files"),
            
            # Databases (Requirement 4.3)
            ("*.db", "Database files"),
            ("*.sqlite", "SQLite files"),
            
            # Logs (Requirement 4.3)
            ("*.log", "Log files"),
            ("logs/", "logs/ directory"),
            ("temp/", "temp/ directory"),
            
            # Exports (Requirement 4.3)
            ("exports/", "exports/ directory"),
            
            # Large model files (Requirement 4.3)
            ("infra/models/embeddings/*", "Embedding models"),
            ("infra/models/llm/*", "LLM models"),
            ("*.gguf", "GGUF model files"),
            ("yolov8*.pt", "YOLO models"),
        ]
        
        all_present = True
        for pattern, description in required_patterns:
            present = self.check_pattern_present(pattern)
            status = "✓" if present else "✗"
            self.results.append((description, present, pattern))
            print(f"{status} {description}: {pattern}")
            if not present:
                all_present = False
        
        return all_present
    
    def verify_inclusion_patterns(self):
        """Verify that important files are explicitly included."""
        print("\n=== Verifying Inclusion Patterns ===")
        
        required_inclusions = [
            # Environment examples (Requirement 4.4)
            ("!.env.example", ".env.example file"),
            ("!.env.template", ".env.template file"),
            
            # Test databases (Requirement 4.5)
            ("!test_data/*.db", "Test data databases"),
            ("!ingest/data/test_*.db", "Test databases in ingest/data"),
            
            # Test UFDR files (Requirement 4.5)
            ("!test_data/*.ufdr", "Test UFDR files"),
            
            # Model structure (Requirement 4.3)
            ("!infra/models/embeddings/.gitkeep", "Model directory structure"),
            ("!infra/models/llm/.gitkeep", "LLM directory structure"),
            ("!infra/models/MODEL_MANIFEST.json", "Model manifest"),
        ]
        
        all_present = True
        for pattern, description in required_inclusions:
            present = self.check_pattern_present(pattern)
            status = "✓" if present else "✗"
            self.results.append((description, present, pattern))
            print(f"{status} {description}: {pattern}")
            if not present:
                all_present = False
        
        return all_present
    
    def verify_documentation_included(self):
        """Verify that documentation is not excluded."""
        print("\n=== Verifying Documentation Handling ===")
        
        # Check that common documentation patterns are NOT excluded
        doc_patterns = [
            "*.md",
            "docs/",
            "README.md",
            "CONTRIBUTING.md",
        ]
        
        all_ok = True
        for pattern in doc_patterns:
            excluded = self.check_pattern_present(pattern)
            status = "✓" if not excluded else "✗"
            description = f"Documentation pattern NOT excluded: {pattern}"
            self.results.append((description, not excluded, pattern))
            print(f"{status} {description}")
            if excluded:
                all_ok = False
        
        return all_ok
    
    def verify_requirements_included(self):
        """Verify that requirements files are not excluded."""
        print("\n=== Verifying Requirements Files Handling ===")
        
        # Check that requirements files are NOT excluded
        req_patterns = [
            "requirements*.txt",
            "requirements.txt",
        ]
        
        all_ok = True
        for pattern in req_patterns:
            excluded = self.check_pattern_present(pattern)
            status = "✓" if not excluded else "✗"
            description = f"Requirements pattern NOT excluded: {pattern}"
            self.results.append((description, not excluded, pattern))
            print(f"{status} {description}")
            if excluded:
                all_ok = False
        
        return all_ok
    
    def generate_report(self) -> str:
        """Generate a summary report."""
        total = len(self.results)
        passed = sum(1 for _, status, _ in self.results if status)
        failed = total - passed
        
        report = f"\n{'='*70}\n"
        report += f"GITIGNORE CONTENT VERIFICATION REPORT\n"
        report += f"{'='*70}\n"
        report += f"Total checks: {total}\n"
        report += f"Passed: {passed}\n"
        report += f"Failed: {failed}\n"
        report += f"Success rate: {(passed/total*100):.1f}%\n"
        report += f"{'='*70}\n"
        
        if failed > 0:
            report += "\nFailed checks:\n"
            for desc, status, pattern in self.results:
                if not status:
                    report += f"  ✗ {desc}: {pattern}\n"
        
        return report
    
    def run_all_checks(self) -> bool:
        """Run all verification checks."""
        print("Starting .gitignore content verification...")
        print(f"Checking file: {self.gitignore_path.absolute()}")
        
        if not self.content:
            print("Error: Could not read .gitignore file")
            return False
        
        exclusions_ok = self.verify_exclusion_patterns()
        inclusions_ok = self.verify_inclusion_patterns()
        docs_ok = self.verify_documentation_included()
        reqs_ok = self.verify_requirements_included()
        
        print(self.generate_report())
        
        return exclusions_ok and inclusions_ok and docs_ok and reqs_ok


def main():
    """Main entry point."""
    verifier = GitignoreContentVerifier()
    success = verifier.run_all_checks()
    
    if success:
        print("\n✓ All .gitignore content checks passed!")
        print("\nThe .gitignore file correctly:")
        print("  • Excludes Python cache, virtual environments, and bytecode")
        print("  • Excludes sensitive files (.env, keys, certificates)")
        print("  • Excludes large model files and generated outputs")
        print("  • Excludes logs, temp files, and exports")
        print("  • Includes test data, documentation, and requirements files")
        print("  • Includes .env.example and .env.template")
        return 0
    else:
        print("\n✗ Some .gitignore content checks failed.")
        print("Please review the .gitignore file.")
        return 1


if __name__ == "__main__":
    exit(main())
