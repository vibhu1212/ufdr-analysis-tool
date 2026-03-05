#!/usr/bin/env python3
"""
Verification script for .gitignore configuration.
Tests that required patterns are excluded and important files are included.
"""

import subprocess
from pathlib import Path
from typing import List, Tuple


class GitignoreVerifier:
    """Verifies .gitignore configuration meets requirements."""
    
    def __init__(self, repo_root: str = "."):
        self.repo_root = Path(repo_root)
        self.results: List[Tuple[str, bool, str]] = []
    
    def check_pattern_excluded(self, pattern: str) -> bool:
        """Check if a pattern is excluded by .gitignore."""
        try:
            # Use git check-ignore to test if pattern would be ignored
            result = subprocess.run(
                ["git", "check-ignore", pattern],
                cwd=self.repo_root,
                capture_output=True,
                text=True
            )
            # Exit code 0 means the file is ignored
            return result.returncode == 0
        except Exception as e:
            print(f"Warning: Could not check pattern {pattern}: {e}")
            return False
    
    def check_pattern_included(self, pattern: str) -> bool:
        """Check if a pattern is NOT excluded by .gitignore."""
        return not self.check_pattern_excluded(pattern)
    
    def verify_exclusions(self):
        """Verify that required patterns are excluded."""
        print("\n=== Verifying Exclusions ===")
        
        exclusions = [
            # Python cache (Requirement 4.1)
            ("__pycache__/test.pyc", "Python cache files"),
            ("test.pyc", "Python bytecode"),
            ("module.pyo", "Python optimized bytecode"),
            
            # Virtual environments (Requirement 4.2)
            ("venv/lib/python3.9", "venv/ directory"),
            ("env/bin/python", "env/ directory"),
            (".venv/pyvenv.cfg", ".venv/ directory"),
            
            # Sensitive files (Requirement 4.4)
            (".env", "Environment variables"),
            ("secrets/api_key.txt", "Secrets directory"),
            ("credentials/token.json", "Credentials directory"),
            
            # Databases except test DBs (Requirement 4.3)
            ("forensic_data.db", "Production database"),
            ("data/forensic.db", "Data directory database"),
            
            # Logs (Requirement 4.3)
            ("logs/app.log", "logs/ directory"),
            ("temp/cache.tmp", "temp/ directory"),
            ("audit.log", "Log files"),
            
            # Exports (Requirement 4.3)
            ("exports/report.csv", "exports/ directory"),
            
            # Large model files (Requirement 4.3)
            ("infra/models/llm/model.gguf", "LLM models"),
            ("infra/models/embeddings/model.bin", "Embedding models"),
            ("yolov8n.pt", "YOLO models"),
        ]
        
        for pattern, description in exclusions:
            excluded = self.check_pattern_excluded(pattern)
            status = "✓" if excluded else "✗"
            self.results.append((description, excluded, pattern))
            print(f"{status} {description}: {pattern}")
        
        return all(result[1] for result in self.results[-len(exclusions):])
    
    def verify_inclusions(self):
        """Verify that important files are NOT excluded."""
        print("\n=== Verifying Inclusions ===")
        
        inclusions = [
            # Test data (Requirement 4.5)
            ("test_data/README_TEST_DATA.md", "Test data directory"),
            ("test_data/complete_test.ufdr", "Test UFDR files"),
            
            # Environment examples (Requirement 4.4)
            (".env.example", ".env.example file"),
            (".env.template", ".env.template file"),
            
            # Requirements files (Requirement 4.5)
            ("requirements.txt", "requirements.txt"),
            ("requirements-frontend.txt", "requirements-frontend.txt"),
            ("requirements-all.txt", "requirements-all.txt (future)"),
            
            # Documentation (Requirement 4.5)
            ("README.md", "README.md"),
            ("docs/INSTALLATION.md", "Documentation files"),
            ("CONTRIBUTING.md", "CONTRIBUTING.md (future)"),
            
            # Model manifest
            ("infra/models/MODEL_MANIFEST.json", "Model manifest"),
            
            # Gitkeep files
            ("infra/models/llm/.gitkeep", "Model directory structure"),
        ]
        
        for pattern, description in inclusions:
            included = self.check_pattern_included(pattern)
            status = "✓" if included else "✗"
            self.results.append((description, included, pattern))
            print(f"{status} {description}: {pattern}")
        
        return all(result[1] for result in self.results[-len(inclusions):])
    
    def verify_test_databases(self):
        """Verify that test databases are included while others are excluded."""
        print("\n=== Verifying Test Database Handling ===")
        
        test_cases = [
            # Should be excluded
            ("data/forensic.db", False, "Production database"),
            ("forensic_data.db", False, "Root database"),
            
            # Should be included (test databases)
            ("test_data/test.db", True, "Test data database"),
            ("ingest/data/test_mvp.db", True, "Test MVP database"),
            ("ingest/data/test_e2e_mvp.db", True, "Test E2E database"),
        ]
        
        all_correct = True
        for pattern, should_include, description in test_cases:
            included = self.check_pattern_included(pattern)
            correct = (included == should_include)
            status = "✓" if correct else "✗"
            expected = "included" if should_include else "excluded"
            actual = "included" if included else "excluded"
            
            self.results.append((description, correct, pattern))
            print(f"{status} {description}: {pattern} (expected {expected}, got {actual})")
            
            if not correct:
                all_correct = False
        
        return all_correct
    
    def generate_report(self) -> str:
        """Generate a summary report."""
        total = len(self.results)
        passed = sum(1 for _, status, _ in self.results if status)
        failed = total - passed
        
        report = f"\n{'='*60}\n"
        report += f"GITIGNORE VERIFICATION REPORT\n"
        report += f"{'='*60}\n"
        report += f"Total checks: {total}\n"
        report += f"Passed: {passed}\n"
        report += f"Failed: {failed}\n"
        report += f"Success rate: {(passed/total*100):.1f}%\n"
        report += f"{'='*60}\n"
        
        if failed > 0:
            report += "\nFailed checks:\n"
            for desc, status, pattern in self.results:
                if not status:
                    report += f"  ✗ {desc}: {pattern}\n"
        
        return report
    
    def run_all_checks(self) -> bool:
        """Run all verification checks."""
        print("Starting .gitignore verification...")
        print(f"Repository root: {self.repo_root.absolute()}")
        
        exclusions_ok = self.verify_exclusions()
        inclusions_ok = self.verify_inclusions()
        test_dbs_ok = self.verify_test_databases()
        
        print(self.generate_report())
        
        return exclusions_ok and inclusions_ok and test_dbs_ok


def main():
    """Main entry point."""
    verifier = GitignoreVerifier()
    success = verifier.run_all_checks()
    
    if success:
        print("\n✓ All .gitignore checks passed!")
        return 0
    else:
        print("\n✗ Some .gitignore checks failed. Please review the .gitignore file.")
        return 1


if __name__ == "__main__":
    exit(main())
