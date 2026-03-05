#!/usr/bin/env python3
"""
Verification script for requirements files.
Checks for duplicates, version pinning, and organization.
"""

import re
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict


class RequirementsVerifier:
    """Verifies requirements file meets quality standards."""
    
    def __init__(self, requirements_path: str = "requirements-all.txt"):
        self.requirements_path = Path(requirements_path)
        self.packages: Dict[str, str] = {}
        self.duplicates: List[str] = []
        self.unpinned: List[str] = []
        self.results: List[Tuple[str, bool, str]] = []
    
    def parse_requirements(self) -> bool:
        """Parse the requirements file."""
        try:
            with open(self.requirements_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            package_counts = defaultdict(int)
            
            for line in lines:
                line = line.strip()
                
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                
                # Parse package name and version
                # Handle formats: package==version, package>=version, package~=version
                match = re.match(r'^([a-zA-Z0-9_\-\[\]]+)(==|>=|~=|<=|>|<)(.+)$', line)
                if match:
                    package_name = match.group(1).lower()
                    operator = match.group(2)
                    version = match.group(3)
                    
                    package_counts[package_name] += 1
                    
                    if package_counts[package_name] > 1:
                        self.duplicates.append(package_name)
                    
                    self.packages[package_name] = f"{operator}{version}"
                    
                    # Check if version is properly pinned
                    if operator not in ['==', '~=', '>=']:
                        self.unpinned.append(f"{package_name}{operator}{version}")
                else:
                    # Package without version specifier
                    package_name = line.split('[')[0].strip().lower()
                    if package_name and not package_name.startswith('-'):
                        self.unpinned.append(package_name)
                        package_counts[package_name] += 1
            
            return True
        except FileNotFoundError:
            print(f"Error: Requirements file not found at {self.requirements_path}")
            return False
        except Exception as e:
            print(f"Error parsing requirements: {e}")
            return False
    
    def verify_no_duplicates(self) -> bool:
        """Verify there are no duplicate packages."""
        print("\n=== Verifying No Duplicate Packages ===")
        
        if not self.duplicates:
            print("✓ No duplicate packages found")
            self.results.append(("No duplicate packages", True, ""))
            return True
        else:
            print(f"✗ Found {len(self.duplicates)} duplicate packages:")
            for pkg in set(self.duplicates):
                print(f"  - {pkg}")
            self.results.append(("No duplicate packages", False, f"{len(self.duplicates)} duplicates"))
            return False
    
    def verify_version_pinning(self) -> bool:
        """Verify all packages have version specifiers."""
        print("\n=== Verifying Version Pinning ===")
        
        if not self.unpinned:
            print(f"✓ All {len(self.packages)} packages have version specifiers")
            self.results.append(("All packages pinned", True, f"{len(self.packages)} packages"))
            return True
        else:
            print(f"✗ Found {len(self.unpinned)} packages without proper version pinning:")
            for pkg in self.unpinned[:10]:  # Show first 10
                print(f"  - {pkg}")
            if len(self.unpinned) > 10:
                print(f"  ... and {len(self.unpinned) - 10} more")
            self.results.append(("All packages pinned", False, f"{len(self.unpinned)} unpinned"))
            return False
    
    def verify_organization(self) -> bool:
        """Verify file is organized with category comments."""
        print("\n=== Verifying Organization ===")
        
        try:
            with open(self.requirements_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for category headers
            category_pattern = r'# ={10,}'
            categories = re.findall(category_pattern, content)
            
            if len(categories) >= 10:  # Should have multiple categories
                print(f"✓ File is well-organized with {len(categories)} category sections")
                self.results.append(("Well-organized", True, f"{len(categories)} categories"))
                return True
            else:
                print(f"✗ File has only {len(categories)} category sections (expected 10+)")
                self.results.append(("Well-organized", False, f"Only {len(categories)} categories"))
                return False
        except Exception as e:
            print(f"✗ Error checking organization: {e}")
            self.results.append(("Well-organized", False, str(e)))
            return False
    
    def verify_common_packages(self) -> bool:
        """Verify common required packages are present."""
        print("\n=== Verifying Common Packages ===")
        
        required_packages = [
            'streamlit',
            'pandas',
            'numpy',
            'neo4j',
            'sentence-transformers',
            'torch',
            'faiss-cpu',
            'opencv-python',
            'pillow',
            'pytest',
        ]
        
        missing = []
        for pkg in required_packages:
            if pkg.lower() not in self.packages:
                missing.append(pkg)
        
        if not missing:
            print(f"✓ All {len(required_packages)} common packages present")
            self.results.append(("Common packages present", True, f"{len(required_packages)} packages"))
            return True
        else:
            print(f"✗ Missing {len(missing)} common packages:")
            for pkg in missing:
                print(f"  - {pkg}")
            self.results.append(("Common packages present", False, f"{len(missing)} missing"))
            return False
    
    def check_conflicts(self) -> bool:
        """Check for known package conflicts."""
        print("\n=== Checking for Known Conflicts ===")
        
        conflicts = []
        
        # Check for torch CPU vs CUDA conflicts
        if 'torch' in self.packages:
            torch_version = self.packages['torch']
            if 'cuda' in torch_version.lower():
                conflicts.append("torch has CUDA version (should be CPU for compatibility)")
        
        if conflicts:
            print(f"✗ Found {len(conflicts)} potential conflicts:")
            for conflict in conflicts:
                print(f"  - {conflict}")
            self.results.append(("No conflicts", False, f"{len(conflicts)} conflicts"))
            return False
        else:
            print("✓ No known package conflicts detected")
            self.results.append(("No conflicts", True, ""))
            return True
    
    def generate_report(self) -> str:
        """Generate a summary report."""
        total = len(self.results)
        passed = sum(1 for _, status, _ in self.results if status)
        failed = total - passed
        
        report = f"\n{'='*70}\n"
        report += f"REQUIREMENTS VERIFICATION REPORT\n"
        report += f"{'='*70}\n"
        report += f"File: {self.requirements_path}\n"
        report += f"Total packages: {len(self.packages)}\n"
        report += f"Total checks: {total}\n"
        report += f"Passed: {passed}\n"
        report += f"Failed: {failed}\n"
        report += f"Success rate: {(passed/total*100):.1f}%\n"
        report += f"{'='*70}\n"
        
        if failed > 0:
            report += "\nFailed checks:\n"
            for desc, status, detail in self.results:
                if not status:
                    report += f"  ✗ {desc}"
                    if detail:
                        report += f": {detail}"
                    report += "\n"
        
        return report
    
    def run_all_checks(self) -> bool:
        """Run all verification checks."""
        print("Starting requirements verification...")
        print(f"Checking file: {self.requirements_path.absolute()}")
        
        if not self.parse_requirements():
            return False
        
        no_dupes = self.verify_no_duplicates()
        pinned = self.verify_version_pinning()
        organized = self.verify_organization()
        common = self.verify_common_packages()
        no_conflicts = self.check_conflicts()
        
        print(self.generate_report())
        
        return no_dupes and pinned and organized and common and no_conflicts


def main():
    """Main entry point."""
    verifier = RequirementsVerifier()
    success = verifier.run_all_checks()
    
    if success:
        print("\n✓ All requirements checks passed!")
        print("\nThe requirements-all.txt file:")
        print("  • Has no duplicate packages")
        print("  • All packages have version specifiers")
        print("  • Is well-organized with category sections")
        print("  • Contains all common required packages")
        print("  • Has no known package conflicts")
        return 0
    else:
        print("\n✗ Some requirements checks failed.")
        print("Please review the requirements-all.txt file.")
        return 1


if __name__ == "__main__":
    exit(main())
