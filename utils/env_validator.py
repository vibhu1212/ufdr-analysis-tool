"""
Environment variable validation for UFDR Analysis Tool.
Validates required environment variables on application startup.
"""

import os
from typing import List, Tuple, Dict
from pathlib import Path


class EnvironmentValidator:
    """Validates environment variables and provides helpful error messages."""
    
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.suggestions: List[str] = []
    
    def validate_required_vars(self) -> bool:
        """
        Validate required environment variables.
        Returns True if all required vars are present, False otherwise.
        """
        # Define required variables based on deployment mode
        deployment_mode = os.getenv('DEPLOYMENT_MODE', 'local')
        
        required_vars = []
        
        # Cloud deployment requires API keys
        if deployment_mode == 'cloud':
            cloud_provider = os.getenv('CLOUD_LLM_PROVIDER', '')
            
            if cloud_provider == 'openai':
                required_vars.append('OPENAI_API_KEY')
            elif cloud_provider == 'anthropic':
                required_vars.append('ANTHROPIC_API_KEY')
            elif cloud_provider == 'cohere':
                required_vars.append('COHERE_API_KEY')
            elif cloud_provider:
                self.warnings.append(
                    f"Unknown CLOUD_LLM_PROVIDER: {cloud_provider}. "
                    "Supported providers: openai, anthropic, cohere"
                )
        
        # Check for missing required variables
        for var in required_vars:
            if not os.getenv(var):
                self.errors.append(f"Missing required environment variable: {var}")
                self.suggestions.append(
                    f"Set {var} in your .env file or environment"
                )
        
        return len(self.errors) == 0
    
    def validate_optional_vars(self) -> None:
        """Validate optional environment variables and provide warnings."""
        # Check database path
        db_path = os.getenv('DATABASE_PATH', 'forensic_data.db')
        if not Path(db_path).exists() and not Path(f"data/{db_path}").exists():
            self.warnings.append(
                f"Database file not found: {db_path}. "
                "A new database will be created on first use."
            )
        
        # Check API keys (optional — only needed for AI reports)
        gemini_key = os.getenv('GEMINI_API_KEY', '')
        openrouter_key = os.getenv('OPENROUTER_API_KEY', '')
        
        if not gemini_key and not openrouter_key:
            self.warnings.append(
                "Neither GEMINI_API_KEY nor OPENROUTER_API_KEY set. "
                "AI report generation & reasoning disabled. "
                "Search and indexing still work fully offline."
            )
        
        # Check storage directories
        upload_dir = os.getenv('UPLOAD_DIR', 'uploads/ufdr')
        if not Path(upload_dir).exists():
            try:
                os.makedirs(upload_dir, exist_ok=True)
            except Exception as e:
                self.warnings.append(f"Upload directory could not be created: {upload_dir} ({e})")
        
        # Check log level
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if log_level.upper() not in valid_levels:
            self.warnings.append(
                f"Invalid LOG_LEVEL: {log_level}. "
                f"Valid levels: {', '.join(valid_levels)}. "
                "Defaulting to INFO."
            )
    
    def validate_port_config(self) -> None:
        """Validate port configuration."""
        port = os.getenv('STREAMLIT_PORT', '8501')
        try:
            port_num = int(port)
            if port_num < 1024 or port_num > 65535:
                self.warnings.append(
                    f"STREAMLIT_PORT {port_num} is outside typical range (1024-65535). "
                    "Make sure this port is available."
                )
        except ValueError:
            self.errors.append(
                f"Invalid STREAMLIT_PORT: {port}. Must be a number between 1024 and 65535."
            )
            self.suggestions.append("Set STREAMLIT_PORT=8501 in your .env file")
    
    def validate_neo4j_config(self) -> None:
        """Validate Neo4j configuration if graph features are explicitly enabled."""
        # Default to false — Neo4j is optional and not required
        enable_graph = os.getenv('ENABLE_GRAPH_VIZ', 'false').lower() == 'true'
        
        if enable_graph:
            neo4j_uri = os.getenv('NEO4J_URI', '')
            if not neo4j_uri:
                self.warnings.append(
                    "ENABLE_GRAPH_VIZ is true but NEO4J_URI not set. "
                    "Set NEO4J_URI=bolt://localhost:7687 or set ENABLE_GRAPH_VIZ=false."
                )
    
    
    def validate_all(self) -> Tuple[bool, Dict[str, List[str]]]:
        """
        Run all validation checks.
        
        Returns:
            Tuple of (success, results) where results contains errors, warnings, and suggestions
        """
        # Run all validations
        required_ok = self.validate_required_vars()
        self.validate_optional_vars()
        self.validate_port_config()
        self.validate_neo4j_config()
        
        results = {
            'errors': self.errors,
            'warnings': self.warnings,
            'suggestions': self.suggestions
        }
        
        return required_ok, results
    
    def format_error_message(self, results: Dict[str, List[str]]) -> str:
        """Format validation results into a user-friendly error message."""
        message_parts = []
        
        if results['errors']:
            message_parts.append("[ERROR] Configuration Errors:")
            for error in results['errors']:
                message_parts.append(f"  - {error}")
            message_parts.append("")
        
        if results['warnings']:
            message_parts.append("[WARNING] Configuration Warnings:")
            for warning in results['warnings']:
                message_parts.append(f"  - {warning}")
            message_parts.append("")
        
        if results['suggestions']:
            message_parts.append("[INFO] Suggestions:")
            for suggestion in results['suggestions']:
                message_parts.append(f"  - {suggestion}")
            message_parts.append("")
        
        if results['errors']:
            message_parts.append("Please fix the errors above and restart the application.")
            message_parts.append("")
            message_parts.append("For help, see:")
            message_parts.append("  - .env.example for configuration template")
            message_parts.append("  - docs/installation/INSTALLATION.md for setup guide")
        
        return "\n".join(message_parts)


def validate_environment() -> Tuple[bool, str]:
    """
    Validate environment configuration.
    
    Returns:
        Tuple of (success, message)
    """
    validator = EnvironmentValidator()
    success, results = validator.validate_all()
    message = validator.format_error_message(results)
    
    return success, message


def check_env_file_exists() -> Tuple[bool, str]:
    """
    Check if .env file exists and provide guidance if not.
    
    Returns:
        Tuple of (exists, message)
    """
    # Use absolute path relative to this file (utils/env_validator.py -> project_root/.env)
    project_root = Path(__file__).resolve().parent.parent
    env_file = project_root / ".env"
    
    if not env_file.exists():
        message = """
[WARNING] No .env file found

The application will use default configuration values.

For production use or custom configuration:
1. Copy .env.example to .env
2. Edit .env with your configuration
3. Restart the application

Example:
  cp .env.example .env  (Linux/Mac)
  copy .env.example .env  (Windows)
"""
        return False, message
    
    return True, ""


if __name__ == "__main__":
    # Test the validator
    success, message = validate_environment()
    
    if message:
        print(message)
    
    if success:
        print("[OK] Environment validation passed!")
    else:
        print("[FAILED] Environment validation failed!")
        exit(1)
