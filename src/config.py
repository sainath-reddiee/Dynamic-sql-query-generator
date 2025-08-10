"""
Configuration management module
"""
import os
import logging
from typing import Optional, List
from pathlib import Path

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    
    # Look for .env file in the parent directory
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    # python-dotenv not installed, skip loading .env file
    pass


class Config:
    """Application configuration class"""
    
    # Application Settings
    APP_NAME: str = os.getenv('APP_NAME', 'JSON-to-SQL Analyzer for Snowflake')
    APP_VERSION: str = os.getenv('APP_VERSION', '1.0.0')
    DEBUG: bool = os.getenv('DEBUG', 'false').lower() == 'true'
    
    # Streamlit Settings
    STREAMLIT_SERVER_PORT: int = int(os.getenv('STREAMLIT_SERVER_PORT', '8501'))
    STREAMLIT_SERVER_ADDRESS: str = os.getenv('STREAMLIT_SERVER_ADDRESS', '0.0.0.0')
    STREAMLIT_CONFIG_FILE: Optional[str] = os.getenv('STREAMLIT_CONFIG_FILE')
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO').upper()
    LOG_FILE_PATH: str = os.getenv('LOG_FILE_PATH', 'logs/app.log')
    
    # Performance Settings
    MAX_UPLOAD_SIZE_MB: int = int(os.getenv('MAX_UPLOAD_SIZE_MB', '200'))
    JSON_ANALYSIS_MAX_DEPTH: int = int(os.getenv('JSON_ANALYSIS_MAX_DEPTH', '20'))
    CACHE_TTL_SECONDS: int = int(os.getenv('CACHE_TTL_SECONDS', '3600'))
    
    # Security Settings
    SECRET_KEY: Optional[str] = os.getenv('SECRET_KEY')
    API_KEY: Optional[str] = os.getenv('API_KEY')
    
    # Database Settings (for future use)
    DATABASE_URL: Optional[str] = os.getenv('DATABASE_URL')
    DATABASE_NAME: Optional[str] = os.getenv('DATABASE_NAME')
    
    # JSON Analysis Settings (Application-specific)
    DEFAULT_SCHEMA: str = os.getenv('DEFAULT_SCHEMA', 'PUBLIC')
    DEFAULT_WAREHOUSE: str = os.getenv('DEFAULT_WAREHOUSE', 'COMPUTE_WH')
    DEFAULT_QUERY_TIMEOUT: int = int(os.getenv('DEFAULT_QUERY_TIMEOUT', '300'))
    MAX_RESULT_ROWS: int = int(os.getenv('MAX_RESULT_ROWS', '10000'))
    
    # File Upload Settings
    ALLOWED_FILE_EXTENSIONS: List[str] = os.getenv('ALLOWED_FILE_EXTENSIONS', '.json,.txt').split(',')
    MAX_FIELD_CONDITIONS_LENGTH: int = int(os.getenv('MAX_FIELD_CONDITIONS_LENGTH', '1000'))
    
    # UI Settings
    ENABLE_PERFORMANCE_MODE: bool = os.getenv('ENABLE_PERFORMANCE_MODE', 'true').lower() == 'true'
    SHOW_DEBUG_INFO: bool = os.getenv('SHOW_DEBUG_INFO', 'false').lower() == 'true'
    
    @classmethod
    def setup_logging(cls):
        """Setup logging configuration with directory creation"""
        # Ensure logs directory exists
        log_file_path = Path(cls.LOG_FILE_PATH)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=cls.get_log_level(),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(cls.LOG_FILE_PATH, encoding='utf-8'),
                logging.StreamHandler()  # Also log to console
            ]
        )
        
        # Set specific logger levels
        if not cls.DEBUG:
            # In production, reduce noise from external libraries
            logging.getLogger('urllib3').setLevel(logging.WARNING)
            logging.getLogger('requests').setLevel(logging.WARNING)
    
    @classmethod
    def get_log_level(cls) -> int:
        """Convert log level string to logging level constant"""
        return getattr(logging, cls.LOG_LEVEL, logging.INFO)
    
    @classmethod
    def is_production(cls) -> bool:
        """Check if running in production environment"""
        return not cls.DEBUG and os.getenv('ENVIRONMENT', '').lower() == 'production'
    
    @classmethod
    def validate_config(cls) -> List[str]:
        """Validate configuration and return list of issues"""
        issues = []
        
        # Check required settings
        if cls.MAX_UPLOAD_SIZE_MB <= 0:
            issues.append("MAX_UPLOAD_SIZE_MB must be positive")
        
        if cls.JSON_ANALYSIS_MAX_DEPTH <= 0:
            issues.append("JSON_ANALYSIS_MAX_DEPTH must be positive")
        
        if cls.CACHE_TTL_SECONDS <= 0:
            issues.append("CACHE_TTL_SECONDS must be positive")
        
        # Check file extensions
        if not cls.ALLOWED_FILE_EXTENSIONS or not all(ext.startswith('.') for ext in cls.ALLOWED_FILE_EXTENSIONS):
            issues.append("ALLOWED_FILE_EXTENSIONS must be a list of extensions starting with '.'")
        
        return issues
    
    @classmethod
    def get_config_summary(cls) -> dict:
        """Get a summary of current configuration (without sensitive data)"""
        return {
            'app_name': cls.APP_NAME,
            'app_version': cls.APP_VERSION,
            'debug': cls.DEBUG,
            'server_port': cls.STREAMLIT_SERVER_PORT,
            'server_address': cls.STREAMLIT_SERVER_ADDRESS,
            'log_level': cls.LOG_LEVEL,
            'max_upload_size_mb': cls.MAX_UPLOAD_SIZE_MB,
            'json_max_depth': cls.JSON_ANALYSIS_MAX_DEPTH,
            'cache_ttl': cls.CACHE_TTL_SECONDS,
            'default_schema': cls.DEFAULT_SCHEMA,
            'default_warehouse': cls.DEFAULT_WAREHOUSE,
            'query_timeout': cls.DEFAULT_QUERY_TIMEOUT,
            'max_result_rows': cls.MAX_RESULT_ROWS,
            'allowed_extensions': cls.ALLOWED_FILE_EXTENSIONS,
            'performance_mode_enabled': cls.ENABLE_PERFORMANCE_MODE,
            'is_production': cls.is_production(),
            'validation_issues': cls.validate_config()
        }


# Create a global config instance
config = Config()

# Validate configuration on import
validation_issues = config.validate_config()
if validation_issues and not config.DEBUG:
    print(f"⚠️  Configuration issues detected: {', '.join(validation_issues)}")
