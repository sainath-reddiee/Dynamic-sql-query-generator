"""
Configuration management module
"""
import os
from typing import Optional
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
    
    @classmethod
    def get_log_level(cls) -> int:
        """Convert log level string to logging level constant"""
        import logging
        return getattr(logging, cls.LOG_LEVEL, logging.INFO)
    
    @classmethod
    def is_production(cls) -> bool:
        """Check if running in production environment"""
        return not cls.DEBUG and os.getenv('ENVIRONMENT', '').lower() == 'production'
    
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
            'is_production': cls.is_production()
        }


# Create a global config instance
config = Config()