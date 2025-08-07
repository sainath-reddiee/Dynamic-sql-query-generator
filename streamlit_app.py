"""
Production entry point for the Streamlit JSON-to-SQL Analyzer
"""
import os
import sys
import logging.config
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Set up logging
log_config_path = Path(__file__).parent / "config" / "logging.conf"
if log_config_path.exists():
    try:
        # Ensure logs directory exists
        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(exist_ok=True)
        logging.config.fileConfig(log_config_path)
    except Exception as e:
        # Fallback to basic logging if config fails
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        logging.getLogger(__name__).warning(f"Failed to load logging config: {e}")
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

# Import and run the main app
try:
    from src.main import main
    
    if __name__ == "__main__":
        main()
except ImportError as e:
    import streamlit as st
    st.error(f"Failed to import main application: {e}")
    st.stop()