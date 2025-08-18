import os
import sys
import logging.config
from pathlib import Path

def setup_paths():
    """Setup Python paths for proper module resolution"""
    # Get the root directory of the project
    root_dir = Path(__file__).parent
    src_dir = root_dir / "src"
    
    # Add both root and src to Python path
    for path in [str(root_dir), str(src_dir)]:
        if path not in sys.path:
            sys.path.insert(0, path)
    
    # Change to src directory for relative imports (REMOVED)
    # os.chdir(str(src_dir))
    
    return src_dir

def setup_logging():
    """Setup logging configuration"""
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

def main():
    """Main entry point"""
    try:
        # Setup paths and logging
        src_dir = setup_paths()
        setup_logging()
        
        # Verify all required files exist
        required_files = [
            "main.py",
            "universal_db_analyzer.py", 
            "python_sql_generator.py",
            "unified_snowflake_connector.py",
            "json_analyzer.py",
            "utils.py",
            "sql_generator.py",
            "config.py"
        ]
        
        missing_files = []
        for file in required_files:
            if not Path(file).exists():
                missing_files.append(file)
        
        if missing_files:
            import streamlit as st
            st.error(f"Missing required files in src/ directory: {', '.join(missing_files)}")
            st.stop()
        
        # Import and run the main app
        import main as app_main
        app_main.main()
        
    except ImportError as e:
        import streamlit as st
        st.error(f"Failed to import main application: {e}")
        st.error("Please check that all required modules are present in the src/ directory")
        
        # Debug information
        with st.expander("Debug Information"):
            st.code(f"Import error: {e}")
            st.code(f"Python path: {sys.path}")
            st.code(f"Current directory: {os.getcwd()}")
            st.code(f"Src directory exists: {src_dir.exists()}")
            if src_dir.exists():
                st.code(f"Files in src: {list(src_dir.glob('*.py'))}")
        st.stop()
        
    except Exception as e:
        import streamlit as st
        st.error(f"Application startup error: {e}")
        st.stop()

if __name__ == "__main__":
    main()
