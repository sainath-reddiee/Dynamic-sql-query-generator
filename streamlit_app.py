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
        
        # Verify all required files exist in src directory
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
            # Check in src directory, not root
            file_path = src_dir / file
            if not file_path.exists():
                missing_files.append(file)
        
        if missing_files:
            import streamlit as st
            st.error(f"Missing required files in src/ directory: {', '.join(missing_files)}")
            
            # Debug information
            with st.expander("🔍 Debug Information"):
                st.code(f"Root directory: {Path(__file__).parent}")
                st.code(f"Src directory: {src_dir}")
                st.code(f"Src directory exists: {src_dir.exists()}")
                if src_dir.exists():
                    st.code(f"Files in src: {[f.name for f in src_dir.glob('*.py')]}")
                else:
                    st.error("src/ directory does not exist!")
                    st.info("Expected project structure:")
                    st.code("""
project-root/
├── streamlit_app.py
├── src/
│   ├── main.py
│   ├── config.py
│   ├── utils.py
│   └── ... (other modules)
└── config/
    └── logging.conf (optional)
                    """)
            st.stop()
        
        # Import and run the main app from src directory
        from src.main import main as app_main
        app_main()
        
    except ImportError as e:
        import streamlit as st
        st.error(f"Failed to import main application: {e}")
        st.error("Please check that all required modules are present in the src/ directory")
        
        # Enhanced debug information
        with st.expander("🔧 Debug Information"):
            st.code(f"Import error: {e}")
            st.code(f"Python path: {sys.path}")
            st.code(f"Current directory: {os.getcwd()}")
            st.code(f"Root directory: {Path(__file__).parent}")
            st.code(f"Src directory: {src_dir}")
            st.code(f"Src directory exists: {src_dir.exists()}")
            if src_dir.exists():
                st.code(f"Files in src: {[f.name for f in src_dir.glob('*.py')]}")
            
            # Try alternative import methods
            st.markdown("**Attempting alternative imports:**")
            try:
                import main as direct_main
                st.success("✅ Direct import of main.py succeeded")
            except ImportError as direct_e:
                st.error(f"❌ Direct import failed: {direct_e}")
            
            try:
                from src import main as src_main
                st.success("✅ Import from src.main succeeded")
            except ImportError as src_e:
                st.error(f"❌ Import from src.main failed: {src_e}")
        st.stop()
        
    except Exception as e:
        import streamlit as st
        st.error(f"Application startup error: {e}")
        
        with st.expander("🚨 Error Details"):
            import traceback
            st.code(traceback.format_exc())
            
            st.markdown("**Troubleshooting steps:**")
            st.markdown("1. Ensure all files are in the `src/` directory")
            st.markdown("2. Check that Python can import the modules")
            st.markdown("3. Verify file permissions")
            st.markdown("4. Try running from the correct directory")
        st.stop()

if __name__ == "__main__":
    main()
