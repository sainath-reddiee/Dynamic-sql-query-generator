"""
Health check and monitoring module
"""
import streamlit as st
import psutil
import json
import logging
from datetime import datetime
from typing import Dict, Any
from config import config

logger = logging.getLogger(__name__)


def get_system_info() -> Dict[str, Any]:
    """Get system information for health monitoring"""
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            'timestamp': datetime.now().isoformat(),
            'status': 'healthy',
            'system': {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available_mb': memory.available / (1024 * 1024),
                'disk_percent': disk.percent,
                'disk_free_gb': disk.free / (1024 * 1024 * 1024)
            },
            'application': {
                'name': config.APP_NAME,
                'version': config.APP_VERSION,
                'debug_mode': config.DEBUG,
                'environment': 'production' if config.is_production() else 'development'
            }
        }
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        return {
            'timestamp': datetime.now().isoformat(),
            'status': 'error',
            'error': str(e)
        }


def check_application_health() -> Dict[str, Any]:
    """Perform application health checks"""
    health_status = {
        'timestamp': datetime.now().isoformat(),
        'status': 'healthy',
        'checks': {}
    }
    
    try:
        # Check if main modules can be imported
        from json_analyzer import analyze_json_structure
        from utils import get_snowflake_type
        from sql_generator import generate_procedure_examples
        
        health_status['checks']['modules'] = 'ok'
        
        # Check if sample data can be processed
        sample_data = {"test": "value", "number": 123}
        schema = analyze_json_structure(sample_data)
        if schema:
            health_status['checks']['json_analysis'] = 'ok'
        else:
            health_status['checks']['json_analysis'] = 'warning'
        
        # Check configuration
        config_summary = config.get_config_summary()
        health_status['checks']['configuration'] = 'ok'
        health_status['config'] = config_summary
        
    except Exception as e:
        health_status['status'] = 'unhealthy'
        health_status['checks']['modules'] = f'error: {str(e)}'
        logger.error(f"Health check failed: {e}")
    
    return health_status


def display_health_dashboard():
    """Display health dashboard in Streamlit"""
    st.title("🏥 Application Health Dashboard")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("System Information")
        system_info = get_system_info()
        
        if system_info['status'] == 'healthy':
            st.success("✅ System Status: Healthy")
            
            # CPU Usage
            cpu_usage = system_info['system']['cpu_percent']
            st.metric("CPU Usage", f"{cpu_usage:.1f}%")
            if cpu_usage > 80:
                st.warning("⚠️ High CPU usage detected")
            
            # Memory Usage
            memory_usage = system_info['system']['memory_percent']
            st.metric("Memory Usage", f"{memory_usage:.1f}%")
            if memory_usage > 85:
                st.warning("⚠️ High memory usage detected")
            
            # Disk Usage
            disk_usage = system_info['system']['disk_percent']
            st.metric("Disk Usage", f"{disk_usage:.1f}%")
            if disk_usage > 90:
                st.error("🚨 Low disk space!")
        else:
            st.error("❌ System Status: Error")
            st.error(system_info.get('error', 'Unknown error'))
    
    with col2:
        st.subheader("Application Health")
        app_health = check_application_health()
        
        if app_health['status'] == 'healthy':
            st.success("✅ Application Status: Healthy")
        else:
            st.error("❌ Application Status: Unhealthy")
        
        # Display check results
        for check_name, check_result in app_health['checks'].items():
            if check_result == 'ok':
                st.success(f"✅ {check_name.title()}: OK")
            elif check_result == 'warning':
                st.warning(f"⚠️ {check_name.title()}: Warning")
            else:
                st.error(f"❌ {check_name.title()}: {check_result}")
    
    # Configuration Summary
    st.subheader("🔧 Configuration Summary")
    if 'config' in app_health:
        config_df = st.json(app_health['config'])
    
    # Raw JSON for debugging
    if config.DEBUG:
        st.subheader("🐛 Debug Information")
        st.json({
            'system_info': system_info,
            'app_health': app_health
        })


def log_health_metrics():
    """Log health metrics for monitoring"""
    try:
        system_info = get_system_info()
        app_health = check_application_health()
        
        # Log key metrics
        if system_info['status'] == 'healthy':
            cpu = system_info['system']['cpu_percent']
            memory = system_info['system']['memory_percent']
            disk = system_info['system']['disk_percent']
            
            logger.info(f"Health metrics - CPU: {cpu:.1f}%, Memory: {memory:.1f}%, Disk: {disk:.1f}%")
            
            # Log warnings for high usage
            if cpu > 80:
                logger.warning(f"High CPU usage: {cpu:.1f}%")
            if memory > 85:
                logger.warning(f"High memory usage: {memory:.1f}%")
            if disk > 90:
                logger.error(f"Low disk space: {disk:.1f}% used")
        
        if app_health['status'] != 'healthy':
            logger.error(f"Application health check failed: {app_health}")
            
    except Exception as e:
        logger.error(f"Error logging health metrics: {e}")


# Streamlit endpoint for external health checks
def health_check_endpoint() -> Dict[str, Any]:
    """Simple health check endpoint that returns JSON status"""
    try:
        system_info = get_system_info()
        app_health = check_application_health()
        
        return {
            'status': 'healthy' if system_info['status'] == 'healthy' and app_health['status'] == 'healthy' else 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'version': config.APP_VERSION,
            'system_ok': system_info['status'] == 'healthy',
            'app_ok': app_health['status'] == 'healthy'
        }
    except Exception as e:
        return {
            'status': 'error',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }