import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List, Set, Tuple
import re
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="❄️ Dynamic SQL Generator for Complex Json Data in Snowflake",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        color: #ff7f0e;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .feature-box {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .json-path {
        font-family: 'Courier New', monospace;
        background-color: #e8f4fd;
        padding: 0.2rem 0.4rem;
        border-radius: 0.3rem;
        color: #0066cc;
    }
    .array-indicator {
        background-color: #ffe6cc;
        color: #cc6600;
        padding: 0.2rem 0.4rem;
        border-radius: 0.3rem;
        font-size: 0.8rem;
    }
    .nested-indicator {
        background-color: #e6f3ff;
        color: #0080ff;
        padding: 0.2rem 0.4rem;
        border-radius: 0.3rem;
        font-size: 0.8rem;
    }
    .queryable-indicator {
        background-color: #e6ffe6;
        color: #00b300;
        padding: 0.2rem 0.4rem;
        border-radius: 0.3rem;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

def analyze_json_structure(json_obj: Any, parent_path: str = "", max_depth: int = 20) -> Dict[str, Dict]:
    """
    Analyze JSON structure and return comprehensive metadata
    """
    schema = {}
    
    def traverse_json(obj: Any, path: str = "", array_hierarchy: List[str] = [], depth: int = 0):
        if depth > max_depth:
            return
            
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                current_type = type(value).__name__
                
                # Determine if this is queryable (leaf node or simple type)
                is_queryable = not isinstance(value, (dict, list)) or (isinstance(value, list) and len(value) > 0 and not isinstance(value[0], (dict, list)))
                
                schema_entry = {
                    "type": current_type,
                    "snowflake_type": get_snowflake_type(current_type),
                    "array_hierarchy": array_hierarchy.copy(),
                    "depth": len(new_path.split('.')),
                    "full_path": new_path,
                    "parent_path": path,
                    "is_array_item": len(array_hierarchy) > 0,
                    "is_nested_object": isinstance(value, dict),
                    "is_queryable": is_queryable,
                    "sample_value": str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                }
                
                # Handle type conflicts
                if new_path in schema:
                    existing_type = schema[new_path]["type"]
                    if existing_type != current_type:
                        if current_type in ['str', 'int', 'float'] and existing_type == 'NoneType':
                            schema[new_path]["type"] = current_type
                            schema[new_path]["snowflake_type"] = get_snowflake_type(current_type)
                        elif existing_type in ['str', 'int', 'float'] and current_type == 'NoneType':
                            pass  # Keep existing
                        else:
                            schema[new_path]["type"] = "variant"
                            schema[new_path]["snowflake_type"] = "VARIANT"
                else:
                    schema[new_path] = schema_entry
                
                traverse_json(value, new_path, array_hierarchy, depth + 1)
                
        elif isinstance(obj, list) and obj:
            if path:
                schema[path] = {
                    "type": "list",
                    "snowflake_type": "ARRAY",
                    "array_hierarchy": array_hierarchy.copy(),
                    "depth": len(path.split('.')) if path else 0,
                    "full_path": path,
                    "parent_path": ".".join(path.split('.')[:-1]) if '.' in path else "",
                    "is_array_item": len(array_hierarchy) > 0,
                    "is_nested_object": False,
                    "is_queryable": True,
                    "sample_value": f"Array with {len(obj)} items",
                    "array_length": len(obj)
                }
            
            new_hierarchy = array_hierarchy + ([path] if path else [])
            
            # Analyze multiple array elements for better coverage
            sample_size = min(len(obj), 3)
            for i in range(sample_size):
                if isinstance(obj[i], (dict, list)):
                    traverse_json(obj[i], path, new_hierarchy, depth + 1)
                else:
                    # For primitive arrays, update schema info
                    if path in schema:
                        schema[path]["item_type"] = type(obj[i]).__name__
                        schema[path]["item_snowflake_type"] = get_snowflake_type(type(obj[i]).__name__)
                
    traverse_json(json_obj, parent_path)
    return schema

def get_snowflake_type(python_type: str) -> str:
    """Map Python types to Snowflake types"""
    type_mapping = {
        'str': 'STRING',
        'int': 'NUMBER',
        'float': 'NUMBER',
        'bool': 'BOOLEAN',
        'datetime': 'TIMESTAMP',
        'date': 'DATE',
        'dict': 'VARIANT',
        'list': 'ARRAY',
        'NoneType': 'VARIANT',
        'variant': 'VARIANT'
    }
    return type_mapping.get(python_type, 'VARIANT')

def find_arrays(schema: Dict[str, Dict]) -> List[Dict]:
    """Find all arrays in the schema"""
    arrays = []
    for path, info in schema.items():
        if info['type'] == 'list':
            arrays.append({
                'path': path,
                'depth': info['depth'],
                'length': info.get('array_length', 'Unknown'),
                'item_type': info.get('item_type', 'Mixed'),
                'parent_arrays': info['array_hierarchy']
            })
    return sorted(arrays, key=lambda x: x['depth'])

def find_nested_objects(schema: Dict[str, Dict]) -> List[Dict]:
    """Find all nested objects in the schema"""
    nested_objects = []
    for path, info in schema.items():
        if info['is_nested_object']:
            nested_objects.append({
                'path': path,
                'depth': info['depth'],
                'parent_arrays': info['array_hierarchy'],
                'is_in_array': len(info['array_hierarchy']) > 0
            })
    return sorted(nested_objects, key=lambda x: x['depth'])

def find_queryable_fields(schema: Dict[str, Dict]) -> List[Dict]:
    """Find all queryable fields"""
    queryable = []
    for path, info in schema.items():
        if info['is_queryable'] and info['type'] != 'list':
            queryable.append({
                'path': path,
                'type': info['type'],
                'snowflake_type': info['snowflake_type'],
                'depth': info['depth'],
                'sample_value': info['sample_value'],
                'in_array': len(info['array_hierarchy']) > 0,
                'array_context': info['array_hierarchy']
            })
    return sorted(queryable, key=lambda x: (x['depth'], x['path']))

def prettify_json(json_str: str) -> str:
    """Prettify JSON string"""
    try:
        parsed = json.loads(json_str)
        return json.dumps(parsed, indent=2, ensure_ascii=False)
    except json.JSONDecodeError as e:
        return f"Invalid JSON: {str(e)}"

def generate_procedure_examples(schema: Dict[str, Dict]) -> List[str]:
    """Generate example procedure calls based on the schema"""
    examples = []
    queryable_fields = find_queryable_fields(schema)
    
    if queryable_fields:
        # Simple field example
        simple_field = next((f for f in queryable_fields if not f['in_array']), queryable_fields[0])
        examples.append(f"{simple_field['path']}")
        
        # Conditional example
        if simple_field['type'] in ['str']:
            examples.append(f"{simple_field['path']}[=:value]")
        elif simple_field['type'] in ['int', 'float']:
            examples.append(f"{simple_field['path']}[>:100]")
        
        # Multiple fields example
        if len(queryable_fields) > 1:
            field1 = queryable_fields[0]['path']
            field2 = queryable_fields[1]['path']
            examples.append(f"{field1}, {field2}")
        
        # Cast example
        examples.append(f"{simple_field['path']}[CAST:STRING]")
        
        # Complex example with operators
        if len(queryable_fields) > 2:
            examples.append(f"{queryable_fields[0]['path']}[=:value1], {queryable_fields[1]['path']}[>:100:OR]")
    
    return examples

# Main App
def main():
    st.markdown('<h1 class="main-header">Dynamic SQL Generator for Complex Json Data in Snowflake</h1>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="feature-box">
    <h3>📥 Input Methods</h3>
    <p>How would you like to provide your JSON data?</p>
    <ul>
        <li>📁 Upload a JSON file</li>
        <li>✏️ Paste JSON text manually</li>
    </ul>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar for input method selection
    st.sidebar.header("Input Method")
    input_method = st.sidebar.radio(
        "Choose your input method:",
        ["Upload JSON File", "Paste JSON Text"]
    )
    
    json_data = None
    
    # Input handling
    if input_method == "Upload JSON File":
        st.sidebar.markdown("### 📁 File Upload")
        uploaded_file = st.sidebar.file_uploader(
            "Choose a JSON file",
            type=['json'],
            help="Upload a JSON file to analyze its structure"
        )
        
        if uploaded_file is not None:
            try:
                json_data = json.load(uploaded_file)
                st.sidebar.success(f"✅ File '{uploaded_file.name}' loaded successfully!")
            except json.JSONDecodeError as e:
                st.sidebar.error(f"❌ Invalid JSON file: {str(e)}")
            except Exception as e:
                st.sidebar.error(f"❌ Error reading file: {str(e)}")
    
    else:  # Paste JSON Text
        st.sidebar.markdown("### ✏️ Text Input")
        json_text = st.sidebar.text_area(
            "Paste your JSON here:",
            height=200,
            help="Paste JSON text directly into this area"
        )
        
        if json_text.strip():
            try:
                json_data = json.loads(json_text)
                st.sidebar.success("✅ JSON parsed successfully!")
            except json.JSONDecodeError as e:
                st.sidebar.error(f"❌ Invalid JSON: {str(e)}")
    
    # Main content area
    if json_data is not None:
        # Analyze the JSON structure
        schema = analyze_json_structure(json_data)
        
        # Create tabs for different features
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Complete JSON Paths", 
            "📋 Arrays Analysis", 
            "🏗️ Nested Objects", 
            "🔍 Queryable Fields",
            "🎨 JSON Prettifier"
        ])
        
        with tab1:
            st.markdown('<h2 class="section-header">📊 Complete JSON Paths</h2>', unsafe_allow_html=True)
            
            if schema:
                # Create a DataFrame for better display
                paths_data = []
                for path, info in schema.items():
                    paths_data.append({
                        'Path': path,
                        'Type': info['type'],
                        'Snowflake Type': info['snowflake_type'],
                        'Depth': info['depth'],
                        'In Array': '✅' if info['is_array_item'] else '❌',
                        'Queryable': '✅' if info['is_queryable'] else '❌',
                        'Sample Value': info['sample_value']
                    })
                
                df = pd.DataFrame(paths_data)
                st.dataframe(df, use_container_width=True)
                
                # Download option
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download paths as CSV",
                    data=csv,
                    file_name=f"json_paths_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.warning("No paths found in the JSON structure.")
        
        with tab2:
            st.markdown('<h2 class="section-header">📋 Arrays Analysis</h2>', unsafe_allow_html=True)
            
            arrays = find_arrays(schema)
            if arrays:
                st.info(f"Found {len(arrays)} array(s) in your JSON structure")
                
                for i, array in enumerate(arrays):
                    with st.expander(f"Array {i+1}: {array['path']}", expanded=True):
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("Depth Level", array['depth'])
                        with col2:
                            st.metric("Array Length", array['length'])
                        with col3:
                            st.write("**Item Type:**")
                            st.write(array['item_type'])
                        with col4:
                            st.write("**Parent Arrays:**")
                            if array['parent_arrays']:
                                for parent in array['parent_arrays']:
                                    st.markdown(f'<span class="array-indicator">{parent}</span>', unsafe_allow_html=True)
                            else:
                                st.write("Root level")
                        
                        st.markdown(f'**Full Path:** <span class="json-path">{array["path"]}</span>', unsafe_allow_html=True)
            else:
                st.info("No arrays found in the JSON structure.")
        
        with tab3:
            st.markdown('<h2 class="section-header">🏗️ Nested Objects</h2>', unsafe_allow_html=True)
            
            nested_objects = find_nested_objects(schema)
            if nested_objects:
                st.info(f"Found {len(nested_objects)} nested object(s)")
                
                for i, obj in enumerate(nested_objects):
                    with st.expander(f"Nested Object {i+1}: {obj['path']}", expanded=True):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.metric("Nesting Depth", obj['depth'])
                            st.write("**In Array Context:**")
                            if obj['is_in_array']:
                                st.markdown('<span class="array-indicator">Yes</span>', unsafe_allow_html=True)
                            else:
                                st.markdown('<span class="nested-indicator">No</span>', unsafe_allow_html=True)
                        
                        with col2:
                            st.write("**Parent Arrays:**")
                            if obj['parent_arrays']:
                                for parent in obj['parent_arrays']:
                                    st.markdown(f'<span class="array-indicator">{parent}</span>', unsafe_allow_html=True)
                            else:
                                st.write("None")
                        
                        st.markdown(f'**Full Path:** <span class="json-path">{obj["path"]}</span>', unsafe_allow_html=True)
            else:
                st.info("No nested objects found in the JSON structure.")
        
        with tab4:
            st.markdown('<h2 class="section-header">🔍 Queryable Fields</h2>', unsafe_allow_html=True)
            
            queryable_fields = find_queryable_fields(schema)
            if queryable_fields:
                st.info(f"Found {len(queryable_fields)} queryable field(s)")
                
                # Filter options
                col1, col2 = st.columns(2)
                with col1:
                    type_filter = st.selectbox(
                        "Filter by type:",
                        ["All"] + list(set([f['type'] for f in queryable_fields]))
                    )
                with col2:
                    array_filter = st.selectbox(
                        "Filter by array context:",
                        ["All", "In Arrays Only", "Not in Arrays"]
                    )
                
                # Apply filters
                filtered_fields = queryable_fields
                if type_filter != "All":
                    filtered_fields = [f for f in filtered_fields if f['type'] == type_filter]
                if array_filter == "In Arrays Only":
                    filtered_fields = [f for f in filtered_fields if f['in_array']]
                elif array_filter == "Not in Arrays":
                    filtered_fields = [f for f in filtered_fields if not f['in_array']]
                
                # Display fields
                for i, field in enumerate(filtered_fields):
                    with st.expander(f"Field {i+1}: {field['path']}", expanded=False):
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.write("**Python Type:**")
                            st.code(field['type'])
                            st.write("**Snowflake Type:**")
                            st.code(field['snowflake_type'])
                        
                        with col2:
                            st.metric("Depth", field['depth'])
                            st.write("**In Array:**")
                            if field['in_array']:
                                st.markdown('<span class="array-indicator">Yes</span>', unsafe_allow_html=True)
                            else:
                                st.markdown('<span class="queryable-indicator">No</span>', unsafe_allow_html=True)
                        
                        with col3:
                            st.write("**Sample Value:**")
                            st.code(field['sample_value'])
                        
                        if field['array_context']:
                            st.write("**Array Context:**")
                            for ctx in field['array_context']:
                                st.markdown(f'<span class="array-indicator">{ctx}</span>', unsafe_allow_html=True)
            else:
                st.info("No queryable fields found in the JSON structure.")
        
        with tab5:
            st.markdown('<h2 class="section-header">🎨 JSON Prettifier</h2>', unsafe_allow_html=True)
            
            # Display original JSON
            st.subheader("Original JSON:")
            original_json = json.dumps(json_data, separators=(',', ':'))
            st.text_area("Compact JSON", original_json, height=150, disabled=True)
            
            # Display prettified JSON
            st.subheader("Prettified JSON:")
            prettified = json.dumps(json_data, indent=2, ensure_ascii=False)
            st.text_area("Formatted JSON", prettified, height=300, disabled=True)
            
            # Download options
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="📥 Download Compact JSON",
                    data=original_json,
                    file_name=f"compact_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
            with col2:
                st.download_button(
                    label="📥 Download Prettified JSON",
                    data=prettified,
                    file_name=f"prettified_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        
        # Procedure Parameters Section
        st.markdown('<h2 class="section-header">🧪 Procedure Parameters</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("Parameter Input")
            procedures_input = st.text_area(
                "Enter procedure parameters:",
                height=100,
                help="Specify fields and conditions for the dynamic SQL procedure",
                placeholder="e.g., name, age[>:18], status[=:active]"
            )
            
            if st.button("🔍 Validate Parameters", type="primary"):
                if procedures_input.strip():
                    st.success(f"✅ Parameters entered: `{procedures_input}`")
                else:
                    st.warning("⚠️ Please enter some parameters to validate")
        
        with col2:
            st.subheader("Examples Based on Your Data")
            examples = generate_procedure_examples(schema)
            
            if examples:
                st.markdown("**Click to copy:**")
                for i, example in enumerate(examples):
                    if st.button(f"📋 {example}", key=f"example_{i}"):
                        st.code(example)
                        st.success(f"Copied: {example}")
            else:
                st.info("No examples available - please provide valid JSON data first.")
        
        # Instructions
        st.markdown("""
        <div class="feature-box">
        <h4>📚 Parameter Format Guide</h4>
        <ul>
            <li><code>field_name</code> - Simple field extraction</li>
            <li><code>field_name[operator:value]</code> - Field with condition</li>
            <li><code>field_name[CAST:TYPE]</code> - Type casting</li>
            <li><code>field1, field2</code> - Multiple fields</li>
            <li><code>field[=:value:OR]</code> - Custom logic operator</li>
        </ul>
        <p><strong>Supported operators:</strong> =, !=, >, <, >=, <=, LIKE, NOT LIKE, IN, NOT IN, BETWEEN, CONTAINS, IS NULL, IS NOT NULL</p>
        </div>
        """, unsafe_allow_html=True)
    
    else:
        # Welcome screen
        st.markdown("""
        <div class="feature-box">
        <h3>🧪 JSON Structure Analytics Features</h3>
        <ul>
            <li>📊 <strong>View complete JSON Paths</strong> - See all possible paths in your JSON structure</li>
            <li>📋 <strong>Identify Arrays for flattening</strong> - Find arrays that need special handling</li>
            <li>🏗️ <strong>Detect Nested Objects</strong> - Understand your data hierarchy</li>
            <li>🔍 <strong>Highlight Queryable Fields</strong> - Identify fields suitable for database queries</li>
        </ul>
        </div>
        
        <div class="feature-box">
        <h3>🎨 Utility Add-ons</h3>
        <ul>
            <li>✨ <strong>JSON Prettifier</strong> - Format and beautify JSON data instantly after input</li>
            <li>🧪 <strong>Procedure Examples</strong> - Generate parameter examples based on your data structure</li>
            <li>📥 <strong>Export Options</strong> - Download analysis results and formatted JSON</li>
        </ul>
        </div>
        
        <div style="text-align: center; margin-top: 2rem;">
        <h4>👆 Choose an input method from the sidebar to get started!</h4>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
