# JSON Structure Analyzer

A comprehensive Streamlit application for analyzing JSON data structures with support for dynamic SQL procedure parameter generation.

## 🚀 Features

### 📥 Input Methods
- **File Upload**: Upload JSON files directly
- **Text Input**: Paste JSON data manually into a text area

### 🧪 JSON Structure Analytics
- **Complete JSON Paths**: View all possible paths in your JSON structure
- **Array Analysis**: Identify arrays that need flattening for database operations
- **Nested Objects Detection**: Understand data hierarchy and nesting levels
- **Queryable Fields**: Highlight fields suitable for database queries with type information

### 🎨 Utility Features
- **JSON Prettifier**: Format and beautify JSON data instantly
- **Export Options**: Download analysis results as CSV and formatted JSON
- **Procedure Examples**: Generate parameter examples based on your data structure
- **Interactive Interface**: Filter and explore data with an intuitive UI

## 🛠️ Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Run the Streamlit application:
```bash
streamlit run json_analyzer_app.py
```

3. Open your browser and navigate to `http://localhost:8501`

## 📋 Usage

### Basic Usage
1. Choose your input method (File Upload or Text Input)
2. Provide your JSON data
3. Explore the different analysis tabs:
   - **Complete JSON Paths**: See all available paths
   - **Arrays Analysis**: Understand array structures
   - **Nested Objects**: Explore object hierarchies
   - **Queryable Fields**: Find fields suitable for queries
   - **JSON Prettifier**: Format your JSON

### Procedure Parameters
The app generates procedure parameter examples based on your JSON structure:

- `field_name` - Simple field extraction
- `field_name[operator:value]` - Field with condition
- `field_name[CAST:TYPE]` - Type casting
- `field1, field2` - Multiple fields
- `field[=:value:OR]` - Custom logic operator

**Supported operators**: =, !=, >, <, >=, <=, LIKE, NOT LIKE, IN, NOT IN, BETWEEN, CONTAINS, IS NULL, IS NOT NULL

### Example JSON Structure
```json
{
  "user_id": 12345,
  "name": "John Doe",
  "profile": {
    "age": 30,
    "address": {
      "city": "New York",
      "coordinates": {"lat": 40.7128, "lng": -74.0060}
    }
  },
  "orders": [
    {
      "order_id": "ORD-001",
      "items": [
        {"product_id": "PROD-123", "price": 799.99}
      ]
    }
  ]
}
```

This would generate paths like:
- `user_id` (NUMBER)
- `name` (STRING)
- `profile.age` (NUMBER)
- `profile.address.city` (STRING)
- `orders.order_id` (STRING - in array context)
- `orders.items.price` (NUMBER - in nested array context)

## 📁 Files

- `json_analyzer_app.py` - Main Streamlit application
- `improved_latest_script.py` - Fixed Snowflake procedure script
- `requirements.txt` - Python dependencies
- `sample_data.json` - Example JSON data for testing
- `README.md` - This documentation

## 🎯 Use Cases

1. **Database Schema Design**: Understand JSON structure before creating tables
2. **ETL Pipeline Planning**: Identify arrays and nested objects for flattening strategies
3. **API Data Analysis**: Explore complex JSON responses from APIs
4. **Data Migration**: Plan field mappings between JSON and relational databases
5. **Query Development**: Generate procedure parameters for dynamic SQL operations

## 🤝 Contributing

Feel free to submit issues and enhancement requests!

## 📝 License

This project is open source and available under the MIT License.
