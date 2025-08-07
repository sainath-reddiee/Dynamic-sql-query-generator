# ❄️ Dynamic SQL Generator for Complex JSON Data in Snowflake

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://your-app-url.streamlit.app)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

A comprehensive web application for analyzing complex JSON data structures and generating dynamic SQL procedures for Snowflake databases. Perfect for data engineers, developers, and analysts working with nested JSON data.

## 🚀 Live Demo

Try the live application: **[Dynamic SQL Generator](https://your-app-url.streamlit.app)**

## ✨ Features

### 📥 Multiple Input Methods
- **File Upload**: Upload JSON files directly
- **Text Input**: Paste JSON data manually
- **Sample Data**: Use built-in examples to explore features

### 🧪 Comprehensive JSON Analysis
- **📊 Complete Path Mapping**: View all possible paths in your JSON structure
- **📋 Array Detection**: Identify arrays requiring LATERAL FLATTEN operations
- **🏗️ Nested Object Analysis**: Understand complex hierarchical structures
- **🔍 Queryable Field Identification**: Find database-ready fields with type mapping

### ⚙️ Snowflake Integration
- **Dynamic SQL Generation**: Create flexible procedures for varying JSON structures
- **Parameter Validation**: Smart parameter suggestions based on your data
- **Type Casting Support**: Handle complex type conversions
- **Production-Ready Code**: Generate deployment-ready stored procedures

### 🎨 Utility Features
- **JSON Prettification**: Format and beautify JSON data
- **Export Capabilities**: Download analysis results and formatted JSON
- **Interactive Filtering**: Search and filter results dynamically
- **Real-time Statistics**: Live overview of your JSON structure

## 🛠️ Installation & Setup

### Option 1: Run Locally

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/snowflake-json-generator.git
   cd snowflake-json-generator
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application**:
   ```bash
   streamlit run app.py
   ```

4. **Open your browser** and navigate to `http://localhost:8501`

### Option 2: Docker Deployment

1. **Build and run with Docker**:
   ```bash
   docker-compose up --build
   ```

2. **Access the application** at `http://localhost:8501`

### Option 3: Deploy to Streamlit Cloud

1. **Fork this repository**
2. **Connect to Streamlit Cloud**
3. **Deploy directly** from your GitHub repository

## 📋 Usage Guide

### Basic Workflow

1. **📥 Input JSON Data**
   - Upload a JSON file, paste JSON text, or use sample data
   
2. **🧪 Analyze Structure**
   - View complete path analysis
   - Identify arrays and nested objects
   - Find queryable fields
   
3. **⚙️ Generate SQL Procedure**
   - Configure table and column names
   - Specify field conditions
   - Get production-ready Snowflake procedure

### Snowflake Setup

1. **Create the Stored Procedure** in your Snowflake environment:
   ```sql
   -- Copy the generated procedure code from the app
   CREATE OR REPLACE PROCEDURE YOUR_SCHEMA.DYNAMIC_SQL_GENERATOR(...)
   -- ... (complete procedure code)
   ```

2. **Test the Procedure**:
   ```sql
   CALL YOUR_SCHEMA.DYNAMIC_SQL_GENERATOR(
       'YOUR_TABLE_NAME',
       'YOUR_JSON_COLUMN',
       'field1, field2[>:100], field3[CAST:STRING]'
   );
   ```

### Parameter Format Examples

| Format | Description | Example |
|--------|-------------|---------|
| `field_name` | Simple extraction | `user_id` |
| `field[op:value]` | With condition | `age[>:18]` |
| `field[CAST:TYPE]` | Type casting | `price[CAST:NUMBER]` |
| `field1, field2` | Multiple fields | `name, email` |
| `field[op:val:LOGIC]` | Custom logic | `status[=:active:OR]` |

**Supported Operators**: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `NOT LIKE`, `IN`, `NOT IN`, `BETWEEN`, `CONTAINS`, `IS NULL`, `IS NOT NULL`

## 🎯 Use Cases

### 1. **E-commerce Analytics**
```json
{
  "order_id": "ORD-123",
  "customer": {"id": 456, "tier": "premium"},
  "items": [{"product_id": "PROD-789", "price": 99.99}]
}
```
**Generated Fields**: `order_id`, `customer.tier`, `items.product_id`, `items.price`

### 2. **API Response Processing**
```json
{
  "user": {"profile": {"settings": {"theme": "dark"}}},
  "activity": [{"action": "login", "timestamp": "2024-01-01"}]
}
```
**Generated Fields**: `user.profile.settings.theme`, `activity.action`, `activity.timestamp`

### 3. **IoT Data Analysis**
```json
{
  "device_id": "DEV-001",
  "sensors": [
    {"type": "temperature", "value": 23.5, "unit": "celsius"}
  ]
}
```
**Generated Fields**: `device_id`, `sensors.type`, `sensors.value`, `sensors.unit`

## 📊 Sample Data

The application includes comprehensive sample data showcasing:
- **Nested Objects**: User profiles with address and preferences
- **Complex Arrays**: Order histories with multiple items
- **Mixed Data Types**: Strings, numbers, booleans, nulls
- **Deep Nesting**: Multi-level hierarchical structures

## 🔧 Advanced Features

### Smart Type Detection
- **Automatic Snowflake type mapping** from Python types
- **Conflict resolution** for mixed-type fields
- **Custom casting** support for type conversion

### Array Handling
- **Automatic LATERAL FLATTEN** generation
- **Parent-child relationship** detection
- **Nested array** support with proper aliasing

### Export Options
- **CSV export** of analysis results
- **SQL procedure** download
- **Formatted JSON** export
- **Analysis reports** in multiple formats

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

1. **Fork and clone** the repository
2. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. **Install development dependencies**:
   ```bash
   pip install -r requirements-dev.txt
   ```
4. **Run tests**:
   ```bash
   pytest tests/
   ```

### Feature Requests & Bug Reports

- 🐛 **Bug Reports**: [Create an issue](https://github.com/yourusername/repo/issues)
- 💡 **Feature Requests**: [Start a discussion](https://github.com/yourusername/repo/discussions)
- 📧 **Direct Contact**: your-email@domain.com

## 📈 Roadmap

- [ ] **Multi-database Support**: PostgreSQL, BigQuery, Redshift
- [ ] **Batch Processing**: Handle multiple JSON files
- [ ] **API Integration**: REST API for programmatic access
- [ ] **Advanced Analytics**: Statistical analysis of JSON structures
- [ ] **Template Library**: Pre-built templates for common use cases
- [ ] **Performance Optimization**: Large file handling improvements

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Streamlit Team** for the amazing web app framework
- **Snowflake** for powerful JSON processing capabilities
- **Open Source Community** for inspiration and feedback

## 📞 Support

- **Documentation**: [Wiki](https://github.com/yourusername/repo/wiki)
- **Community**: [Discussions](https://github.com/yourusername/repo/discussions)
- **Issues**: [Bug Tracker](https://github.com/yourusername/repo/issues)

---

**Built with ❤️ for the data community**

[![Made with Streamlit](https://img.shields.io/badge/Made%20with-Streamlit-red.svg)](https://streamlit.io)
[![Snowflake](https://img.shields.io/badge/Snowflake-Ready-blue.svg)](https://snowflake.com)
[![JSON](https://img.shields.io/badge/JSON-Processing-green.svg)](https://json.org)
