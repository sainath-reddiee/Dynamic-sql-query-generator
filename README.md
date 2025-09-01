❄️ Dynamic SQL Analyzer & Generator for Snowflake v3.0
Transform complex JSON into powerful, production-ready Snowflake SQL with an intelligent, interactive application.

This tool has evolved from a powerful script into a full-featured Streamlit application designed for enterprise-grade JSON processing. It offers a dual-mode workflow that caters to both rapid development with sample data and live, large-scale analysis directly against your Snowflake database.

🚀 Live Demo & Features
Experience the application live: Dynamic SQL Generator v3.0

The live demo showcases the full power of the application, allowing you to:

Interactively Analyze JSON: Upload or paste your JSON and see a complete structural analysis in real-time.

Generate SQL Dynamically: Use the interactive UI to define your query parameters and watch the SQL code generate instantly.

Connect to Snowflake: Securely connect to your Snowflake instance to perform live data analysis and query execution.

Explore Smart Suggestions: Get intelligent, context-aware field suggestions based on your data's actual schema.

workflow
The application provides two distinct, powerful workflows:

1. 🐍 Python Mode (Rapid Prototyping)
Ideal for developers who need to quickly generate and test SQL based on a sample of their JSON.

Provide Sample Data: Paste or upload your JSON data directly into the application's sidebar.

Analyze Structure: The application instantly parses the JSON, identifies all possible paths, and detects any fields with ambiguous names (e.g., id appearing in multiple nested objects).

Define Your Query: Specify the fields you want to select and the conditions to apply. The UI provides smart suggestions based on your data to accelerate this process.

Generate & Export: Instantly generate the SQL. From there, you can export it as a .sql file, a complete dbt model, or a Jupyter Notebook.

2. 🏔️ Snowflake Mode (Live Database Analysis)
Perfect for data analysts and engineers who need to work with live, large-scale data directly in their warehouse.

Connect Securely: Use the unified connector to establish a connection to your Snowflake instance in either Standard or Enhanced (high-performance) mode.

Analyze Live Schema: The tool samples your actual table to build an accurate schema, including data types and field frequencies, ensuring your queries are based on the real structure of your data.

Generate & Execute: Construct your query using intelligent, database-aware suggestions. You can then generate and execute the SQL directly against your database.

Review Performance: Get immediate feedback with performance metrics, including execution time, rows returned, and memory usage, to understand the impact of your queries.

🎯 Use Cases
1. E-commerce Analytics
Unpack complex order and customer data with ease.

Example JSON:

{
  "order_id": "ORD-123",
  "customer": {"id": 456, "tier": "premium"},
  "items": [{"product_id": "PROD-789", "price": 99.99}]
}

Generated Fields: order_id, customer_tier, items_product_id, items_price.

2. API Response Processing
Flatten and query nested JSON responses from APIs in a single step.

Example JSON:

{
  "user": {"profile": {"settings": {"theme": "dark"}}},
  "activity": [{"action": "login", "timestamp": "2024-01-01"}]
}

Generated Fields: user_profile_settings_theme, activity_action, activity_timestamp.

3. IoT Data Analysis
Query time-series data from sensors and devices with complex nested structures.

Example JSON:

{
  "device_id": "DEV-001",
  "sensors": [
    {"type": "temperature", "value": 23.5, "unit": "celsius"}
  ]
}

Generated Fields: device_id, sensors_type, sensors_value, sensors_unit.

⚙️ Parameter Format Examples
Format

Description

Example

field_name

Simple extraction

user_id

field[op:value]

With condition

age[>:18]

field[CAST:TYPE]

Type casting

price[CAST:NUMBER]

field1, field2

Multiple fields

name, email

field[op:val:LOGIC]

Custom logic

status[=:active:OR]

Supported Operators: =, !=, >, <, >=, <=, LIKE, NOT LIKE, IN, NOT IN, BETWEEN, CONTAINS, IS NULL, IS NOT NULL.

🔧 Technical Highlights
Intelligent Field Disambiguation: The core engine automatically detects fields with the same name in different parts of the JSON structure and generates unique, context-aware aliases to prevent conflicts in your final query.

Performance-Tuned Engine: Features an optional high-performance mode with Modin acceleration for processing large datasets, alongside intelligent schema caching to dramatically speed up repeated analyses.

Robust Engine: The SQL generation engine correctly handles deeply nested arrays, parent-child relationships, and context-aware aliasing to produce optimized and readable queries.

Security-First Design: Includes built-in input sanitization to prevent SQL injection and follows security best practices, ensuring that the generated code is safe for production environments.

🤝 Contributing
Contributions are welcome! Please see our Contributing Guide for details on setting up a development environment, running tests, and submitting pull requests.

📝 License
This project is licensed under the MIT License. See the LICENSE file for details.
