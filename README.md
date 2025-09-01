# ❄️ Dynamic SQL Analyzer & Generator for Snowflake v3.0

Transform complex JSON into powerful, production-ready Snowflake SQL with an intelligent, interactive Streamlit application.

---

## 🚀 Live Demo & Features

Experience the full power of the application:

- **Interactively Analyze JSON**  
  Upload or paste your JSON and see a complete structural analysis in real-time.

- **Generate SQL Dynamically**  
  Define query parameters via the UI and watch SQL code generate instantly.

- **Connect to Snowflake**  
  Securely connect to your Snowflake instance for live data analysis and query execution.

- **Explore Smart Suggestions**  
  Get intelligent, context-aware field suggestions based on your actual schema.

---

## 🔁 Application Workflow

### 1. 🐍 Python Mode (Rapid Prototyping)

Ideal for developers working with sample JSON data.

- Paste or upload JSON in the sidebar.
- Instantly parse structure and detect ambiguous fields.
- Select fields and conditions with smart suggestions.
- Generate SQL and export as `.sql`, dbt model, or Jupyter Notebook.

### 2. 🏔️ Snowflake Mode (Live Database Analysis)

Perfect for analysts working with live, large-scale data.

- Securely connect to Snowflake (Standard or Enhanced mode).
- Sample actual tables to build accurate schema.
- Generate and execute SQL directly against your warehouse.
- Review performance metrics: execution time, rows returned, memory usage.

---

## 🎯 Use Cases

### 1. E-commerce Analytics

**Input JSON:**
```json
{
  "order_id": "ORD-123",
  "customer": {"id": 456, "tier": "premium"},
  "items": [{"product_id": "PROD-789", "price": 99.99}]
}
```

**Generated Fields:**  
`order_id`, `customer_tier`, `items_product_id`, `items_price`

---

### 2. API Response Processing

**Input JSON:**
```json
{
  "user": {"profile": {"settings": {"theme": "dark"}}},
  "activity": [{"action": "login", "timestamp": "2024-01-01"}]
}
```

**Generated Fields:**  
`user_profile_settings_theme`, `activity_action`, `activity_timestamp`

---

### 3. IoT Data Analysis

**Input JSON:**
```json
{
  "device_id": "DEV-001",
  "sensors": [
    {"type": "temperature", "value": 23.5, "unit": "celsius"}
  ]
}
```

**Generated Fields:**  
`device_id`, `sensors_type`, `sensors_value`, `sensors_unit`

---

## ⚙️ Parameter Format Examples

| Format               | Description       | Example               |
|----------------------|-------------------|------------------------|
| `field_name`         | Simple extraction | `user_id`             |
| `field[op:value]`    | With condition    | `age[>:18]`           |
| `field[CAST:TYPE]`   | Type casting      | `price[CAST:NUMBER]`  |
| `field1, field2`     | Multiple fields   | `name, email`         |
| `field[op:val:LOGIC]`| Custom logic      | `status[=:active:OR]` |

**Supported Operators:**  
`=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `NOT LIKE`, `IN`, `NOT IN`, `BETWEEN`, `CONTAINS`, `IS NULL`, `IS NOT NULL`

---

## 🔧 Technical Highlights

- **Intelligent Field Disambiguation**  
  Automatically detects duplicate field names and generates unique aliases.

- **Performance-Tuned Engine**  
  Optional Modin acceleration and schema caching for large datasets.

- **Robust SQL Generation**  
  Handles deeply nested arrays, parent-child relationships, and aliasing.

- **Security-First Design**  
  Built-in input sanitization and best practices to prevent SQL injection.

---

## 🤝 Contributing

We welcome contributions!  
Please refer to the [Contributing Guide](CONTRIBUTING.md) for setup instructions, testing, and pull request guidelines.

---

## 📝 License

This project is licensed under the MIT License.  
See the [LICENSE](LICENSE) file for details.

---
