Dynamic SQL Query Generator for Snowflake

Welcome to the Dynamic SQL Query Generator for Snowflake! This procedure, DYNAMIC_SQL_LARGE, empowers Snowflake users to dynamically generate SQL queries from complex JSON data. Designed for enterprise-scale processing, it enables efficient handling of JSON schema, operator compatibility, type casting, and performance optimization.

🚀 Features

	•	Dynamic Schema Caching: Enhances query generation speed by caching JSON schemas.
	•	Comprehensive Operator Support: Supports operators such as IN, NOT IN, BETWEEN, LIKE, etc.
	•	Flexible Type Casting: Dynamically casts fields to specified data types.
	•	Array and Nested Structure Handling: Processes deeply nested JSON with complex hierarchies.
	•	Robust Error Handling: Offers detailed feedback to streamline troubleshooting.

📘 Usage

Call the procedure in the following format:

CALL DYNAMIC_SQL_LARGE('TABLE_NAME', 'COLUMN_NAME', 'FIELD_CONDITIONS');

Example

CALL DYNAMIC_SQL_LARGE(
    'SAMPLE_TABLE_VARIANT',
    'DATA',
    'height[IN:10|30,CAST:INTEGER],month[CAST:TIMESTAMP],product_id[=:P200]'
);

Explanation:
	•	height: Filters with IN for values 10 and 30, casting as INTEGER.
	•	month: Casts to TIMESTAMP.
	•	product_id: Filters with the = operator, matching P200.

🔍 Sample Test Scenarios

1. Simple Filtering and Casting

CALL DYNAMIC_SQL_LARGE(
    'EMPLOYEE_DATA',
    'INFO',
    'salary[>:50000,CAST:INTEGER],start_date[CAST:DATE]'
);

	•	Filters salary > 50000, casting as INTEGER.
	•	Casts start_date to DATE.

2. Using Array Paths

CALL DYNAMIC_SQL_LARGE(
    'ORDERS_TABLE',
    'ORDER_DETAILS',
    'order_ids[IN:101|102|103],status[=:Shipped]'
);

	•	Filters order_ids for specific values.
	•	Filters status for Shipped.

3. Complex Nested JSON Structures

CALL DYNAMIC_SQL_LARGE(
    'INVENTORY',
    'PRODUCT_DETAILS',
    'dimensions.width[>:15,CAST:FLOAT],tags[LIKE:%Electronic%]'
);

	•	Filters dimensions.width > 15, casting as FLOAT.
	•	Uses LIKE on tags to filter for records containing “Electronic”.

4. BETWEEN Operator Usage

CALL DYNAMIC_SQL_LARGE(
    'TRANSACTION_HISTORY',
    'DATA',
    'transaction_amount[BETWEEN:1000|5000],date[CAST:DATE]'
);

	•	Filters transaction_amount between 1000 and 5000.
	•	Casts date to DATE.

📁 Repository Structure

	•	dynamic_sql_large.sql: Contains the main procedure for dynamic SQL generation.
	•	README.md: Documentation and usage instructions.
	•	test_cases.sql: Sample test cases to validate the procedure’s functionality.

🤝 Contributing

We welcome contributions! Feel free to test the code, open issues, and suggest enhancements. For questions or bug reports, please create an issue in this repository.
