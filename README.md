# Dynamic-sql-query-generator
Dynamic SQL Query Generator for JSON in Snowflake

Introduction

Welcome to the Dynamic SQL Query Generator for JSON in Snowflake! This project enables users to dynamically generate SQL queries for JSON data stored in Snowflake tables. The goal is to simplify the querying process, reduce manual work, and enhance data extraction capabilities by automatically analyzing JSON structures.

Table of Contents

	•	Features
	•	Getting Started
	•	Procedure Overview
	•	Usage Examples
	•	Contributing

Features

	•	Dynamic SQL Generation: Automatically constructs SQL queries tailored to the JSON structure without hardcoding paths or field names.
	•	Schema Generation: Analyzes JSON data to generate a comprehensive schema with array path tracking.
	•	Error Handling: Provides detailed error messages for easier debugging of missing fields or invalid data formats.
	•	Metadata Support: Optionally includes metadata about fields and their types in the generated SQL queries.

Getting Started

Prerequisites

	1.	Snowflake Account: Ensure you have a Snowflake account with sufficient permissions to create and execute stored procedures.
	2.	Snowpark Library: This project uses the Snowpark library for Python, so make sure it is available in your environment.

 Procedure Overview

The DYNAMIC_SQL_QUERY procedure accepts the following parameters:

	•	TABLE_NAME: Name of the table containing the JSON column.
	•	JSON_COLUMN: Name of the JSON column you wish to query.
	•	FIELD_NAMES: Comma-separated list of JSON fields to extract.
	•	INCLUDE_METADATA: Boolean flag to include field metadata in the output.

How It Works

	1.	Fetches sample JSON data from the specified column.
	2.	Analyzes the JSON structure to create a schema.
	3.	Dynamically generates SQL queries for the requested fields.
	4.	Returns the constructed SQL queries as a string.

Usage Examples

Here’s how to call the procedure in Snowflake:

CALL SAINATH.SNOW.DYNAMIC_SQL_QUERY(
    'your_table_name',
    'your_json_column',
    'field1, field2, field3',
    TRUE
);

The procedure will return SQL queries based on the specified fields in the JSON structure.

Contributinge

Contributions are welcome! If you have suggestions for improvements or new features, please open an issue or submit a pull request. Ensure your code follows the project’s style guidelines.
