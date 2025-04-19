# Naming Conventions

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.

---

## Table of Contents
- [General Principles](#general-principles)
- [Table Naming Conventions](#table-naming-conventions)
  - [Bronze Rules](#bronze-rules)
  - [Silver Rules](#silver-rules)
  - [Gold Rules](#gold-rules)
- [Column Naming Conventions](#column-naming-conventions)
  - [Surrogate Keys](#surrogate-keys)
  - [Technical Columns](#technical-columns)
- [Stored Procedure](#stored-procedure)

---

## General Principles
- **Naming Conventions**: Use `snake_case`, with lowercase letters and underscores `_` to separate words.
- **Language**: Use English for all names.
- **Avoid Reserved Words**: Do not use SQL reserved words as object names.

---

## Table Naming Conventions

### Bronze Rules
- All names must start with the **source system name**, and table names must **match their original names without renaming**.  
- Format: `<sourcesystem>_<entity>`  
  - `<sourcesystem>`: Name of the source system (e.g., `crm`, `erp`)
  - `<entity>`: Exact table name from the source system  
- **Example**:  
  - `crm_customer_info` → Customer information from the CRM system

### Silver Rules
- Follows the same pattern as Bronze.  
- Format: `<sourcesystem>_<entity>`  
- **Example**:  
  - `erp_invoice_header` → Invoice header table from ERP system

### Gold Rules
- Use **business-aligned names**, starting with a **category prefix**  
- Format: `<category>_<entity>`  
  - `<category>`: Describes the role of the table (e.g., `dim`, `fact`)
  - `<entity>`: Descriptive name aligned with the business domain  
- **Examples**:  
  - `dim_customers` → Dimension table for customer data  
  - `fact_sales` → Fact table containing sales transactions

#### Glossary of Category Patterns

| Pattern   | Meaning         | Example(s)                              |
|-----------|------------------|------------------------------------------|
| `dim_`    | Dimension table   | `dim_customer`, `dim_product`            |
| `fact_`   | Fact table        | `fact_sales`                             |
| `report_` | Report table      | `report_customers`, `report_sales_monthly` |

---

## Column Naming Conventions

### Surrogate Keys
- All **primary keys in dimension tables** must use the suffix `_key`  
- Format: `<table_name>_key`  
- **Example**:  
  - `customer_key` → Surrogate key in the `dim_customers` table

### Technical Columns
- All system-generated metadata columns must use the prefix `dwh_`  
- Format: `dwh_<column_name>`  
- **Example**:  
  - `dwh_load_date` → Column to store when the record was loaded

---

## Stored Procedure
- All stored procedures for loading data must follow the pattern:  
  - `load_<layer>`  
  - `<layer>` can be `bronze`, `silver`, or `gold`

- **Examples**:  
  - `load_bronze` → Loads data into the Bronze layer  
  - `load_silver` → Loads data into the Silver layer
