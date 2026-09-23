# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Config Base Great Expectations
# This notebook defines the data quality checks for the **Base layer**.
# The config contains object-specific metadata and the technical/factual validation
# rules. The actual Great Expectations execution logic lives centrally in the
# reusable Utils notebook.
# For each Base object a separate entry is created in `BASE_GX_CONFIG`.

# CELL ********************

BASE_GX_CONFIG = {

    "public_holidays": {

        # Factual and technical metadata of the object to validate.
        "source": "histlanding",
        "schema_name": "base",
        "table_name": "public_holidays",
        "layer": "base",

        # Object-specific data quality checks.
        "gx_validation": [

            # Mandatory columns must exist in the Base DataFrame.
            {
                "expectation": "expect_column_to_exist",
                "column": "country_region",
                "severity": "critical"
            },
            {
                "expectation": "expect_column_to_exist",
                "column": "holiday_name",
                "severity": "critical"
            },
            {
                "expectation": "expect_column_to_exist",
                "column": "holiday_name_normalized",
                "severity": "critical"
            },
            {
                "expectation": "expect_column_to_exist",
                "column": "is_paidtimeoff",
                "severity": "critical"
            },
            {
                "expectation": "expect_column_to_exist",
                "column": "country_region_code",
                "severity": "critical"
            },
            {
                "expectation": "expect_column_to_exist",
                "column": "date",
                "severity": "critical"
            },

            # After casting, the column must have the expected data type.
            {
                "expectation": "expect_column_values_to_be_of_type",
                "column": "is_paidtimeoff",
                "type_": "BooleanType",
                "severity": "critical"
            },
            {
                "expectation": "expect_column_values_to_be_of_type",
                "column": "date",
                "type_": "DateType",
                "severity": "critical"
            },

            # The primary key must not be NULL.
            {
                "expectation": "expect_column_values_to_not_be_null",
                "column": "date",
                "severity": "critical"
            },
            {
                "expectation": "expect_column_values_to_not_be_null",
                "column": "country_region_code",
                "severity": "critical"
            },
            {
                "expectation": "expect_column_values_to_not_be_null",
                "column": "holiday_name_normalized",
                "severity": "critical"
            },

            # The composite primary key must be unique.
            {
                "expectation": "expect_compound_columns_to_be_unique",
                "columns": ["date", "country_region_code", "holiday_name_normalized"],
                "severity": "critical"
            }
        ]
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
