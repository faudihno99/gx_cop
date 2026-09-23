# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d5239aa0-743a-4a11-ba53-5714db5e4180",
# META       "default_lakehouse_name": "lh_sample",
# META       "default_lakehouse_workspace_id": "db1a93cb-2278-4b41-bfad-92bd940d9574",
# META       "known_lakehouses": [
# META         {
# META           "id": "d5239aa0-743a-4a11-ba53-5714db5e4180"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "e826ed2f-36f2-415c-b85f-6f0640dcd22c",
# META       "workspaceId": "0d96e512-a8bc-4aa8-83bc-3fc5409052a6"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build Base public_holidays
# This notebook builds the **Base `public_holidays` table** for the
# AdventureWorks project.
# It loads:
# - the project-specific Base Utils (`nb_utils_base`) for reusable DataFrame
#   helpers (`renaming_columns`, `data_type_casting`),
# - the Great Expectations integration layer (`nb_build_base_greatexpectation`)
#   which makes `run_base_gx()` available.
# The transformation steps are:
# 1. Read the source Delta table.
# 2. Rename columns to the Base naming convention.
# 3. Cast columns to the expected data types.
# 4. Run the configured Great Expectations data quality checks.
# 5. Write the validated DataFrame to the Base Delta table (overwrite mode).

# CELL ********************

# Import required packages
from pyspark.sql.types import BooleanType, DateType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils_base

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_build_base_greatexpectation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# The paths and mapping dictionaries for the public_holidays build.

# CELL ********************

# Define global variables
SOURCE_PATH = "Tables/histlanding/publicholidays"
TARGET_PATH = "Tables/base/public_holidays"

RENAME_MAPPING = {
    "countryOrRegion": "country_region",
    "holidayName": "holiday_name",
    "normalizeHolidayName": "holiday_name_normalized",
    "isPaidTimeOff": "is_paidtimeoff",
    "countryRegionCode": "country_region_code",
    "date": "date",
}

CAST_MAPPING = {
    "country_region": StringType(),
    "holiday_name": StringType(),
    "holiday_name_normalized": StringType(),
    "is_paidtimeoff": BooleanType(),
    "country_region_code": StringType(),
    "date": DateType(),
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Build public_holidays
# `build_public_holidays()` orchestrates the whole Base build: read, rename,
# cast, validate with Great Expectations and finally write the Base Delta table.

# CELL ********************

def build_public_holidays() -> None:
    # Read Source Table
    df = spark.read.format("delta").load(SOURCE_PATH)

    # Rename Columns
    df_renamed = renaming_columns(df, RENAME_MAPPING)

    # Data Type Casting
    df_casted = data_type_casting(df_renamed, CAST_MAPPING)

    # GX Data Quality Validation
    run_base_gx(df_casted, "public_holidays")

    # Write Target Delta Table
    df_casted.write.format("delta").mode("overwrite").save(TARGET_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Building Base-Table public_holidays
build_public_holidays()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
