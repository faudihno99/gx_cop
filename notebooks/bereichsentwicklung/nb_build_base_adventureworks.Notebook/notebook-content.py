# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7dac0dcd-0c57-4bd3-8630-01ceab3cccaa",
# META       "default_lakehouse_name": "lh_cop",
# META       "default_lakehouse_workspace_id": "2657fe4d-c298-4d4e-856c-78c3306cfe8d",
# META       "known_lakehouses": [
# META         {
# META           "id": "7dac0dcd-0c57-4bd3-8630-01ceab3cccaa"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "ed6a0cff-34a1-a975-41f3-5dca9273c293",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
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
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import great_expectations as gx

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils_greatexpectation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_config_base_greatexpectation 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sample Utils
def renaming_columns(df: DataFrame, rename_dict: dict) -> DataFrame:
    """Rename DataFrame columns based on a mapping of {source: target}.

    Fails fast if a source column is not present in the DataFrame.
    """
    source_columns = set(df.columns)
    missing_columns = set(rename_dict.keys()) - source_columns
    if missing_columns:
        raise ValueError(f"Columns not found in DataFrame: {sorted(missing_columns)}")

    for source_name, target_name in rename_dict.items():
        df = df.withColumnRenamed(source_name, target_name)
    return df

def data_type_casting(df: DataFrame, cast_dict: dict) -> DataFrame:
    """Cast DataFrame columns to target data types based on {column: type}.

    Fails fast if a column is not present in the DataFrame.
    """
    source_columns = set(df.columns)
    missing_columns = set(cast_dict.keys()) - source_columns
    if missing_columns:
        raise ValueError(f"Columns not found in DataFrame: {sorted(missing_columns)}")

    for column_name, data_type in cast_dict.items():
        df = df.withColumn(column_name, col(column_name).cast(data_type))
    return df


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

# MARKDOWN ********************

# ## Parameters
# The paths and mapping dictionaries for the public_holidays build.

# CELL ********************

# Define global variables
SOURCE_PATH = "Tables/histlanding/sample_public_holidays"
TARGET_PATH = "Tables/base/sample_public_holidays"

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

# Building Base-Table public_holidays
build_public_holidays()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
