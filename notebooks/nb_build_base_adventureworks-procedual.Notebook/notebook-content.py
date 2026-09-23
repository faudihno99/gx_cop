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
# This notebook executes the process **procedurally, step by step** — instead of
# calling the orchestrating functions. Every Great Expectations concept (Data
# Context, Data Source, Data Asset & Batch Definition, Batch, Expectation Suite,
# Expectations, Validation, Results Extraction) lives in its own cell with a
# `#### <concept-gx>` markdown header so the full DQ process becomes visible.


# CELL ********************

# Import required packages
from pyspark.sql.types import BooleanType, DateType, StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import great_expectations as gx
import uuid
from datetime import datetime, timezone
from collections import defaultdict

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

# MARKDOWN ********************

# ## Helper functions
# Two small, reusable helpers used by the procedural flow below.

# CELL ********************

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

# ## Read source table
# Read the source Delta table from the `histlanding` layer.

# CELL ********************

df = spark.read.format("delta").load(SOURCE_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Rename and Cast columns
# Apply the Base naming convention via `renaming_columns()`.
# Cast columns to the expected Base data types via `data_type_casting()`.

# CELL ********************

df_renamed = renaming_columns(df, RENAME_MAPPING)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_casted = data_type_casting(df_renamed, CAST_MAPPING)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Great Expectations Validation
# The data quality run executes each Great Expectations concept step by step.
# The configured checks are taken from `BASE_GX_CONFIG` (loaded via
# `%run nb_config_base_greatexpectation`).

# MARKDOWN ********************

# #### Run Parameters
# A unique run ID and a UTC timestamp identify this data quality run.

# CELL ********************

run_id = str(uuid.uuid4())
run_timestamp = datetime.now(timezone.utc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Data Context
# The **Data Context** is Great Expectations' central runtime. An ephemeral
# context is created per run — it exists only for the current validation and
# provides no persistence.

# CELL ********************

context = gx.get_context(mode="ephemeral")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show Empty Data Context
print(context)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Data Source
# The **Data Source** connects GX to the runtime environment. Only Spark
# DataFrame data sources are supported (`type="spark_df"`).

# CELL ********************

datasource = context.data_sources.add_spark(name="dq_spark_runtime")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Data Asset & Batch Definition
# The **Data Asset** holds the DataFrame to validate; the **Batch Definition**
# selects which part of the asset is validated — here the whole DataFrame.

# CELL ********************

asset_name = f"base_public_holidays_{run_id}"
data_asset = datasource.add_dataframe_asset(name=asset_name)
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    name="whole_dataframe"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Batch
# The **Batch** binds the actual in-memory Spark DataFrame to the batch
# definition at runtime. This is the data being validated.

# CELL ********************

batch = batch_definition.get_batch(
    batch_parameters={"dataframe": df_casted}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Expectation Suite
# The **Expectation Suite** is a container for the configured checks. It must
# be registered with the context **before** expectations are added.

# CELL ********************

suite_name = f"dq_base_public_holidays"
suite = gx.ExpectationSuite(name=suite_name)
suite = context.suites.add(suite)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show Filled Data Context. 
# Now contains Data Source (spark-df), Data-Asset (public-holiday) & Batch-Definition (Whole-Dataframe), 
print(context)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Expectations
# Map the human-readable check names from `BASE_GX_CONFIG` to GX expectation
# classes, translate framework parameter names (e.g. `columns` -> `column_list`)
# and add every configured check to the suite.

# CELL ********************

_EXPECTATION_MAP = {
    "expect_column_values_to_not_be_null":
        gx.expectations.ExpectColumnValuesToNotBeNull,
    "expect_column_values_to_be_unique":
        gx.expectations.ExpectColumnValuesToBeUnique,
    "expect_compound_columns_to_be_unique":
        gx.expectations.ExpectCompoundColumnsToBeUnique,
    "expect_column_to_exist":
        gx.expectations.ExpectColumnToExist,
    "expect_column_values_to_be_of_type":
        gx.expectations.ExpectColumnValuesToBeOfType,
    "expect_column_values_to_be_in_set":
        gx.expectations.ExpectColumnValuesToBeInSet,
    "expect_column_distinct_values_to_be_in_set":
        gx.expectations.ExpectColumnDistinctValuesToBeInSet,
}

config_entry = BASE_GX_CONFIG["public_holidays"]
gx_validation = config_entry["gx_validation"]

for validation in gx_validation:
    # Get Expectation from Config
    expectation_name = validation["expectation"]
    expectation_class = _EXPECTATION_MAP[expectation_name]

    # Define Parameters for respective Expectation
    kwargs = {}
    if "column" in validation:
        kwargs["column"] = validation["column"]
    if "columns" in validation:
        kwargs["column_list"] = validation["columns"]
    if "value_set" in validation:
        kwargs["value_set"] = validation["value_set"]
    if "type_" in validation:
        kwargs["type_"] = validation["type_"]

    expectation = expectation_class(**kwargs)
    suite.add_expectation(expectation)

    column = validation.get("column") or ", ".join(
        validation.get("columns", [])
    )
    print(f"Added expectation '{expectation_name}' on '{column}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Validation
# __Execute__ the populated **Expectation Suite** against the **Batch**.

# CELL ********************

validation_results = batch.validate(suite, result_format="SUMMARY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Results Extraction
# Translate the native GX results into the standardised framework result format
# using `_extract_results()` from `nb_utils_greatexpectation`, then print a
# per-check status (`PASS`, `FAIL`, `ERROR`) and a run summary.

# CELL ********************

config_entry = BASE_GX_CONFIG["public_holidays"]

validation_results, results = _extract_results(
    validation_results=validation_results,
    gx_validation=config_entry["gx_validation"],
    config_entry=config_entry,
    object_name="public_holidays",
    run_id=run_id,
    run_timestamp=run_timestamp
)

for row in results:
    if row["status"] == "PASS":
        print(
            f"[{row['status']}] {row['expectation_type']} "
            f"on '{row['column_name']}'"
        )
    elif row["status"] == "ERROR":
        print(
            f"[{row['status']}] {row['expectation_type']} "
            f"on '{row['column_name']}': {row['error_message']}"
        )
    else:
        print(
            f"[{row['status']}] {row['expectation_type']} "
            f"on '{row['column_name']}': "
            f"unexpected_count={row['unexpected_count']} "
            f"unexpected_percent={row['unexpected_percent']} "
            f"observed_value={row['observed_value']}"
        )

summary_statuses = defaultdict(int)
for row in results:
    summary_statuses[row["status"]] += 1

print(
    "DQ run summary - "
    f"total={len(results)} "
    f"pass={summary_statuses.get('PASS', 0)} "
    f"fail={summary_statuses.get('FAIL', 0)} "
    f"error={summary_statuses.get('ERROR', 0)} "
    f"success={validation_results.success}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Result Check
# Stop the build process if at least one configured data quality check fails —
# preventing faulty data from being written to the Base table.

# CELL ********************

if not validation_results.success:
    raise RuntimeError(
        f"Base Data Quality validation failed for object 'public_holidays'. "
        f"Run ID: {run_id}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write Base table
# Persist the validated DataFrame to the Base Delta table (overwrite mode).

# CELL ********************

df_casted.write.format("delta").mode("overwrite").save(TARGET_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
