# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_utils_greatexpectation
# Reusable utility functions for Data Quality checks using Great Expectations (GX)
# on already transformed Spark DataFrames.
# 
# These functions are **layer-independent** (cross-layer utility). They neither
# read nor write project-specific tables. The Spark DataFrame to validate is
# passed in by the respective build process, allowing data quality checks to
# run **in-process and before writing**.

# MARKDOWN ********************

# ## GX Validation Process Overview
# This notebook implements the standard Great Expectations validation flow:
# 1. **Data Context** (`get_datacontext_gx`) — GX runtime; ephemeral by default.
# 2. **Data Source** (`get_datasource_gx`) — Spark DataFrame source on the context.
# 3. **Data Asset + Batch Definition** (`get_dataasset_gx`) — Asset holds the DataFrame;
#    batch definition selects the whole DataFrame.
# 4. **Batch** (`_get_dataframe_batch`) — Binds the actual Spark DataFrame at runtime.
# 5. **Expectation Suite** (`_create_suite`) — Empty container for expectations.
# 6. **Expectations** (`_add_expectations_to_suite`, `_build_expectation`) — Add configured checks.
# 7. **Run Validation** (`_run_suite`) — Execute suite against the batch.
# 8. **Extract Results** (`_extract_results`) — Transform GX-native results to framework format.
# 
# The orchestrator `run_validation_gx()` calls these steps in order; build notebooks only need to call that single entry point.

# MARKDOWN ********************

# ## Packages & Parameters

# CELL ********************

import logging
import uuid
import great_expectations as gx
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List

from pyspark.sql import DataFrame

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
_logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Expectation Mapping
# Maps human-readable expectation names used in configs to their GX Expectation
# classes. Add new standardised checks here; project-specific build notebooks
# do not need to change.

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Build Expectation
# Converts a single config check entry into a GX Expectation object. Translates
# framework-specific parameter names (e.g. ``columns`` -> ``column_list``) into
# what GX expects.
# 
# Severity is a framework-level concept and is **not** passed to the GX
# expectation constructor.

# CELL ********************

def _build_expectation(validation: Dict[str, Any]):
    """
    Create a GX Expectation from a single config check entry.

    Parameters
    ----------
    validation : dict
        Configuration for one data quality check. Must contain the key
        ``expectation``. Optionally includes ``column``, ``columns``,
        ``value_set``, ``type_``. Severity is recognised but not forwarded
        to the GX expectation; it is handled by the framework's own
        evaluation logic.

    Returns
    -------
    great_expectations.expectations.Expectation
        A fully configured GX Expectation instance.

    Raises
    ------
    ValueError
        If ``validation`` is not a dict, lacks the ``expectation`` key, or the
        expectation name is not supported by the mapping.

    Example
    -------
    >>> _build_expectation({
    ...     "expectation": "expect_column_values_to_not_be_null",
    ...     "column": "customer_id"
    ... })
    """
    # Validate preconditions before building the GX expectation.
    if not isinstance(validation, dict):
        raise ValueError(
            f"Expected 'validation' to be a dict, got {type(validation).__name__}"
        )

    if "expectation" not in validation:
        raise ValueError(
            "Missing required key 'expectation' in validation configuration"
        )

    expectation_name = validation["expectation"]

    if expectation_name not in _EXPECTATION_MAP:
        raise ValueError(
            f"Unsupported expectation: '{expectation_name}'"
        )

    expectation_class = _EXPECTATION_MAP[expectation_name]

    # Map config keys to GX expectation kwargs; only pass keys that are present.
    kwargs = {}
    if "column" in validation:
        kwargs["column"] = validation["column"]
    if "columns" in validation:
        kwargs["column_list"] = validation["columns"]
    if "value_set" in validation:
        kwargs["value_set"] = validation["value_set"]
    if "type_" in validation:
        kwargs["type_"] = validation["type_"]

    return expectation_class(**kwargs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GX Data Context
# The Data Context is Great Expectations' central runtime. By default an
# **ephemeral** context is used — it exists only for the current validation
# run. All GX objects are built dynamically from the version-controlled config
# every time, so no persistent GX configuration is needed.
# 
# A **file-based** context (``mode_context="file"``) is intentionally not
# implemented yet and will be added in a future iteration. It would require a
# ``context_root_dir`` pointing to a ``great_expectations.yml`` project
# directory to persist suites, data sources, or Data Docs across runs.

# CELL ********************

def get_datacontext_gx(mode_context: str = "ephemeral"):
    """
    Create a Great Expectations Data Context.

    Parameters
    ----------
    mode_context : str, optional
        ``"ephemeral"`` (default, no persistence) or ``"file"``.
        File mode is reserved for future use and not implemented yet.

    Returns
    -------
    EphemeralDataContext
        A GX Data Context for the current validation run.

    Raises
    ------
    ValueError
        If ``mode_context`` is neither ``"ephemeral"`` nor ``"file"``.
    NotImplementedError
        If ``mode_context="file"`` is requested (not implemented yet).

    Example
    -------
    >>> context = get_datacontext_gx()
    """
    if mode_context not in ("ephemeral", "file"):
        raise ValueError(
            f"mode_context must be 'ephemeral' or 'file', got '{mode_context}'"
        )
    if mode_context == "file":
        #datacontext = gx.get_context(mode="file")
        raise NotImplementedError(
            "File-based GX DataContext is not implemented yet."
        )
    elif mode_context == "ephemeral":
        datacontext = gx.get_context(mode="ephemeral")

    _logger.info("GX Data Context created (mode_context='%s')", mode_context)
    return datacontext

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Source
# Creates or retrieves a Spark data source on the context. Only Spark
# DataFrame data sources (``type="spark_df"``) are supported for now; other
# types are reserved for future use.

# CELL ********************

def get_datasource_gx(
    context: Any,
    datasource_name: str = "dq_spark_runtime",
    datasource_type: str = "spark_df"
):
    """
    Get an existing Spark data source by name, or create one.

    Parameters
    ----------
    context
        Current GX Data Context.
    datasource_name : str, optional
        Name of the data source. Defaults to ``"dq_spark_runtime"``.
    datasource_type : str, optional
        Type of the data source. Only ``"spark_df"`` is supported for now.

    Returns
    -------
    SparkDatasource
        A GX Spark data source registered on the context.

    Raises
    ------
    ValueError
        If ``datasource_type`` is not ``"spark_df"``.

    Example
    -------
    >>> datasource = get_datasource_gx(context)
    """
    # Validate datasource_type, only support Spark DataFrame for now.
    if datasource_type != "spark_df":
        raise ValueError(
            f"datasource_type must be 'spark_df', got '{datasource_type}'"
        )

    # get-or-create: GX raises LookupError if the datasource does not exist yet.
    try:
        datasource = context.data_sources.get(datasource_name)
        _logger.info(
            "Reusing existing GX datasource '%s' (type='%s')",
            datasource_name, datasource_type
        )
        return datasource
    except LookupError:
        datasource = context.data_sources.add_spark(name=datasource_name)
        _logger.info(
            "Created GX datasource '%s' (type='%s')",
            datasource_name, datasource_type
        )
        return datasource

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Asset & Batch Definition
# Creates a DataFrame asset together with its whole-dataframe batch
# definition. The full DataFrame is validated as a single batch; the
# ephemeral context guarantees a fresh asset on every run.

# CELL ********************

def get_dataasset_gx(
    datasource: Any,
    asset_name: str,
    batch_definition_name: str = "whole_dataframe"
):
    """
    Create a DataFrame asset and a whole-dataframe batch definition.

    Parameters
    ----------
    datasource
        GX Spark data source.
    asset_name : str
        Name of the DataFrame asset. Must be unique within the current
        GX Data Context — the caller is responsible for ensuring uniqueness
        (e.g. by suffixing a run ID).
    batch_definition_name : str, optional
        Name of the batch definition. Defaults to ``"whole_dataframe"``.

    Returns
    -------
    tuple
        ``(data_asset, batch_definition)`` where ``data_asset`` is a GX
        DataFrame asset and ``batch_definition`` its whole-dataframe batch
        definition.

    Example
    -------
    >>> asset, batch_definition = get_dataasset_gx(ds, "base_customer_20241001")
    """
    # Register a DataFrame asset on the Spark datasource.
    data_asset = datasource.add_dataframe_asset(name=asset_name)

    # Define a single batch containing the whole DataFrame.
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name=batch_definition_name
    )
    _logger.info(
        "GX data asset '%s' created with batch definition '%s'",
        asset_name, batch_definition_name
    )
    return data_asset, batch_definition

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Batch Retrieval
# Binds the in-memory Spark DataFrame to the batch definition at runtime.

# CELL ********************

def _get_dataframe_batch(
    batch_definition: Any,
    df: DataFrame
):
    """
    Retrieve a GX Batch by passing the actual Spark DataFrame at runtime.

    Parameters
    ----------
    batch_definition
        GX batch definition (whole-dataframe style).
    df : pyspark.sql.DataFrame
        Transformed Spark DataFrame to validate.

    Returns
    -------
    great_expectations.core.batch.Batch
        A GX Batch pointing to the provided Spark DataFrame.

    Raises
    ------
    ValueError
        If ``df`` is not a Spark DataFrame.

    Example
    -------
    >>> batch = _get_dataframe_batch(batch_def, my_df)
    """
    if not isinstance(df, DataFrame):
        raise ValueError(
            f"Expected 'df' to be a pyspark.sql.DataFrame, "
            f"got {type(df).__name__}"
        )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )
    _logger.info(
        "GX batch bound to DataFrame (%d columns)",
        len(df.columns)
    )
    return batch

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Expectation Suite
# Creates a new Expectation Suite for the current object. The suite is always
# created anew because the ephemeral context provides no persistence.

# CELL ********************

def _create_suite(
    context: Any,
    suite_name: str
):
    """
    Create and register a new Expectation Suite.

    The suite must be registered with the context **before** expectations are
    added to it. Otherwise GX silently discards all expectations on
    ``context.suites.add()`` and returns a different handle.

    Parameters
    ----------
    context
        Current GX Data Context.
    suite_name : str
        Name of the Expectation Suite.

    Returns
    -------
    great_expectations.ExpectationSuite
        A registered, empty GX Expectation Suite.

    Example
    -------
    >>> suite = _create_suite(context, "dq_base_customer")
    """
    suite = gx.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)
    _logger.info("GX expectation suite '%s' created", suite_name)
    return suite

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add Expectations to Suite
# Populates a registered suite with all configured checks.

# CELL ********************

def _add_expectations_to_suite(suite: Any, gx_validation: List[Dict[str, Any]]):
    """Populate a registered suite with all configured expectations."""
    if not isinstance(gx_validation, list):
        raise ValueError(
            f"Expected 'gx_validation' to be a list, got {type(gx_validation).__name__}"
        )

    for validation in gx_validation:
        expectation = _build_expectation(validation)

        column = validation.get("column") or ", ".join(
            validation.get("columns", [])
        )
        _logger.info(
            "Added expectation '%s' on '%s'",
            validation.get("expectation"), column
        )
        suite.add_expectation(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run Validation
# Executes the suite against the batch.

# CELL ********************

def _run_suite(
    batch: Any,
    suite: Any,
    result_format: str = "SUMMARY"
):
    """
    Validate the batch against the expectation suite.

    Parameters
    ----------
    batch
        GX Batch (data to validate).
    suite
        GX Expectation Suite (checks to run).
    result_format : str, optional
        GX result format, defaults to ``"SUMMARY"``.

    Returns
    -------
    ValidateExpectationResults
        Native GX validation results.

    Example
    -------
    >>> results = _run_suite(batch, suite)
    """
    return batch.validate(suite, result_format=result_format)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Results Extraction
# Translates native GX validation results into a **standardised framework
# format** independent of GX.
# 
# **Identity-based matching.** GX does *not* return results in the order
# expectations were added: expectations are grouped by column, and errored
# metrics are moved to the front. Matching by position is therefore
# unreliable.
# This implementation builds a lookup key
# ``(expectation_type, column_or_columns)`` from each check config and maps
# each GX result to its originating check by identity. A per-key queue handles
# identical checks (same type, same column) correctly.
# 
# **Metric errors** (e.g. missing column, type mismatch) are reported
# separately: ``success=False`` and an ``error_message`` describing the cause,
# whereas genuine data failures carry ``unexpected_count`` / ``unexpected_percent``
# / ``observed_value``.

# CELL ********************

def _check_identity_key(validation: Dict[str, Any]):
    """Build a stable lookup key ``(expectation_type, columns)`` for a config entry."""
    cols = validation.get("column", validation.get("columns", ""))
    if isinstance(cols, list):
        cols = tuple(sorted(cols))
    return (validation["expectation"], cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _result_identity_key(result: Any):
    """
    Build a stable identity key from a GX validation result.

    Parameters
    ----------
    result
        One entry from ``validation_results.results`` (a GX ExpectationResult
        or similar).

    Returns
    -------
    tuple
        ``(expectation_type, column_identifier)`` matching the key built by
        ``_check_identity_key()``.
    """
    config = result.expectation_config
    cols = config.kwargs.get("column", config.kwargs.get("column_list", ""))
    if isinstance(cols, list):
        cols = tuple(sorted(cols))
    return (config.type, cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _get_result_value(result_data: Any, key: str, default: Any = None):
    """
    Read a field from a GX result payload.

    GX result payloads may be plain dicts (real GX) or objects such as
    ``SimpleNamespace`` (mocks). This helper normalises both access patterns.

    Parameters
    ----------
    result_data
        The result payload, or ``None``.
    key : str
        Field name to read.
    default
        Value returned when ``key`` is absent.

    Returns
    -------
    Any
        The field value, or ``default``.
    """
    if result_data is None:
        return default
    if isinstance(result_data, dict):
        return result_data.get(key, default)
    return getattr(result_data, key, default)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _extract_results(
    validation_results: Any,
    gx_validation: List[Dict[str, Any]],
    config_entry: Dict[str, Any],
    object_name: str,
    run_id: str,
    run_timestamp: datetime
):
    """Transform GX-native results into the standardised framework result format."""
    if not hasattr(validation_results, "results"):
        raise ValueError(
            "'validation_results' must have a 'results' attribute"
        )

    check_lookup = defaultdict(list)
    for validation in gx_validation:
        check_lookup[_check_identity_key(validation)].append(validation)

    results = []

    for gx_result in validation_results.results:
        identity_key = _result_identity_key(gx_result)

        matched_validation = None
        queue = check_lookup.get(identity_key, [])
        if queue:
            matched_validation = queue.pop(0)

        if matched_validation is None:
            column_name = (
                gx_result.expectation_config.kwargs.get("column", "")
                or gx_result.expectation_config.kwargs.get("column_list", "")
            )
        else:
            column_name = matched_validation.get(
                "column",
                ", ".join(matched_validation.get("columns", []))
            )

        gx_result_data = _get_result_value(gx_result, "result", {})
        is_success = gx_result.success

        if is_success:
            status = "PASS"
            error_message = None
            unexpected_count = _get_result_value(gx_result_data, "unexpected_count", 0)
            unexpected_percent = _get_result_value(gx_result_data, "unexpected_percent", 0.0)
            observed_value = str(_get_result_value(gx_result_data, "observed_value", ""))
        elif not gx_result.result:
            status = "ERROR"
            error_message = _format_metric_error(gx_result)
            unexpected_count = 0
            unexpected_percent = 0.0
            observed_value = ""
        else:
            status = "FAIL"
            error_message = None
            unexpected_count = _get_result_value(gx_result_data, "unexpected_count", 0)
            unexpected_percent = _get_result_value(gx_result_data, "unexpected_percent", 0.0)
            observed_value = str(_get_result_value(gx_result_data, "observed_value", ""))

        if status == "PASS":
            _logger.info(
                "DQ check PASSED - type='%s' column='%s' (object='%s')",
                identity_key[0], column_name, object_name
            )
        elif status == "ERROR":
            _logger.warning(
                "DQ check ERROR - type='%s' column='%s' (object='%s'): %s",
                identity_key[0], column_name, object_name, error_message
            )
        else:
            _logger.warning(
                "DQ check FAILED - type='%s' column='%s' (object='%s'): "
                "unexpected_count=%s unexpected_percent=%s observed_value=%s",
                identity_key[0], column_name, object_name,
                unexpected_count, unexpected_percent, observed_value
            )

        results.append({
            "run_id": run_id,
            "run_timestamp": run_timestamp,
            "layer": config_entry["layer"],
            "source": config_entry.get("source"),
            "schema_name": config_entry.get("schema_name"),
            "table_name": config_entry.get("table_name"),
            "object_name": object_name,
            "column_name": column_name,
            "expectation_type": identity_key[0],
            "success": is_success,
            "status": status,
            "severity": matched_validation.get("severity", "critical")
                if matched_validation else "critical",
            "error_message": error_message,
            "unexpected_count": unexpected_count,
            "unexpected_percent": unexpected_percent,
            "observed_value": observed_value,
        })

    # Emit ERROR rows for any expectations that GX did not return a result for.
    for identity_key, queue in check_lookup.items():
        for missing in queue:
            column_name = missing.get(
                "column", ", ".join(missing.get("columns", []))
            )
            _logger.warning(
                "DQ check ERROR - type='%s' column='%s' (object='%s'): "
                "GX did not return a result for this expectation",
                identity_key[0], column_name, object_name
            )
            results.append({
                "run_id": run_id,
                "run_timestamp": run_timestamp,
                "layer": config_entry["layer"],
                "source": config_entry.get("source"),
                "schema_name": config_entry.get("schema_name"),
                "table_name": config_entry.get("table_name"),
                "object_name": object_name,
                "column_name": column_name,
                "expectation_type": identity_key[0],
                "success": False,
                "status": "ERROR",
                "severity": missing.get("severity", "critical"),
                "error_message": "GX did not return a result for this expectation",
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "observed_value": "",
            })

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _format_metric_error(gx_result: Any):
    """
    Extract a human-readable error message from a GX result whose metric
    never evaluated.

    Parameters
    ----------
    gx_result
        A GX result where ``success is False`` and ``result`` is falsy.

    Returns
    -------
    str
        Concatenated exception messages, or ``"Unknown metric error"``.
    """
    if not hasattr(gx_result, "exception_info"):
        return "Unknown metric error"
    exc_info = gx_result.exception_info
    if exc_info is None:
        return "Unknown metric error"
    if isinstance(exc_info, dict):
        messages = [
            getattr(v, "exception_message", str(v))
            for v in exc_info.values()
        ]
    elif isinstance(exc_info, list):
        messages = [
            getattr(item, "exception_message", str(item))
            for item in exc_info
        ]
    elif hasattr(exc_info, "exception_message"):
        messages = [exc_info.exception_message]
    else:
        return "Unknown metric error"
    return " | ".join(messages) if messages else "Unknown metric error"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run Validation
# ``run_validation_gx()`` is the main entry point. It orchestrates a complete
# data quality run by calling each single-purpose function in order:
# 1. Create a GX Data Context.
# 2. Register a Spark data source.
# 3. Define a DataFrame asset with a batch definition.
# 4. Bind the Spark DataFrame to the batch.
# 5. Create an Expectation Suite.
# 6. Populate the suite with configured checks.
# 7. Validate and extract standardised results.

# CELL ********************

def run_validation_gx(
    df: DataFrame,
    config_entry: Dict[str, Any],
    object_name: str,
    run_id: str
):
    """
    Orchestrate a complete Great Expectations validation run.

    This is the single entry point for build notebooks. It performs all
    steps of the GX validation pipeline in order:

    1. Validate all input parameters (precondition checks).
    2. Create an ephemeral GX Data Context.
    3. Get or create a Spark DataFrame data source.
    4. Create a per-run DataFrame asset and whole-dataframe batch definition.
    5. Bind the passed Spark DataFrame to the batch.
    6. Create an Expectation Suite and populate it with configured checks.
    7. Execute validation and extract standardised results.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Transformed DataFrame to validate.
    config_entry : dict
        Object-specific DQ configuration. Must contain ``layer`` (str) and
        ``gx_validation`` (list). Optional metadata: ``source``,
        ``schema_name``, ``table_name``.
    object_name : str
        Logical name of the object being validated (e.g. ``"customer"``).
        Used in suite naming and audit columns.
    run_id : str
        Unique identifier for this validation run (e.g. pipeline run ID).

    Returns
    -------
    tuple
        ``(validation_results, results)`` where ``validation_results`` is the
        native GX ``ValidateExpectationResults`` object and ``results`` is a
        list of standardised dicts ready for writing into a DQ result table.

    Raises
    ------
    ValueError
        If any input parameter fails type or value validation.
    """

    # ---- Precondition: df must be a Spark DataFrame ----
    if not isinstance(df, DataFrame):
        raise ValueError(
            f"Expected 'df' to be a pyspark.sql.DataFrame, "
            f"got {type(df).__name__}"
        )

    # ---- Precondition: config_entry must be a dict with required keys ----
    if not isinstance(config_entry, dict):
        raise ValueError(
            f"Expected 'config_entry' to be a dict, "
            f"got {type(config_entry).__name__}"
        )

    if "gx_validation" not in config_entry:
        raise ValueError(
            "Missing required key 'gx_validation' in config_entry"
        )

    if "layer" not in config_entry:
        raise ValueError(
            "Missing required key 'layer' in config_entry"
        )

    if not isinstance(config_entry["layer"], str) or not config_entry["layer"]:
        raise ValueError(
            "config_entry['layer'] must be a non-empty string"
        )

    if not isinstance(config_entry["gx_validation"], list):
        raise ValueError(
            f"Expected 'gx_validation' to be a list, "
            f"got {type(config_entry['gx_validation']).__name__}"
        )

    # ---- Precondition: object_name and run_id must be non-empty strings ----
    if not object_name or not isinstance(object_name, str):
        raise ValueError(
            f"Expected 'object_name' to be a non-empty string, "
            f"got '{object_name}'"
        )

    if not run_id or not isinstance(run_id, str):
        raise ValueError(
            f"Expected 'run_id' to be a non-empty string, "
            f"got '{run_id}'"
        )

    # ---- Resolve runtime values ----
    gx_validation = config_entry["gx_validation"]
    layer = config_entry["layer"]
    run_timestamp = datetime.now(timezone.utc)

    _logger.info("Starting GX validation for '%s' (layer=%s)", object_name, layer)

    # ---- Step 1: Create GX Data Context (ephemeral) ----
    context = get_datacontext_gx()

    # ---- Step 2: Get or create Spark Data Source ----
    datasource = get_datasource_gx(context=context)

    # ---- Step 3: Create Data Asset + Batch Definition (unique name per run) ----
    # Using a unique asset name avoids collisions when reusing the same
    # ephemeral context across multiple validations.
    asset_name = f"{layer}_{object_name}_{run_id}"
    _, batch_definition = get_dataasset_gx(
        datasource=datasource,
        asset_name=asset_name
    )

    # ---- Step 4: Bind the runtime Spark DataFrame to the batch ----
    batch = _get_dataframe_batch(
        batch_definition=batch_definition,
        df=df
    )

    # ---- Step 5: Create Expectation Suite ----
    suite_name = f"dq_{layer}_{object_name}"
    suite = _create_suite(
        context=context,
        suite_name=suite_name
    )

    # ---- Step 6: Populate suite with configured expectations ----
    _add_expectations_to_suite(suite=suite, gx_validation=gx_validation)
    _logger.info("Expectation suite '%s' ready with %d expectations", suite_name, len(gx_validation))

    # ---- Step 7: Run validation (execute suite against batch) ----
    validation_results = _run_suite(
        batch=batch,
        suite=suite
    )
    _logger.info("GX validation executed for '%s'", object_name)

    # ---- Step 8: Extract standardised results ----
    results = _extract_results(
        validation_results=validation_results,
        gx_validation=gx_validation,
        config_entry=config_entry,
        object_name=object_name,
        run_id=run_id,
        run_timestamp=run_timestamp
    )
    _logger.info("Extracted %d result rows for '%s'", len(results), object_name)

    summary_statuses = defaultdict(int)
    for row in results:
        summary_statuses[row["status"]] += 1
    _logger.info(
        "DQ run summary for '%s' - total=%d pass=%d fail=%d error=%d success=%s",
        object_name,
        len(results),
        summary_statuses.get("PASS", 0),
        summary_statuses.get("FAIL", 0),
        summary_statuses.get("ERROR", 0),
        validation_results.success
    )

    return validation_results, results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Base Build Runner
# ``run_base_gx()`` is a convenience wrapper for Base-layer build notebooks.
# It expects ``BASE_GX_CONFIG`` to be available in the current session, e.g. by
# loading ``%run nb_config_base_greatexpectation``. It then orchestrates a
# complete validation via ``run_validation_gx()`` and raises a ``RuntimeError``
# if at least one configured data quality check fails — preventing faulty data
# from being written to the Base table.

# CELL ********************

def run_base_gx(df, object_name):
    """
    Run the configured Great Expectations checks for a Base object.

    Parameters
    ----------
    df
        Already transformed Spark DataFrame to be validated immediately
        before writing.

    object_name : str
        Name of the object as defined in `BASE_GX_CONFIG`.

    Returns
    -------
    tuple
        Native Great Expectations validation results as well as the
        standardised result list of the framework.

    Raises
    ------
    ValueError
        Raised if no GX configuration exists for the given object.

    RuntimeError
        Raised if at least one configured data quality check fails.
    """

    # Ensure that a config exists for the given object.
    if object_name not in BASE_GX_CONFIG:
        raise ValueError(
            f"No Base GX configuration found "
            f"for object '{object_name}'."
        )

    # Generate a unique ID for the current data quality run.
    run_id = str(uuid.uuid4())

    # Load the object-specific Base config.
    config_entry = BASE_GX_CONFIG[
        object_name
    ]

    # Run the generic GX validation from the Utils.
    validation_results, results = run_validation_gx(
        df=df,
        config_entry=config_entry,
        object_name=object_name,
        run_id=run_id
    )

    # Stop the build process if data quality fails.
    if not validation_results.success:
        raise RuntimeError(
            f"Base Data Quality validation failed "
            f"for object '{object_name}'. "
            f"Run ID: {run_id}"
        )

    # Return native GX results and standardised framework results.
    return validation_results, results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
