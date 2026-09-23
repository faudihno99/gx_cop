# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "e826ed2f-36f2-415c-b85f-6f0640dcd22c",
# META       "workspaceId": "0d96e512-a8bc-4aa8-83bc-3fc5409052a6"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_test_greatexpectation
# Procedural unit tests for the Great Expectations Utils notebook.
# Uses simple assertion-based helpers instead of unittest classes.
# Designed to run in **Microsoft Fabric** where a global ``spark`` session exists.
# Pure Python tests run everywhere; Spark-dependent tests require Fabric runtime.

# MARKDOWN ********************

# ## Load Utils Notebook
# Load the reusable Great Expectations Utils notebook into this session so its
# functions are available to the tests below.

# CELL ********************

%run nb_utils_greatexpectation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports & Setup

# CELL ********************

import logging
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List
from uuid import uuid4

from pyspark.sql import DataFrame, Row, SparkSession

import great_expectations as gx

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_gx_utils")

passed_count = 0
failed_count = 0


def assert_test(test_name: str, condition: bool, detail: str = ""):
    global passed_count, failed_count
    if condition:
        passed_count += 1
        logger.info("  [PASS] %s", test_name)
    else:
        failed_count += 1
        logger.warning("  [FAIL] %s%s", test_name,
                        " - %s" % detail if detail else "")


def print_header(title: str):
    logger.info("")
    logger.info("=" * 70)
    logger.info("  %s", title)
    logger.info("=" * 70)


def print_summary():
    total = passed_count + failed_count
    logger.info("")
    logger.info("=" * 70)
    logger.info("  SUMMARY: %d/%d tests passed", passed_count, total)
    logger.info("=" * 70)
    if failed_count > 0:
        logger.warning("  !  %d test(s) failed - please check.", failed_count)
    else:
        logger.info("  All tests passed!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Shared Test Data

# CELL ********************

SAMPLE_VALIDATION_NOT_NULL = {
    "expectation": "expect_column_values_to_not_be_null",
    "column": "customer_id",
    "severity": "critical",
}

SAMPLE_VALIDATION_EXIST = {
    "expectation": "expect_column_to_exist",
    "column": "customer_id",
}

SAMPLE_VALIDATION_UNIQUE = {
    "expectation": "expect_column_values_to_be_unique",
    "column": "customer_id",
}

SAMPLE_CONFIG = {
    "layer": "base",
    "source": "sap",
    "schema_name": "sales",
    "table_name": "customer",
    "gx_validation": [SAMPLE_VALIDATION_NOT_NULL, SAMPLE_VALIDATION_UNIQUE],
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: _build_expectation

# CELL ********************

print_header("Test: _build_expectation")

expect = _build_expectation(SAMPLE_VALIDATION_NOT_NULL)
assert_test(
    "_build_expectation: not_null returns correct GX type",
    isinstance(expect, gx.expectations.ExpectColumnValuesToNotBeNull)
)

expect = _build_expectation(SAMPLE_VALIDATION_UNIQUE)
assert_test(
    "_build_expectation: unique returns correct GX type",
    isinstance(expect, gx.expectations.ExpectColumnValuesToBeUnique)
)

expect = _build_expectation(SAMPLE_VALIDATION_EXIST)
assert_test(
    "_build_expectation: to_exist returns correct GX type",
    isinstance(expect, gx.expectations.ExpectColumnToExist)
)

try:
    _build_expectation({"expectation": "nonexistent_check"})
    assert_test("_build_expectation: unsupported expectation raises", False)
except ValueError as e:
    assert_test("_build_expectation: unsupported expectation raises",
                "Unsupported" in str(e))

try:
    _build_expectation({"column": "x"})
    assert_test("_build_expectation: missing expectation key raises", False)
except ValueError as e:
    assert_test("_build_expectation: missing expectation key raises",
                "expectation" in str(e))

try:
    _build_expectation("not_a_dict")
    assert_test("_build_expectation: not dict raises", False)
except ValueError as e:
    assert_test("_build_expectation: not dict raises",
                "dict" in str(e))

expect = _build_expectation(SAMPLE_VALIDATION_NOT_NULL)
assert_test(
    "_build_expectation: severity does not leak into GX kwargs",
    isinstance(expect, gx.expectations.ExpectColumnValuesToNotBeNull)
)

validation_compound = {
    "expectation": "expect_compound_columns_to_be_unique",
    "columns": ["a", "b"],
}
expect = _build_expectation(validation_compound)
assert_test(
    "_build_expectation: columns mapped to column_list",
    isinstance(expect, gx.expectations.ExpectCompoundColumnsToBeUnique)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: Identity Keys

# CELL ********************

print_header("Test: Identity Keys")

key = _check_identity_key(SAMPLE_VALIDATION_NOT_NULL)
assert_test("_check_identity_key: with column",
            key == ("expect_column_values_to_not_be_null", "customer_id"))

validation_unsorted = {
    "expectation": "expect_compound_columns_to_be_unique",
    "columns": ["b", "a"]
}
key = _check_identity_key(validation_unsorted)
assert_test("_check_identity_key: compound columns sorted",
            key == ("expect_compound_columns_to_be_unique", ("a", "b")))

key = _check_identity_key({"expectation": "expect_table_row_count_to_be_between"})
assert_test("_check_identity_key: no column",
            key == ("expect_table_row_count_to_be_between", ""))

# _result_identity_key tests
mock_result = SimpleNamespace(
    expectation_config=SimpleNamespace(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "customer_id"}
    )
)
assert_test("_result_identity_key: with column",
            _result_identity_key(mock_result) == ("expect_column_values_to_not_be_null", "customer_id"))

mock_result_compound = SimpleNamespace(
    expectation_config=SimpleNamespace(
        type="expect_compound_columns_to_be_unique",
        kwargs={"column_list": ["b", "a"]}
    )
)
assert_test("_result_identity_key: compound columns sorted",
            _result_identity_key(mock_result_compound) == ("expect_compound_columns_to_be_unique", ("a", "b")))

mock_result_no_col = SimpleNamespace(
    expectation_config=SimpleNamespace(
        type="expect_table_row_count_to_be_between",
        kwargs={}
    )
)
assert_test("_result_identity_key: no column/column_list",
            _result_identity_key(mock_result_no_col) == ("expect_table_row_count_to_be_between", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: _format_metric_error

# CELL ********************

print_header("Test: _format_metric_error")

mock_dict = SimpleNamespace(
    exception_info={
        "metric_1": SimpleNamespace(exception_message="Column 'nope' not found")
    }
)
msg = _format_metric_error(mock_dict)
assert_test("_format_metric_error: dict with exception_message",
            "not found" in msg)

mock_list = SimpleNamespace(
    exception_info=[
        SimpleNamespace(exception_message="First error"),
        SimpleNamespace(exception_message="Second error"),
    ]
)
msg = _format_metric_error(mock_list)
assert_test("_format_metric_error: list of exceptions",
            "First error" in msg and "Second error" in msg)

mock_single = SimpleNamespace(
    exception_info=SimpleNamespace(exception_message="Single error")
)
msg = _format_metric_error(mock_single)
assert_test("_format_metric_error: single object",
            "Single error" in msg)

mock_none = SimpleNamespace(exception_info=None)
msg = _format_metric_error(mock_none)
assert_test("_format_metric_error: None exception_info",
            msg == "Unknown metric error")

mock_missing = SimpleNamespace()
msg = _format_metric_error(mock_missing)
assert_test("_format_metric_error: missing exception_info attribute",
            msg == "Unknown metric error")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: get_datacontext_gx

# CELL ********************

print_header("Test: get_datacontext_gx")

ctx = get_datacontext_gx()
assert_test("get_datacontext_gx: ephemeral returns context", ctx is not None)
assert_test("get_datacontext_gx: context has data_sources",
            ctx.data_sources is not None)

ctx2 = get_datacontext_gx(mode_context="ephemeral")
assert_test("get_datacontext_gx: explicit ephemeral", ctx2 is not None)

try:
    get_datacontext_gx(mode_context="invalid")
    assert_test("get_datacontext_gx: unsupported mode raises", False)
except ValueError as e:
    assert_test("get_datacontext_gx: unsupported mode raises",
                "mode_context" in str(e))

try:
    get_datacontext_gx(mode_context="file")
    assert_test("get_datacontext_gx: file mode raises NotImplementedError", False)
except NotImplementedError:
    assert_test("get_datacontext_gx: file mode raises NotImplementedError", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: get_datasource_gx

# CELL ********************

print_header("Test: get_datasource_gx")

ctx = get_datacontext_gx()
ds = get_datasource_gx(ctx)
assert_test("get_datasource_gx: creates data source", ds is not None)

ds1 = get_datasource_gx(ctx)
ds2 = get_datasource_gx(ctx)
assert_test("get_datasource_gx: reuses existing", ds1.name == ds2.name)

ds_custom = get_datasource_gx(ctx, datasource_name="custom_ds")
assert_test("get_datasource_gx: custom name", ds_custom.name == "custom_ds")

try:
    get_datasource_gx(ctx, datasource_type="sql")
    assert_test("get_datasource_gx: unsupported type raises", False)
except ValueError as e:
    assert_test("get_datasource_gx: unsupported type raises",
                "spark_df" in str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: get_dataasset_gx

# CELL ********************

print_header("Test: get_dataasset_gx")

ctx = get_datacontext_gx()
ds = get_datasource_gx(ctx, "dq_spark_runtime")

asset_name = "test_asset_" + uuid4().hex[:8]
asset, bd = get_dataasset_gx(ds, asset_name)
assert_test("get_dataasset_gx: creates asset", asset is not None)
assert_test("get_dataasset_gx: creates batch definition", bd is not None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: _get_dataframe_batch (requires Spark)

# CELL ********************

print_header("Test: _get_dataframe_batch")

if spark is not None:
    df = spark.createDataFrame(
        [Row(customer_id=1, name="Alice")],
        schema="customer_id INT, name STRING"
    )
    ctx = get_datacontext_gx()
    ds = get_datasource_gx(ctx)
    asset_name = "test_batch_" + uuid4().hex[:8]
    _, bd = get_dataasset_gx(ds, asset_name)
    batch = _get_dataframe_batch(bd, df)
    assert_test("_get_dataframe_batch: valid DataFrame",
                batch is not None and hasattr(batch, "validate"))

    try:
        _get_dataframe_batch(bd, "not_a_dataframe")
        assert_test("_get_dataframe_batch: invalid type raises", False)
    except ValueError as e:
        assert_test("_get_dataframe_batch: invalid type raises",
                    "DataFrame" in str(e))
else:
    logger.info("  SKIP: _get_dataframe_batch requires Spark")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: _create_suite

# CELL ********************

print_header("Test: _create_suite")

ctx = get_datacontext_gx()
suite = _create_suite(ctx, "dq_base_customer")
assert_test("_create_suite: creates suite",
            suite is not None and suite.name == "dq_base_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: _add_expectations_to_suite

# CELL ********************

print_header("Test: _add_expectations_to_suite")

ctx = get_datacontext_gx()
suite = _create_suite(ctx, "dq_base_customer")
_add_expectations_to_suite(suite, [SAMPLE_VALIDATION_NOT_NULL, SAMPLE_VALIDATION_UNIQUE])
assert_test("_add_expectations_to_suite: adds expectations",
            len(suite.expectations) == 2)

try:
    _add_expectations_to_suite(suite, "not_a_list")
    assert_test("_add_expectations_to_suite: not list raises", False)
except ValueError as e:
    assert_test("_add_expectations_to_suite: not list raises",
                "list" in str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: _run_suite (requires Spark)

# CELL ********************

print_header("Test: _run_suite")

if spark is not None:
    df = spark.createDataFrame(
        [Row(customer_id=1)],
        schema="customer_id INT"
    )
    ctx = get_datacontext_gx()
    ds = get_datasource_gx(ctx)
    asset_name = "test_run_suite_" + uuid4().hex[:8]
    _, bd = get_dataasset_gx(ds, asset_name)
    batch = _get_dataframe_batch(bd, df)
    suite = _create_suite(ctx, "dq_run_suite_" + uuid4().hex[:8])
    _add_expectations_to_suite(suite, [
        {"expectation": "expect_column_to_exist", "column": "customer_id"}
    ])
    results = _run_suite(batch, suite)
    assert_test("_run_suite: validation executes",
                results is not None and hasattr(results, "results"))
else:
    logger.info("  SKIP: _run_suite requires Spark")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: _extract_results

# CELL ********************

print_header("Test: _extract_results")

def make_mock_result(success, expectation_type="expect_column_to_exist",
                     column="customer_id", unexpected_count=0,
                     unexpected_percent=0.0, observed_value="",
                     has_result=True):
    result = SimpleNamespace(
        unexpected_count=unexpected_count,
        unexpected_percent=unexpected_percent,
        observed_value=observed_value
    ) if has_result else None
    return SimpleNamespace(
        success=success,
        result=result,
        expectation_config=SimpleNamespace(
            type=expectation_type,
            kwargs={"column": column}
        ),
        exception_info={
            "m1": SimpleNamespace(
                exception_message="Metric evaluation failed: column 'nope' not found"
            )
        }
    )


def make_mock_result_compound(success, expectation_type, columns):
    return SimpleNamespace(
        success=success,
        result=SimpleNamespace(
            unexpected_count=0, unexpected_percent=0.0, observed_value=""
        ),
        expectation_config=SimpleNamespace(
            type=expectation_type,
            kwargs={"column_list": list(columns)}
        ),
        exception_info=None
    )


# Standardised result format
run_ts = datetime.now(timezone.utc)
mock_results = SimpleNamespace(results=[
    make_mock_result(success=True)
])
results = _extract_results(
    validation_results=mock_results,
    gx_validation=[SAMPLE_VALIDATION_EXIST],
    config_entry=SAMPLE_CONFIG,
    object_name="customer",
    run_id="test_extract",
    run_timestamp=run_ts
)
assert_test("_extract_results: returns correct number of rows",
            len(results) == 1)
entry = results[0]
assert_test("_extract_results: run_id matches", entry["run_id"] == "test_extract")
assert_test("_extract_results: run_timestamp is constant", entry["run_timestamp"] == run_ts)
assert_test("_extract_results: layer matches", entry["layer"] == "base")
assert_test("_extract_results: object_name matches", entry["object_name"] == "customer")
assert_test("_extract_results: expectation_type matches",
            entry["expectation_type"] == "expect_column_to_exist")
assert_test("_extract_results: success=True", entry["success"] is True)
assert_test("_extract_results: status=PASS", entry["status"] == "PASS")

# Severity defaults to critical
validation_no_severity = {
    "expectation": "expect_column_values_to_not_be_null",
    "column": "x",
}
mock_results = SimpleNamespace(results=[
    make_mock_result(success=True,
                     expectation_type="expect_column_values_to_not_be_null",
                     column="x")
])
results = _extract_results(
    validation_results=mock_results,
    gx_validation=[validation_no_severity],
    config_entry=SAMPLE_CONFIG,
    object_name="customer",
    run_id="test_severity_default",
    run_timestamp=run_ts
)
assert_test("_extract_results: severity defaults to critical",
            results[0]["severity"] == "critical")

# Severity from config
validation_with_severity = {
    "expectation": "expect_column_values_to_not_be_null",
    "column": "x",
    "severity": "warning",
}
mock_results = SimpleNamespace(results=[
    make_mock_result(success=True,
                     expectation_type="expect_column_values_to_not_be_null",
                     column="x")
])
results = _extract_results(
    validation_results=mock_results,
    gx_validation=[validation_with_severity],
    config_entry=SAMPLE_CONFIG,
    object_name="customer",
    run_id="test_severity_config",
    run_timestamp=run_ts
)
assert_test("_extract_results: severity from config",
            results[0]["severity"] == "warning")

# Missing results attribute raises
try:
    _extract_results(
        validation_results="no_results",
        gx_validation=[],
        config_entry={"layer": "base"},
        object_name="test",
        run_id="test",
        run_timestamp=run_ts
    )
    assert_test("_extract_results: missing results attribute raises", False)
except ValueError as e:
    assert_test("_extract_results: missing results attribute raises",
                "results" in str(e))

# Identity matching (FAIL case)
mock_results = SimpleNamespace(results=[
    make_mock_result(
        success=False,
        expectation_type="expect_column_values_to_not_be_null",
        column="customer_id",
        unexpected_count=3,
        unexpected_percent=30.0,
        observed_value="[1, 2, 3]"
    )
])
results = _extract_results(
    validation_results=mock_results,
    gx_validation=[SAMPLE_VALIDATION_NOT_NULL],
    config_entry=SAMPLE_CONFIG,
    object_name="customer",
    run_id="test_identity",
    run_timestamp=run_ts
)
entry = results[0]
assert_test("_extract_results: identity match success=False", not entry["success"])
assert_test("_extract_results: identity match status=FAIL",
            entry["status"] == "FAIL")
assert_test("_extract_results: identity match unexpected_count=3",
            entry["unexpected_count"] == 3)

# Metric error reported separately
mock_results = SimpleNamespace(results=[
    make_mock_result(
        success=False,
        expectation_type="expect_column_to_exist",
        column="nope",
        has_result=False
    )
])
results = _extract_results(
    validation_results=mock_results,
    gx_validation=[{"expectation": "expect_column_to_exist", "column": "nope"}],
    config_entry=SAMPLE_CONFIG,
    object_name="customer",
    run_id="test_metric_error",
    run_timestamp=run_ts
)
entry = results[0]
assert_test("_extract_results: metric error success=False", not entry["success"])
assert_test("_extract_results: metric error status=ERROR",
            entry["status"] == "ERROR")
assert_test("_extract_results: metric error has message",
            entry["error_message"] is not None)
assert_test("_extract_results: metric error unexpected_count=0",
            entry["unexpected_count"] == 0)

# Duplicate checks preserve order/severity
mock_results = SimpleNamespace(results=[
    make_mock_result(success=True,
                     expectation_type="expect_column_to_exist",
                     column="customer_id"),
    make_mock_result(success=True,
                     expectation_type="expect_column_to_exist",
                     column="customer_id"),
])
gx_validation_dup = [
    {"expectation": "expect_column_to_exist", "column": "customer_id", "severity": "warning"},
    {"expectation": "expect_column_to_exist", "column": "customer_id", "severity": "critical"},
]
results = _extract_results(
    validation_results=mock_results,
    gx_validation=gx_validation_dup,
    config_entry=SAMPLE_CONFIG,
    object_name="customer",
    run_id="test_duplicates",
    run_timestamp=run_ts
)
assert_test("_extract_results: duplicate checks produce 2 rows",
            len(results) == 2)
severities = [r["severity"] for r in results]
assert_test("_extract_results: duplicate checks preserve severities",
            "warning" in severities and "critical" in severities)

# Unmatched result defaults severity
mock_results = SimpleNamespace(results=[
    make_mock_result(success=True, expectation_type="unexpected_check", column="x")
])
results = _extract_results(
    validation_results=mock_results,
    gx_validation=[],
    config_entry=SAMPLE_CONFIG,
    object_name="customer",
    run_id="test_unmatched",
    run_timestamp=run_ts
)
assert_test("_extract_results: unmatched result defaults severity",
            results[0]["severity"] == "critical")

# Missing expectations create ERROR rows
mock_results = SimpleNamespace(results=[])
gx_validation = [
    {"expectation": "expect_column_to_exist", "column": "a"},
    {"expectation": "expect_column_values_to_not_be_null", "column": "b"},
]
results = _extract_results(
    validation_results=mock_results,
    gx_validation=gx_validation,
    config_entry=SAMPLE_CONFIG,
    object_name="customer",
    run_id="test_missing",
    run_timestamp=run_ts
)
assert_test("_extract_results: missing expectations create ERROR rows",
            len(results) == 2 and all(r["status"] == "ERROR" for r in results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test: run_validation_gx (end-to-end, requires Spark)

# CELL ********************

print_header("Test: run_validation_gx (end-to-end)")

if spark is not None:
    df_valid = spark.createDataFrame(
        [Row(customer_id=1), Row(customer_id=2)],
        schema="customer_id INT"
    )
    df_with_nulls = spark.createDataFrame(
        [Row(customer_id=1), Row(customer_id=None)],
        schema="customer_id INT"
    )

    # All PASS
    config = {
        "layer": "base",
        "gx_validation": [
            {"expectation": "expect_column_to_exist", "column": "customer_id"},
        ],
    }
    native, results = run_validation_gx(
        df=df_valid, config_entry=config,
        object_name="customer", run_id="e2e_pass"
    )
    assert_test("run_validation_gx: native results returned", native is not None)
    assert_test("run_validation_gx: all pass count", len(results) == 1)
    assert_test("run_validation_gx: all pass success", results[0]["success"] is True)
    assert_test("run_validation_gx: all pass status=PASS",
                results[0]["status"] == "PASS")

    # With failure
    config_fail = {
        "layer": "base",
        "gx_validation": [
            {"expectation": "expect_column_values_to_not_be_null", "column": "customer_id"},
        ],
    }
    native, results = run_validation_gx(
        df=df_with_nulls, config_entry=config_fail,
        object_name="customer", run_id="e2e_fail"
    )
    assert_test("run_validation_gx: with failure returns native", native is not None)
    assert_test("run_validation_gx: with failure returns results",
                len(results) == 1)
    assert_test("run_validation_gx: with failure success=False",
                results[0]["success"] is False)

    # Multiple checks
    config_multi = {
        "layer": "base",
        "gx_validation": [
            {"expectation": "expect_column_to_exist", "column": "customer_id"},
            {"expectation": "expect_column_to_exist", "column": "customer_id"},
        ],
    }
    native, results = run_validation_gx(
        df=df_valid, config_entry=config_multi,
        object_name="customer", run_id="e2e_multi"
    )
    assert_test("run_validation_gx: multiple checks returns 2 rows",
                len(results) == 2)

    # Missing gx_validation raises
    try:
        run_validation_gx(
            df=df_valid, config_entry={"layer": "base"},
            object_name="customer", run_id="test"
        )
        assert_test("run_validation_gx: missing gx_validation raises", False)
    except ValueError as e:
        assert_test("run_validation_gx: missing gx_validation raises",
                    "gx_validation" in str(e))

    # Missing layer raises
    try:
        run_validation_gx(
            df=df_valid, config_entry={"gx_validation": []},
            object_name="customer", run_id="test"
        )
        assert_test("run_validation_gx: missing layer raises", False)
    except ValueError as e:
        assert_test("run_validation_gx: missing layer raises",
                    "layer" in str(e))

    # Empty object_name raises
    try:
        run_validation_gx(
            df=df_valid, config_entry={"layer": "base", "gx_validation": []},
            object_name="", run_id="test"
        )
        assert_test("run_validation_gx: empty object_name raises", False)
    except ValueError as e:
        assert_test("run_validation_gx: empty object_name raises",
                    "object_name" in str(e))

    # Empty run_id raises
    try:
        run_validation_gx(
            df=df_valid, config_entry={"layer": "base", "gx_validation": []},
            object_name="test", run_id=""
        )
        assert_test("run_validation_gx: empty run_id raises", False)
    except ValueError as e:
        assert_test("run_validation_gx: empty run_id raises",
                    "run_id" in str(e))

    # Invalid df type raises
    try:
        run_validation_gx(
            df="not_a_dataframe",
            config_entry={"layer": "base", "gx_validation": []},
            object_name="test", run_id="test"
        )
        assert_test("run_validation_gx: invalid df type raises", False)
    except ValueError as e:
        assert_test("run_validation_gx: invalid df type raises",
                    "DataFrame" in str(e))

    # Invalid config_entry type raises
    try:
        run_validation_gx(
            df=df_valid, config_entry="not_a_dict",
            object_name="test", run_id="test"
        )
        assert_test("run_validation_gx: invalid config_entry type raises", False)
    except ValueError as e:
        assert_test("run_validation_gx: invalid config_entry type raises",
                    "config_entry" in str(e))
else:
    logger.info("  SKIP: run_validation_gx end-to-end tests require Spark")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary

# CELL ********************

print_summary()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
