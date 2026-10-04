import pytest
from pyspark.sql import SparkSession

from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("PySparkTest")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def input_df(spark):
    data = [
        (1, "Alice", 100.0),
        (2, "Bob", 200.0),
        (3, "Charlie", 0.0),
        (4, "David", -50.0),
        (5, None, 150.0),
    ]

    return spark.createDataFrame(
        data,
        ["id", "name", "amount"]
    )


def test_valid_records_are_kept(input_df):
    result = clean_data(input_df)

    ids = [row.id for row in result.collect()]

    assert 1 in ids
    assert 2 in ids


def test_records_with_amount_less_than_or_equal_to_zero_are_removed(input_df):
    result = clean_data(input_df)

    ids = [row.id for row in result.collect()]

    assert 3 not in ids
    assert 4 not in ids


def test_records_with_null_names_are_removed(input_df):
    result = clean_data(input_df)

    ids = [row.id for row in result.collect()]

    assert 5 not in ids


def test_amount_with_tax_is_calculated_correctly(input_df):
    result = clean_data(input_df)

    rows = result.collect()

    amounts = {
        row.id: row.amount_with_tax
        for row in rows
    }

    assert amounts[1] == pytest.approx(120.0)
    assert amounts[2] == pytest.approx(240.0)