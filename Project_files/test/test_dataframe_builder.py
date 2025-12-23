import os
from typing import List, Tuple

import pytest
from app.dataframe_builder import DataframeBuilder
from pyspark.sql import SparkSession

TEST_CSV_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../csv_inputs/customer_purchases.csv')
spark_session = SparkSession.builder.appName('test_session').getOrCreate()


@pytest.mark.parametrize("top_n_products, expected_count", [(1, 4), (2, 8)])
def test_top_n_products_by_country_count(top_n_products: int, expected_count: List[Tuple]):
    output_df = DataframeBuilder(). \
        create_df_from_csv(TEST_CSV_PATH). \
        get_top_n_products_by_country(top_n_products). \
        sdf

    assert output_df.count() == expected_count


def test_top_n_products_by_country_order():
    customer_origin = 'customer_origin'
    output_df = DataframeBuilder(). \
        create_df_from_csv(TEST_CSV_PATH). \
        get_top_n_products_by_country(2). \
        sdf
    output_rows = output_df.rdd.map(lambda row: row.asDict()).collect()
    assert all(row[customer_origin] <= output_rows[i + 1][customer_origin]
               for i, row in enumerate(output_rows)
               if i != len(output_rows) - 1)


@pytest.mark.parametrize('top_n_products', [0, -5])
def test_top_n_products_by_country_raises(top_n_products: int):
    with pytest.raises(ValueError):
        _ = DataframeBuilder(). \
            create_df_from_csv(TEST_CSV_PATH). \
            get_top_n_products_by_country(top_n_products)


def test_customer_purchases_by_country_count():
    output_df = DataframeBuilder(). \
        create_df_from_csv(TEST_CSV_PATH). \
        customer_purchases_by_country(). \
        sdf

    assert output_df.count() == 6


def test_customer_purchases_null_data():
    output_df = DataframeBuilder(). \
        create_df_from_csv(TEST_CSV_PATH). \
        customer_purchases_by_country(). \
        sdf
    output_rows = output_df.rdd.map(lambda row: row.asDict()).collect()

    assert output_rows[0]['ES'] == output_rows[0]['PL'] == 0


def test_customer_purchases_order():
    customer_id = 'customer_id'
    output_df = DataframeBuilder(). \
        create_df_from_csv(TEST_CSV_PATH). \
        customer_purchases_by_country(). \
        sdf
    output_rows = output_df.rdd.map(lambda row: row.asDict()).collect()

    assert all(int(row[customer_id]) < int(output_rows[i + 1][customer_id])
               for i, row in enumerate(output_rows)
               if i != len(output_rows) - 1)

