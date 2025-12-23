# Customer Preference Model
 
## Introduction
 
You are working as a Data Engineer for a market chain operating on a worldwide scale. 
 
Having access to the customers purchase history, your goal is to create key analyses for marketers so that they will be able to promote the right products in the right countries and for the right customers.
 
## Prerequisites
To complete this task, you have to use `Python 3` as well as the `pyspark` module.
 
## Problem Statement
 
Note: Please do *NOT* modify any tests unless specifically told to do so.
 
### ETL
 
Your goal is to complete the specific methods inside the `app.dataset_builder.DataframeBuilder` class, that is responsible for the ETL part of the modelling, namely some initial data ingestion and preprocessing.
 
You will be given an initial CSV input file that consists of all the purchases that are relevant within this section, called [customer_purchases.csv](csv_inputs/customer_purchases.csv). This file consists of the following columns:
 * `customer_id`(string): a unique ID of the customer purchasing the given product.
 * `shop_id`(string): a unique ID of the shop in which the given product has been purchased.
 * `product_id`(string): a unique ID of the product which has been purchased.
 * `purchase_date`(date): the date of the transaction.
 * `customer_origin`(string): the country from which the given customer comes from,
 
The `DataframeBuilder` class is returning `self` after every transformation, similarly to `pyspark.sql.Dataframe`.
An example workflow with this class would look like this:
```python
output_sdf: pyspark.sql.Dataframe = DataframeBuilder().create_df_from_csv(path).sample().sdf
```
 
#### get_top_n_products_by_country
 
In this part, your goal will be to implement the `app.dataframe_builder.DataframeBuilder.get_top_n_products_by_country`
method. This method will be responsible for updating the `self._df` parameter, so that it will contain the top n products sold in each country. After updating it, the method will return `self`, similarly to an existing `app.dataframe_builder.DataframeBuilder.sample` method. This new DF should contain four columns:
 * customer_origin: the ISO code of the given country (it will contain all countries present within the input
               Dataframe).
 * product_id: the ID of the product that is among the top n most famous products within that country.
 * purchases_count: the integer indicating the number of purchases of the given product within the given country.
 * rank: an integer indicating that the given product is the n-th most popular product within the given
             country.
An example output Dataframe would look like this:
```
+---------------+----------+---------------+----+
|customer_origin|product_id|purchases_count|rank|
+---------------+----------+---------------+----+
|             AT|     F/C/1|              3|   1|
|             AT|     F/C/2|              2|   2|
|             ES|     F/C/1|             10|   1|
+---------------+----------+---------------+----+
```
The Dataframe should be ordered alphabetically by country and then by rank.
 
#### customer_purchases_by_country
 
In this part, your goal will be to implement the `app.dataframe_builder.DataframeBuilder.pivot_by_country` method.
This method will be responsible for updating the `self._df` parameter, so that it will contain the customer_id and the counts of the product bought by every customer in each country. An example output Dataframe would look like this:
```
+-----------+---+---+---+---+
|customer_id| AU| DE| UA| CH|
+-----------+---+---+---+---+
|     100000|  1|  2| 12|  0|
|     100001|  3| 10|  2|  1|
|     100002|  0| 10|  0|  0|
+-----------+---+---+---+---+
```
The missing values should be filled with zeros.
The Dataframe should be ordered by the customer_id as shown in the example above.

## Good luck!
