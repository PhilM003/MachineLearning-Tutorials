from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class DataframeBuilder:
    DEFAULT_CUSTOMER_ID_COLUMN = 'customer_id'
    DEFAULT_PRODUCT_COLUMN = 'product_id'
    DEFAULT_COUNTRY_COLUMN = 'customer_origin'

    def __init__(self, df: DataFrame = None):
        self._df: DataFrame = df
        if self._df:
            self._spark_session = self._df.sql_ctx
        else:
            self._spark_session = self.create_spark_session()

    def create_spark_session(self) -> SparkSession:
        return SparkSession.builder.appName(self.__class__.__name__).getOrCreate()

    @property
    def sdf(self):
        return self._df

    def sample(self, ratio: int):
        self._df = self._df.sample(ratio)
        return self

    def create_df_from_csv(self, path: str):
        self._df = self._spark_session.read.csv(path, header=True)
        return self

    def get_top_n_products_by_country(self, n: int,
                                        product_col: str = DEFAULT_PRODUCT_COLUMN,
                                        country_col: str = DEFAULT_COUNTRY_COLUMN):
            if n <= 0:
                raise ValueError("n must be positive")
            
            # นับจำนวนการซื้อแยกตามประเทศและสินค้า
            df_counts = self._df.groupBy(country_col, product_col) \
                .agg(F.count("*").alias("purchases_count"))
            
            # สร้าง Window สำหรับจัดอันดับ
            window_spec = Window.partitionBy(country_col).orderBy(F.col("purchases_count").desc(), F.col(product_col))
            
            # เพิ่มคอลัมน์ rank และกรองเอาเฉพาะ top n
            self._df = df_counts.withColumn("rank", F.rank().over(window_spec)) \
                .filter(F.col("rank") <= n) \
                .orderBy(country_col, "rank")
                
            return self
    
    """
    Method which returns a DataFrame, which contains 4 columns:
        * customer_origin: ISO code of the given country (will contain all of the countries present within the input
                        Dataframe).
        * product_id: ID of the product that is among the top n most famous products within that country.
        * purchases_count: Integer indicating the amount of purchases of the given product within the given country.
        * rank: An integer indicating that the given product is the n-th most popular product within the given
                    country.
    Args:
        n: Number of most popular products per country to be returned.
        product_col: Name of the column within the input Dataframe that corresponds to product_id's.
        country_col: Name of the column within the input Dataframe that corresponds to country code's.
    Returns:
        An instance of DataframeBuilder with an updated `self._df` parameter.
    """
    # TODO: Complete this method.
    pass

    def customer_purchases_by_country(self,
                                        customer_id_col: str = DEFAULT_CUSTOMER_ID_COLUMN,
                                        country_col: str = DEFAULT_COUNTRY_COLUMN):
            # ทำ Pivot ตาราง
            self._df = self._df.groupBy(customer_id_col) \
                .pivot(country_col) \
                .count() \
                .na.fill(0) \
                .orderBy(customer_id_col)
                
            return self
    """
    Method which returns a Dataframe, which contains customer_id, ISO country codes as columns. Each column with
    a country code will contain count of products that this specific customer has bought in a given country.
    Returns:
        An instance of DataframeBuilder with an updated `self._df` parameter.
    """
    # TODO: Complete this method.
    pass