from workflow.Transformer import Transformer
from workflow.Extractor import Extractor
from workflow.Loader import Loader
from workflow.StringConstant import RAW_DATA_S3_PATH, DATA_MART_S3_PATH, APP_NAME, LOAD_MODE
from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    E, T, L = Extractor(), Transformer(), Loader()

    #### EXTRACT ####
    raw_data = E.extract_parquet(spark, RAW_DATA_S3_PATH)

    ### TRANSFORM ###
    expanded_raw_data = T.transform(raw_data, "ExpandData")
    clean_data = T.transform(expanded_raw_data, "CleanData")
    books_dimension_table = T.transform(clean_data, "BooksDimensionTable")
    customers_dimenstion_table = T.transform(clean_data, "CustomersDimensionTable")
    ratings_fact_table = T.transform(clean_data, "RatingsFactTable")
    top_100_books_table = T.transform(clean_data, "Top100Books")

    ##### LOAD ######
    L.load(books_dimension_table, LOAD_MODE, DATA_MART_S3_PATH, 'Books')
    L.load(customers_dimenstion_table, LOAD_MODE, DATA_MART_S3_PATH, 'Customers')
    L.load(ratings_fact_table, LOAD_MODE, DATA_MART_S3_PATH, 'Ratings')
    L.load(top_100_books_table, LOAD_MODE, DATA_MART_S3_PATH, 'Top100books')