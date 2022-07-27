class Extractor:

    def extract_parquet(self, spark, path):
        df = spark.read.option('header', 'true').parquet(path)
        return df

    def extract_csv(self, spark, path):
        df = spark.read.option('header', 'true').csv(path, inferSchema=True)
        return df