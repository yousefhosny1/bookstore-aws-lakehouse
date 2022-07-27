from pyspark.sql import functions as F


class ExpandDataTransformer:
    def transform(dataframe):
        """This transform method is responsible for parsing the "Location" attribute
        and producing 3 new attributes from it, City, State, Country

        Args:
            dataframe (pyspark.sql.dataframe.DataFrame): A PySpark Dataframe

        Returns:
            dataframe (pyspark.sql.dataframe.DataFrame): Expanded dataframe
        """        
        old_column, new_columns, delimiter = "Location", ["City", "State", "Country"], ','
        for i in range(len(new_columns)):
            dataframe = dataframe.withColumn(new_columns[i], F.split(
                dataframe[old_column], delimiter).getItem(i))
        return dataframe


class CleanDataTransformer:
    def transform(dataframe, columns=["Customer-ID", "ISBN", "Book-Rating", "Country", "State", "City"]):
        """This transform method is responsible for cleaning the data by replacing empty strings with 
        None/NULL value and then dropping all rows that have NULL values in the columns list passed in 
        the method. 

        Args:
            dataframe (pyspark.sql.dataframe.DataFrame): The Expanded dataframe produced by ExpandDataTransformer.transform
            columns (List): The columns where no null/missing values are accepted 

        Returns:
            dataframe (pyspark.sql.dataframe.DataFrame): Clean dataframe
        """        
        return (dataframe.select(
                [F.when(F.col(c) == " ", None).otherwise(F.col(c)).alias(c) for c in dataframe.columns])
                .na.drop(subset=columns))


class CustomersDimensionTableTransformer:
    def transform(dataframe, schema = ["Customer-ID", "Age", "City", "State", "Country"]):
        """This transform method is responsible for creating the Customers Dimension Table by
        selecting the columns that matches the Data Mart star schema.

        Args:
            dataframe (pyspark.sql.dataframe.DataFrame): The Clean dataframe produced by CleanDataTransformer.transform
            schema (List): Customers Dimension Table schema according to the Data Mart Star Schema

        Returns:
            dataframe (pyspark.sql.dataframe.DataFrame): Customers Dimension Table
        """        
        return dataframe.select(schema).distinct()


class BooksDimensionTableTransformer:
    def transform(dataframe, schema = ["ISBN", "Book-Title", "Book-Author", "Year-Of-Publication", "Publisher"]):
        """This transform method is responsible for creating the Books Dimension Table by
        selecting the columns that matches the Data Mart star schema.

        Args:
            dataframe (pyspark.sql.dataframe.DataFrame): The Clean dataframe produced by CleanDataTransformer.transform
            schema (List): Books Dimension Table schema according to the Data Mart Star Schema

        Returns:
            dataframe (pyspark.sql.dataframe.DataFrame): Books Dimension Table
        """        
        return dataframe.select(schema).distinct()


class RatingsFactTableTransformer:
    def transform(dataframe, schema = ["ISBN", "Customer-ID", "Book-Rating"]):
        """This transform method is responsible for creating the Ratings Fact Table by
        selecting the columns that matches the Data Mart star schema.

        Args:
            dataframe (pyspark.sql.dataframe.DataFrame): The Clean dataframe produced by CleanDataTransformer.transform
            schema (List): Rating Facts Table schema according to the Data Mart Star Schema

        Returns:
            dataframe (pyspark.sql.dataframe.DataFrame): Ratings Fact Table
        """        
        return dataframe.select(schema)


class Top100BooksTransformer:
    def transform(dataframe, columns = ["ISBN", "Book-Title", "Book-Rating"]):
        """This transform method is responsible for creating the top 100 books table
        by gouping all the books by their ISBN numbers and aggregating over them using
        an avg() spark function

        Args:
            dataframe (pyspark.sql.dataframe.DataFrame): The clean dataframe produced by CleanDataTransformer.transform
            columns (List): Choosing specific columns of the main dataframe to eliminate data redundancy 

        Returns:
            dataframe: Clean dataframe
        """        
        return (dataframe.select(columns)
                .groupby("ISBN", "Book-Title")
                .agg(F.avg("Book-Rating").alias("Average-Rating"), F.count("Book-Title").alias("Total-Ratings"))
                .filter(F.col("Total-Ratings") >= 100)
                .orderBy(F.col("Average-Rating").desc()).limit(100))


class Transformer:
    '''
    This class will be the entry point to the Spark Job, It will be imported in the 
    spark-main.py script to run all the transformations.
    '''
    def __init__(self) -> None:
        self.transformers = {
            "ExpandData": ExpandDataTransformer,
            "CleanData": CleanDataTransformer,
            "BooksDimensionTable": BooksDimensionTableTransformer,
            "CustomersDimensionTable": CustomersDimensionTableTransformer,
            "RatingsFactTable": RatingsFactTableTransformer,
            "Top100Books": Top100BooksTransformer
        }

    def transform(self, dataframe, type, **kwargs):
        if type not in self.transformers:
            raise Exception(f"{type} is an incorrect type")
        return self.transformers[type].transform(dataframe, **kwargs)
