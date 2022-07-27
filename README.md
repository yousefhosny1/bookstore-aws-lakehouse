# Online Bookstore Data Lakehouse in AWS
***
> **Note:** The project's business case/requirement was designed totally by me, I ensured that it simulates a real world scenario. 


# Hypothetical Business Case:
I am a Data Engineer who is working in an online bookstore. The business decision makers have decided to create a data mart called (Books Ratings) to store all the ratings of customers on all the books we are selling so far. The business is also running a sales day on the upcoming **International day of books** and to ensure the upmost **customer satisfaction** they would like me to get the top 100 books in terms of their average ratings to run a sale on them. 

**I am given a raw, normalized dataset containing all the ratings of our customers on all books and my tasks are as following:**
* Create a star schema for the Data Mart (Books Ratings)
* Write and schedule a spark ETL job to clean and load the data in the Data Mart
* Creating visualizations about the top 10 countries by number of customers, top 10 states by number of customers, top 10 book authors by average ratings, top 100 books by average ratings

# Raw Dataset Description
The dataset is obtained using the following [link](http://www2.informatik.uni-freiburg.de/~cziegler/BX/) and a sample of the dataset can be obtained from ```data/sample-data.csv``` in this github repo.

The dataset was collected by Cai-Nicolas Ziegler in a 4-week crawl (August / September 2004) from the Book-Crossing community. It Contains 278,858 customers (anonymized but with demographic information) providing 1,149,780 ratings (explicit / implicit) about 271,379 books.

**The attributes of the raw dataset is as following**

[![dataset.png](https://i.postimg.cc/BQm6VGPc/dataset.png)](https://postimg.cc/CzfSRXNz)

# Data Mart Star Schema
To store the data efficiently in the Data Mart, I will have to ```denormalize``` the raw dataset, thus I designed a ```star schema``` as shown below that allows me to ```drill down``` and ```roll up``` by customer information and book details
[![Star-Schema.png](https://i.postimg.cc/SKsy8kFP/Star-Schema.png)](https://postimg.cc/CR3TpyKC)

With this Data Mart, we will be able to analyze the performance of books, publishers and authors by countries, states and cities. Interesting actionable insights can be extracted with simple join queries to accelerate the book-store business performance. For example using the following query, we are able to extract the top 100 books by their average ratings and run a sale promotion on these books to generate revenue and achieve customer satisfaction.

```sql 
SELECT Dimension_Books.BookTitle, avg(CAST(Fact_Ratings.BookRating AS double precision)) AS "average-rating"
FROM Dimension_Books INNER JOIN Fact_Ratings ON Dimension_Books.ISBN = Fact_Ratings.ISBN
GROUP BY Dimension_Books.BookTitle
HAVING count(Dimension_Books.BookTitle) > 100
ORDER BY "average-rating" DESC
LIMIT 100;
```

# ETL Data Pipeline Architecture

The data pipeline architecture is as shown below, The initial dataset obtained from the following [link](http://www2.informatik.uni-freiburg.de/~cziegler/BX/) was in ```CSV``` format sizing **300MB**, thus for efficient storage I have changed the data format to ```parquet```, by doing this I have reduced the file size from **300MB** to **34MB** and changed the raw dataset from row based structure to column based structure which is more efficient for querying and transforming with spark. Following that I stored it in the raw_data folder in our data lake ```s3://task-us-east-1/raw-data/book-ratings.parquet```. Then I created an ```EMR Spark Cluster``` to perform the ETL Job. Since Spark jobs relies heavily on cluster memory (RAM) as it performs parallel computing in memory across nodes to reduce the I/O and execution times of tasks, the worker nodes need to be memory optimized thus I went with cluster nodes of type ```R6g```, also considering the size of our dataset is below 32 GB, thus a ```R6g.xlarge``` type is sufficient. The number of nodes will be 1 master node and 1 worker node to save cloud cost.



Through the ETL job, I will be extracting the data from the raw-data folder, applying transformations and loading the transformed data into the **data mart folder** inside the data lake ```s3://task-us-east-1/Book-Ratings-DataMart/``` and finally for querying and visualizations I have decided to **keep the data in the S3 Data Mart Folder** but create a **Redshift external schema** that runs on top of the data mart and queries the transformed data directly from S3 without loading the data into Redshift, this is cost effective as keeping the Data in S3 is cheaper than loading it and storing it in Redshift. To achieve this, I ran ```AWS Glue Crawlers``` to crawl the tables in the **Data Mart** and store the learned schemas in a **Glue Data Catalog** and then I created a Redshift external schema on top of the Glue Data Catalog to query the data directly from S3.
[![Pipeline Architecture.jpg](https://i.postimg.cc/RVwGMdwq/Blank-diagram-1.jpg)](https://postimg.cc/2V8nG4cf)

The ```EMR Cluster Spark Job``` is ```scheduled``` using ```crontab``` to run every **midnight**, this ensures that in the following working day, the data will be ready for analysis and querying in the Data mart. The CRONTAB script is as following

[![Picture1.png](https://i.postimg.cc/j5TckfB1/Picture1.png)](https://postimg.cc/sBn5MMjP)

The query used to create the external schema is as following
```sql
CREATE EXTERNAL SCHEMA datamart
FROM data catalog 
database 'socar-glue-job' 
iam_role 'arn:aws:iam::493849651063:role/redshift-spectrum-role'
CREATE EXTERNAL database if not exists;
```

## Extract
The following class in ```workflow/Extractor.py``` perform the extraction of the data from the data lake
```python
class Extractor:
    def extract_parquet(self, spark, path):
        df = spark.read.option('header', 'true').parquet(path)
        return df
```
where the path is obtained from the ```workflow/StringConstant.py``` module
```python
RAW_DATA_S3_PATH = 's3://task-us-east-1/raw-data/book-ratings.parquet'
```

## Transform
In the transformation phase, there will be 6 transformations. 

**1. Expand the Data**

Since our original dataset has a ```Location``` attribute where its values are in string format and have this structure ```'city, state, country'```, i.e ```'pj, selangor, malaysia'```. I will run a transformation to create new columns, ```City, State, Country```, by **splitting** the ```Location``` attribute. This is achieved by the following class in ```workflow/Transformer.py```. This **transformation is very important** as these columns are required in our ```Data Mart Star Schema```

```python
class ExpandDataTransformer:
    def transform(dataframe):
        old_column, new_columns, delimiter = "Location", ["City", "State", "Country"], ','
        for i in range(len(new_columns)):
            dataframe = dataframe.withColumn(new_columns[i], F.split(
                dataframe[old_column], delimiter).getItem(i))
        return dataframe
```

**2. Clean the Data**

Since one of the main characteristics of a data mart is to be used for business decisions, data quality is very important as garbage in = garbage out thus the data stored in it need to be clean, a common data quality issue is missing values, thus using the ```CleanDataTransformer``` class in ```workflow/Transformer.py``` I have elimited rows that have missing values in important columns, for example, primary and foreign keys cannot be missing as they are crucial for the Star Schema, thus any rows that have missing/null values in these columns have to be dropped.

```python
class CleanDataTransformer:
    def transform(dataframe, columns=["Customer-ID", "ISBN", "Book-Rating", "Country", "State", "City"]):
        '''
        Replaces empty strings by a None value then drops all None values in the passed in columns
        '''
        return (dataframe.select(
                [F.when(F.col(c) == " ", None).otherwise(F.col(c)).alias(c) for c in dataframe.columns])
                .na.drop(subset=columns))
```

**3. Create the Customers Dimension Table**

After the data is expanded and cleaned, we can obtain the Customers Dimension Table using the ```CustomersDimensionTableTransformer``` class by selecting the columns that matches the Star Schema
```python
class CustomersDimensionTableTransformer:
    def transform(dataframe, schema = ["Customer-ID", "Age", "City", "State", "Country"]):
        return dataframe.select(schema).distinct()
```

**4. Create the Books Dimension Table**

Similarly, we can obtain the Books Dimension Table using the ```BooksDimensionTableTransformer``` class by selecting the columns that matches the Star Schema
```python
class BooksDimensionTableTransformer:
    def transform(dataframe, schema = ["ISBN", "Book-Title", "Book-Author", "Year-Of-Publication", "Publisher"]):
        return dataframe.select(schema).distinct()
```

**5. Create the Ratings Fact Table**

Similarly, we can obtain the Ratings Fact Table using the ```RatingsFactTableTransformer``` class by selecting the columns that matches the Star Schema
```python
class RatingsFactTableTransformer:
    def transform(dataframe, schema = ["ISBN", "Customer-ID", "Book-Rating"]):
        return dataframe.select(schema)
```

**6. Create the Top 100 Books Table**

Finally, using the ```Top100BooksTransformer``` class, we can obtain the Top100Books by grouping by the book's ISBN and aggregating using the ```avg``` function

```python
class Top100BooksTransformer:
    def transform(dataframe, columns = ["ISBN", "Book-Title", "Book-Rating"]):
        return (dataframe.select(columns)
                    .groupby("ISBN")
                    .agg(F.avg("Book-Rating").alias("Average-Rating"), F.count("Book-Title").alias("Total-Ratings"))
                    .filter(F.col("Total-Ratings") >= 100)
                    .orderBy(F.col("Average-Rating").desc()).limit(100))
```

## Load
The ```Loader``` class in ```workflow\Loader.py``` is responsible for loading the data
```python
class Loader:
    def load(self, data, mode, path, table_name):
        '''
        Loads CSV Files to output path
        '''
        data.write.mode(mode).option('header', True).csv(path + f'/{table_name}')
```

Where the **path** and **mode** are constants that are obtained from ```workflow/StringConstant.py```
```python
RAW_DATA_S3_PATH = 's3://task-us-east-1/raw-data/book-ratings.parquet'  #  data/raw_data/book-ratings.parquet
DATA_MART_S3_PATH = 's3://task-us-east-1/Book-Ratings-DataMart' # data/data_mart
APP_NAME = 'Yousef-Socar-Task'
LOAD_MODE = 'append' # change to append
```

## Main ETL Job Script
The main ETL Job script to be submitted in the **EMR cluster** is ```spark-main.py``` which imports all the required class and modules from the ```workflow``` package and runs all the logic.


# Visualizations
```Tableau``` was used as a visualization tool for this project, we are able run queries directly on top of the Data Mart in S3 and produce visualizations. 
Because of our Data Mart star schema, we are able to run interesting queries and answer business questions easily by drilling down and rolling up. For example lets assume that we want to run a marketing campaign, however our bugdet is tight, thus we want to only target countries that yield the highest number of customers. Using a simple query as shown below, I am able to create a visualization of the top 10 countries by number of customers.

> **Note:** All the queries used to produce these visualizations are saved in the ```external-table-queries.sql``` file.

### Top 10 Countries by Number of Customers
[![Top-10-countries.png](https://i.postimg.cc/Bvs2bQh9/Top-10-countries.png)](https://postimg.cc/21c1XmR2)

As seen in the bar chart above, there is a huge gap between the first country and the rest, so lets assume we are going to target USA in our marketing campaign, however USA is big, and it has alot of states, thus we want to choose specific states to run our marketing campaign on, so the decision makers asked me to produce the top 10 states in USA by the number of customers, well, that is easy to achieve because of our ```Data Mart Star Schema```, I am able to ```drill down``` with a simple **group by clause** and produce the required visualization as shown below. 
### Top 10 States in USA by Number of Customers
[![Top-10-States-in-USA.png](https://i.postimg.cc/9M94Y6gt/Top-10-States-in-USA.png)](https://postimg.cc/0M828Bsb)

Let's assume that we want to run a promotion sale, on the upcoming **international book day** where the decision makers decided to run a discount on the top 100 books by average ratings, using the following query i am able to produce the required results
### Top 100 Books
[![Top-100-Books.png](https://i.postimg.cc/KcVLL595/Top-100-Books.png)](https://postimg.cc/Dmr8313W)

Finally, as I am looking to buy a book, I have decided to run a query to see the top 10 book authors by their average ratings.
### Top 10 Authors by Average Ratings
[![Top-10-Authors.png](https://i.postimg.cc/tJygtYxp/Top-10-Authors.png)](https://postimg.cc/7GsDqP9W)


