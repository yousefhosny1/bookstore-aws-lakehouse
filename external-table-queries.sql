---------------------------------------------------------------------------------
------------- REDSHIFT EXTERNAL SCHEMA DEFINITION USING GLUE CATALOG ------------
---------------------------------------------------------------------------------
create external schema datamart
from data catalog 
database 'socar-glue-job' 
iam_role 'arn:aws:iam::493849651063:role/redshift-spectrum-role'
create external database if not exists;

-------------------------------------------------------------
------------- QUERIES FOR TABLEAU VISUALIZATIONS ------------
-------------------------------------------------------------
-- Querying the top 100 books by average ratings
SELECT datamart.books."book-title", avg(CAST(datamart.ratings."book-rating" AS double precision)) AS "average-rating"
FROM datamart.books INNER JOIN datamart.ratings ON datamart.books.ISBN = datamart.ratings.ISBN
GROUP BY datamart.books."book-title"
HAVING count(datamart.books."book-title") > 100
ORDER BY "average-rating" DESC
LIMIT 100;

-- Querying the top 10 country by number of customers
SELECT country, count(*) AS "Customer Count"
FROM datamart.customers
GROUP BY country
ORDER BY "Customer Count" DESC
LIMIT 10;

-- Querying the top 10 states in usa by number of customers
SELECT country, state, count(*) AS "Customer Count"
FROM datamart.customers
WHERE country = 'usa'
GROUP BY country, state
ORDER BY "Customer Count" DESC
LIMIT 10;

-- Querying the top 10 book authors by average ratings
SELECT datamart.books."book-author", avg(CAST(datamart.ratings."book-rating" AS double precision)) AS "Average Rating"
FROM datamart.books INNER JOIN datamart.ratings ON datamart.books.ISBN = datamart.ratings.ISBN
GROUP BY datamart.books."book-author"
HAVING count(datamart.books."book-author") > 100
ORDER BY "Average Rating" DESC
LIMIT 10;

-----------------------------------------------------------------------
------------- TOP-100-BOOKS TABLE CREATION AND DATA LOADING------------
-----------------------------------------------------------------------

-- CREATING THE TOP100BOOKS BY AVERAGE RATINGS TABLES
CREATE TABLE top100books (
  ISBN varchar(200) not null,
  BookTitle text not null,
  AverageRating numeric(3,2) not null,
  TotalRating integer not null,
  PRIMARY KEY (ISBN)
  );

-- COPYING ETL JOB OUTPUT INTO REDSHIFT TABLE
COPY top100books
FROM 's3://task-us-east-1/output/top-100-books/part'
credentials
'aws_access_key_id=AKIAXF653U53SDBOELGK;aws_secret_access_key=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'
CSV
IGNOREHEADER 1
DELIMITER ',';