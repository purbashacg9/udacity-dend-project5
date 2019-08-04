## Project:  Data Pipelines with Airflow
The goal of the project was to build a complete data pipeline in Airflow to perform ETL of Sparkify's event logs and songs data.

The source data resides in S3 and needs to be processed in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs Sparkify's users listen to.

In this project I created custom operators to do the following -
1. Read events and songs data from S3 and stage the data in Redshift  
2. populate the fact and dimension tables in Redshift from staging tables and   
3. Perform data quality checks on the data  


All the operators were tied and sequenced in an Airflow DAG which was scheduled to run hourly.

![Tasks in DAG](https://s3.amazonaws.com/video.udacity-data.com/topher/2019/January/5c48ba31_example-dag/example-dag.png)

## Brief description of the operators

### Stage Operator Requirements

- The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift.
- The operator should create and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
- The parameters should be used to distinguish between JSON file loads.
- The stage operator should contain a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

### Fact and Dimension Operators
- The operators are expected to take as input a SQL statement and target database on which to run the query against.
- The operators also define a target table that will contain the results of the transformation.
- Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load.
- Have a parameter that allows switching between truncate before delete or append mode when loading dimensions.

### Data Quality Operator

The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.
