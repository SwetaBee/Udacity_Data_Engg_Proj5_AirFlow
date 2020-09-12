# Udacity_Data_Engg_Proj5_AirFlow
#### Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Project Setup - to run locally
Install Airflow, create variable AIRFLOW_HOME and AIRFLOW_CONFIG with the appropiate paths, and place dags and plugins on airflor_home directory.
Initialize Airflow data base with airflow initdb, and open webserver with airflow webserver
Access the server http://localhost:8080 and create:
AWS Connection Conn Id: Enter aws_credentials. Conn Type: Enter Amazon Web Services. Login: Enter your Access key ID from the IAM User credentials you downloaded earlier. Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

Redshift Connection Conn Id: Enter redshift. Conn Type: Enter Postgres. Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. Schema: This is the Redshift database you want to connect to. Login: Enter awsuser. Password: Enter the password created when launching the Redshift cluster. Port: Enter 5439.

## DataSet
Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

## AWS Configuration
Make sure to add the following Airflow connections:
Create_Cluster.py (Refer to Lesson 3 for AWS connection)
/opt/airflow/start.sh
Open "Access Airflow"
AWS credentials
Connection to Postgres database

## Structure
udac_example_dag.py contains the tasks and dependencies of the DAG. It should be placed in the dags directory of your Airflow installation.
create_tables.sql contains the SQL queries used to create all the required tables in Redshift. It should be placed in the dags directory of your Airflow installation.
sql_queries.py contains the SQL queries used in the ETL process. It should be placed in the plugins/helpers directory of your Airflow installation.
The following operators should be placed in the plugins/operators directory of your Airflow installation:

stage_redshift.py contains StageToRedshiftOperator, which copies JSON data from S3 to staging tables in the Redshift data warehouse.
load_dimension.py contains LoadDimensionOperator, which loads a dimension table from data in the staging table(s).
load_fact.py contains LoadFactOperator, which loads a fact table from data in the staging table(s).
data_quality.py contains DataQualityOperator, which runs a data quality check by passing an SQL query and expected result as arguments, failing if the results don't match.
