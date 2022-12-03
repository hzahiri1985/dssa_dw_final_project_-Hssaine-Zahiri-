# Data Gathering & Warehousing
## ETL Using Python and Database BY Hssaine Zahiri
***
***
### Introduction:
ETL is a process in Data Warehousing and it stands for Extract, Transform and Load. 
It is a process in which an ETL tool extracts the data from various data source systems, 
transforms it in the staging area, and then finally, loads it into the Data Warehouse system. 

Let us understand each step of the ETL process in-depth: 

**Extraction:**

The first step of the ETL process is extraction. In this step, data from various source systems is extracted which can be in various formats like relational databases,
No SQL, XML, and flat files into the staging area. It is important to extract the data from various source systems and store it into the staging area first and not directly into the data warehouse 
because the extracted data is in various formats and can be corrupted also. Hence loading it directly into the data warehouse may damage it and rollback will be much more difficult. 
Therefore, this is one of the most important steps of ETL process.

**Transformation:** 

The second step of the ETL process is transformation. In this step, a set of rules or functions are applied on the extracted data to convert it into a single standard format.
It may involve following processes/tasks: 
Filtering – loading only certain attributes into the data warehouse.
Cleaning – filling up the NULL values with some default values, mapping U.S.A, United States, and America into USA, etc.
Joining – joining multiple attributes into one.
Splitting – splitting a single attribute into multiple attributes.
Sorting – sorting tuples on the basis of some attribute (generally key-attribute).

**Loading:**

The third and final step of the ETL process is loading. In this step, the transformed data is finally loaded into the data warehouse. 
Sometimes the data is updated by loading into the data warehouse very frequently and sometimes it is done after longer but regular intervals.
The rate and period of loading solely depends on the requirements and varies from system to system.
ETL process can also use the pipelining concept i.e. as soon as some data is extracted, it can transformed and during that period some new data can be extracted.
And while the transformed data is being loaded into the data warehouse, the already extracted data can be transformed.


The main objective of this Project is to implement an ETL process in python to create a Star-Schema in a Data Warehouse. The ETL is as flow 
extract data from a OLTP database called dvdrental
transform it by creating an aggregation of the count of rentals
load the data into the new schema in dabase (Data Warehouse)

# Workflow 
+ We import all neccesary labiraries 
+ Create connection to database (dvdrental) 
+ Create new schema (dssa)
+ Create new dimension tables 
+ Query the data from database and use the SQL script to do the transformation and create a DtaFrame 
+ Load the DataFrame into new schema.


# Code

## Using virtualenv to create your virtual environment

pip install virtualenv
pip install virtualenv
Create your virtual enviroment
python -m virtualenv .venv --python=3.10
Access Virtual Environment MacOS or Linux
source .venv/bin/activate 
Access Virtual Environment Windows cmd
.venv\Scripts\activate
Deactivate Virtual Environment
exit
Clean up or Remove a Virtual Environment
.venv --rm

![image](https://user-images.githubusercontent.com/49703976/205454061-98f07ed3-cf5f-4203-872e-4321e7eb0308.png)

# Creating tables
![image](https://user-images.githubusercontent.com/49703976/205454136-66bfe3a8-6ea0-4663-959d-46b5eff5e02e.png)

# Building Star Schema and do the transformation using SQL

![image](https://user-images.githubusercontent.com/49703976/205454214-e302e2cf-3a7f-4901-b727-15d2540fe9e6.png)

# Loading the dataframe into DSSA

![image](https://user-images.githubusercontent.com/49703976/205454246-1a137ab2-e37c-4dbe-b8b7-d1243de4584f.png)


# Conclusion 
Designing and building an ETL system for a data warehouse is an exercise in keeping perspective. This is a typical complex undertaking that demands a comprehensive plan up front. It's easy to start transferring data from a specific source and immediately populate tables that can be queried.



