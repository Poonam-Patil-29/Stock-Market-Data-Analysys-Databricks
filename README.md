# Stock Market Data Analysis

## Introduction :
Stock market data refers to the collection of information related to stocks and their prices, as well as other financial indicators such as trading volume, market capitalization, and stock performance over time. 
		Investors and traders use this data to make informed decisions about buying and selling stocks, as well as to analyze market trends and predict future activity. 
		Stock market data can be obtained through various sources, including financial news websites, stock market indices, and financial data providers. 
The data is often analyzed using various tools and techniques to gain insights into market behavior and inform investment decisions.

## Techonologies :
* Language 		: Python, Pyspark
* Environment 		: Azure Databricks
* Visualization 	: PowerBI
* Azure Services 	: SQL Databases, Azure Data Lake Factory, Storage Accounts, Azure Databricks
* DevOps 		: Continuous Intergration and Continuous Deployment, GitHub

## Work Flow Diagram :
![ERD](https://github.com/Poonam-Patil-29/Stock-Market-Data-Analysys-Databricks/assets/104273538/323b831e-aaeb-4478-a289-ab121dea87b5)

## Databricks & Pyspark: Real Time ETL Pipeline Azure SQL to ADLS
1. Extract data from Azure SQL tables<br>
2. Transform the data with business rule<br>
3. Load the data to Azure Data Lake Storage<br>
ETL:<br>
  E: read tables from Azure SQl [fact and dimension tables], JDBC connector is used, dataframes are created <br>
  T: Business transformations are applied, Null are replaced with default values, duplicates are dropped, join and aggregate are performed <br>
  L: Mount point is created for ADLS integration, Data is loaded in Parquet file format to target location <br>

* Prerequisites:
  Azure subscription: If you don't have an Azure subscription, create a free account before you begin.
  Azure Storage account with Data Lake Storage Gen2 enabled: If you don't have a Storage account, create an account.

* STEP 1 : Create a storage account:
* STEP 2 : Load the csv file into Azure blob storage
  - open the Storage account
  - click on containers
  - Add a Source (mariapgsource) and a destination container (mariapgdestination)
  - Open the source container > Upload the CSV file from the Laptop folder [Max file size supported is 2.1MB]
* STEP 3 : Create a data factory: 2 methods: 1. by using the Azure portal UI or 2. by using Azure Data Factory Studio
* STEP 4 : Launch Azure Data Factory.
* STEP 5 : Launch Azure portal UI.
* STEP 6 : create a resource > Data factory
* STEP 7 : Move data from blob storage to ADLSGen2
* STEP 8 : Creating a Data Pipeline in Azure. Move data from ADLS2 to Azure SQL Server with databrick used for ETL /data transformation
* STEP 9 : Create Azuredatabricks workspace and launch the databricks workspace
* STEP 10 : Create a cluster in Databricks 
* STEP 11 : Once the cluster is up and running, you can now go back to the dashboard and create a Notebook.
* STEP 12 : create Azure SQl Database
* STEP 13 : Give storage account name <storage account> and copy access key from storage account and paste.<br>
  spark.conf.set("fs.azure.account.key.<storage account>.dfs.core.windows.net", "access key")<br>
* STEP 14 : Open the cluster and from advance copy paste below details and execute
  - jdbcHostname = 'databrick-server1.database.windows.net'
  - jdbcport = '1433'<br>
  - jdbcDatabase = 'Database1'           
  - properties = {'user':'Poonam','password':'Encrypted@2023'}<br>
  - url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcport,jdbcDatabase)<br>
  - output = DataFrameWriter(df_new)<br>
  - output.jdbc(url = url, table = 'sample', mode = 'overwrite', properties = properties)<br>
    ( Need to set server firewall to match the system IP address to access Azure SQl Server database )
* STEP 15 : Now you can execute queries on query editor.

## Visualization :
Power BI Desktop is a free application you install on your local computer that lets you connect to, transform, and visualize your data. With Power BI Desktop, you can connect to multiple different sources of data, and combine them (often called modeling) into a data model. This data model lets you build visuals, and collections of visuals you can share as reports, with other people inside your organization. Most users who work on business intelligence projects use Power BI Desktop to create reports, and then use the Power BI service to share their reports with others.<br>
Steps to connect with Power BI Desktop :
* Step 1 : From the Home ribbon, select Get Data > More.
* Step 2 : When you select a data type, you're prompted for information, such as the URL and credentials, necessary for Power BI Desktop to connect to the data    source on your behalf.
* Step 3 : After you connect to one or more data sources, you may want to transform the data so it's useful for you.<br>
![1](https://github.com/Poonam-Patil-29/Stock-Market-Data-Analysys-Databricks/assets/104273538/e8f6808e-838c-4570-94cf-ca5bced5f412)<br>
![2](https://github.com/Poonam-Patil-29/Stock-Market-Data-Analysys-Databricks/assets/104273538/e8e57357-ecb1-4127-ad36-7929fe339771)<br>
![3](https://github.com/Poonam-Patil-29/Stock-Market-Data-Analysys-Databricks/assets/104273538/2af3eeb4-8c70-4221-8d58-196473863abc)<br>
![4](https://github.com/Poonam-Patil-29/Stock-Market-Data-Analysys-Databricks/assets/104273538/29bf4d2f-7df3-46e1-9b69-e4932de34ff7)<br>
![5](https://github.com/Poonam-Patil-29/Stock-Market-Data-Analysys-Databricks/assets/104273538/4f9900e7-df67-42e8-94e3-c8c9fb575c64)<br>
![6](https://github.com/Poonam-Patil-29/Stock-Market-Data-Analysys-Databricks/assets/104273538/d4e9bbfa-eee6-4191-9ff5-055e11cb6218)<br>

## Contributors :
Poonam Patil <br>
Mohammad Ibrahim

