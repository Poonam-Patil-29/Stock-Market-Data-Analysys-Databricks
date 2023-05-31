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
* STEP 14 : Open the cluster and from advance copy paste below details and execute.<br>
  jdbcHostname = 'databrick-server1.database.windows.net'<br>
  jdbcport = '1433'<br>
  jdbcDatabase = 'Database1' <br>                
  properties = {'user':'Poonam', </br>
	'password':'Encrypted@2023'}<br>
  url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcport,jdbcDatabase)<br>
  output = DataFrameWriter(df_new)<br>
  output.jdbc(url = url, table = 'sample', mode = 'overwrite', properties = properties)<br>
* STEP 15 : Now you can execute queries on query editor.
