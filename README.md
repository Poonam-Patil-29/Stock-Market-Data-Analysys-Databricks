# Stock-Market-Data-Analysys-Databricks
import os
from os import system
clear = lambda: system('cls')
import pandas as pd
from pandas import DataFrame
global df
import platform
from pyspark.sql.functions import desc
from pyspark.sql import *
from pyspark.sql.functions import year, min, max
from pyspark.sql.functions import year, avg,count,mean
from pyspark.sql.functions import col
from pyspark.sql.utils import ParseException
from pyspark import StorageLevel
df1=spark.read.csv("dbfs:/FileStore/shared_uploads/poonampatil290499@gmail.com/indexData.csv",inferSchema=True,header=True)
df1.persist(StorageLevel.MEMORY_ONLY)
df2=spark.read.csv("dbfs:/FileStore/shared_uploads/poonampatil290499@gmail.com/indexInfo.csv",inferSchema=True,header=True)
df2.persist(StorageLevel.MEMORY_ONLY)
df3=spark.read.csv("dbfs:/FileStore/shared_uploads/poonampatil290499@gmail.com/indexProcessed.csv",inferSchema=True,header=True)
df3.persist(StorageLevel.MEMORY_ONLY)
"""
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
jdbcHostname = 'databrick-server1.database.windows.net'
jdbcport = '1433'
jdbcDatabase = 'Database1'                       
properties = {

    'user':'Poonam',                            
    'password':'Encrypted@2023'
}
url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcport,jdbcDatabase)
output = DataFrameWriter(df1)
output.jdbc(url = url, table = 'sample', mode = 'overwrite', properties = properties)
"""
print("-------------------------------------------------------------------- ")
print("--- STOCK MARKET DATA ANALYSIS ---*")
print("---------------------------------------------------------------------")
A_name1="123"
A_password1="123"
while True:
    A_name=input("Enter Username ")
    A_password=input("Enter Password ")
    if (A_name==A_name1 and A_password==A_password1):
        print( " ... Login Successfull .. " )
        #1
        def datasetInfo():
            print("Enter 1 to check schema and records for Dataset 1")
            print("Enter 2 to check schema and records for Dataset 2")
            print("Enter 3 to check schema and records for Dataset 3")
            inp1=int(input("Enter your choice"))
            if(inp1==1):
                df1.printSchema()
                df1.show(10)
                #df1.count()
            elif(inp1==2):
                df2.printSchema()
                df2.show(10)
                #df2.count()
            elif(inp1==3):
                df3.printSchema()
                df3.show(10)
                #df3.count()
            else:
                print("! Sorry you entered wrong number")
                #2
        def userchoiceToSelectRow():
            #dataset 1 : 112457
            #dataset 2: 14
            #dataset 3: 104225
            print("Press 1 to check data in dataset first")
            print("Press 2 to check data in dataset second")
            print("Press 3 to check data in dataset third")
            choice=int(input("Enter your choice (1 to 3): "))
            print("Number of records in first dataset")    
            if choice==1:
                inp=int(input("Enter the number of rows you want to display : "))                
                if inp<=df1.count():
                    df1.show(inp)
                else:
                    print("You are exceding the dataset limit! can't fetch the data try again !")                   
            elif choice==2:
                inp=int(input("Enter the number of rows you want to display : "))
                if inp<=df2.count():
                    df2.show(inp)
                else:
                    print("You are exceding the dataset limit! can't fetch the data try again !")
            elif choice==3:
                inp=int(input("Enter the number of rows you want to display : ")) 
                if inp<=df3.count():
                    df3.show(inp)
                else:
                    print("You are exceding the dataset limit! can't fetch the data try again !")
            else:
                print("Sorry ! you enterred wrong choice")
        def availabledatacount():                        
            #3
            print("Enter 1 to show open in sorted order")
            print("Enter 2 to show close in sorted order")
            print("Enter 3 to show low in sorted order")
            print("Enter 4 to show high in sorted order")
            str=int(input("Enter your choice"))
            if str==1:
                n=int(input("How many records you want :"))
                a=df3.sort(desc("Open"))
                a.show(n)
            elif str==2:
                n=int(input("How many records you want :"))
                b=df3.sort(desc("Close"))
                b.show(n)
            elif str==3:
                n=int(input("How many records you want :"))
                c=df3.sort(desc("Low"))
                c.show(n)
            elif str==4:
                n=int(input("How many records you want :"))
                d=df3.sort(desc("High"))
                d.show(n)
            else:
                print("Enter valid option")
                

        def specificDetailsSelection():
            #4
            print("Press 1 for counting United State records")
            print("Press 2 for counting Hong Kong records")
            print("Press 3 for counting China records")
            print("Press 4 for counting Japan records")
            print("Press 5 for counting Europe records")
            print("Press 6 for counting Canada records")
            print("Press 7 for counting India records")
            df = df1.join(df2,on="Index",how="inner")
            k=int(input("Enter your choice...."))
            if(k==1):
                print("Counting United State records : ",df.select("Index","Date","Open","High","Low","Close").filter(df['Index']=='NYA').count())
            elif(k==2):
                print("Counting Hong Kong records : ",df.select("Index","Date","Open","High","Low","Close").filter(df['Index']=='HSI').count())
            elif(k==3):
                print("Counting China records : ",df.select("Index","Date","Open","High","Low","Close").filter(df['Index']=='000001.SS').count())
            elif(k==4):
                print("Counting Japan records : ",df.select("Index","Date","Open","High","Low","Close").filter(df['Index']=='N225').count())
            elif(k==5):
                print("Counting Europe records : ",df.select("Index","Date","Open","High","Low","Close").filter(df['Index']=='N100').count())
            elif(k==6):
                print("Counting Germany records : ",df.select("Index","Date","Open","High","Low","Close").filter(df['Index']=='GDAXI').count())
            elif(k==7):
                print("Counting Indian records : ",df.select("Index","Date","Open","High","Low","Close").filter(df['Index']==
                'NSEI').count())
            else:
                print("Record not found")
                
        def rangewise():           
            # 5
            print("Filtering data from particular range")
            print("Enter 1 to check data between HIGH ")
            print("Enter 2 to check data between LOW ")
            print("Enter 3 to check data between OPEN ")
            print("Enter 4 to check data between CLOSE ")
            choice=int(input("Enter your Choices :"))   
            if choice==1:
                print("Filter the records based on given HIGH range")
                a=int(input("Enter the start range :"))
                b=int(input("Enter the end range :"))
                join=df1.select("Index","Date","Adj Close","High").distinct().filter((df1["High"]>=a)&(df1["High"]<=b))
                join.show()
            elif choice==2:
                print("Filter the records based on given LOW range ")
                a=int(input("Enter the first range :"))
                b=int(input("Enter the second range :"))
                join1=df1.select("Index","Date","Adj Close","Low").filter((df1["Low"]>=a)&(df1["Low"]<=b))
                join1.show()
            elif choice==3:
                print("Filter the records based on given OPEN range ")
                a=int(input("Enter the first range :"))
                b=int(input("Enter the second range :"))
                join2=df1.select("Index","Date","Adj Close","Open").filter((df1["Open"]>=a)&(df1["Open"]<=b))
                join2.show()
            elif choice==4:
                print("Filter the records based on given CLOSE range")
                a=int(input("Enter the first range :"))
                b=int(input("Enter the second range :"))
                join2=df1.select("Index","Date","Adj Close","Close").filter((df1["Close"]>=a)&(df1["Close"]<=b))
                join2.show()
            else:
                print("Sorry Invalid Input")


        def filtring():          
            # 6
            print("Enter 1 to sort index key low wise")
            print("Enter 2 to sort index key high wise")
            print("Enter 3 to sort index key open wise")
            print("Enter 4 to sort index key close wise")
            choice=int(input("Enter your choice :"))
            if choice==1:
                n=int(input("How many records you want :"))
                df=df1.rdd.map(lambda x:(x["Index"],x["Low"])).sortByKey(0)
                df=df.toDF()
                df=df.show(n)
            elif choice ==2:
                n=int(input("How many records you want :"))
                df=df1.rdd.map(lambda x:(x["Index"],x["High"])).sortByKey(0)
                df=df.toDF()
                df=df.show(n)
            elif choice ==3:
                n=int(input("How many records you want :"))
                df=df1.rdd.map(lambda x:(x["Index"],x["Open"])).sortByKey(0)
                df=df.toDF()
                df=df.show(n)
            elif choice ==4:
                n=int(input("How many records you want :"))
                df=df1.rdd.map(lambda x:(x["Index"],x["Close"])).sortByKey(0)
                df=df.toDF()
                df=df.show(n)
            else:
                print("Sorry !Invalid Choice")


        def avgofhigh():            
            # 7
            print("Enter 1 to perform aggretate operations based on high ")
            print("Enter 2 to perform aggretate operations based on low ")
            
            djoined_df = df1.join(df2,df1["Index"] ==df2["Index"], "inner")
            choice =int(input("Enter your Choice :"))
            if choice==1:
                n=int(input("How many records you want :"))
                df3.groupBy("Index","Open","High").agg(count("Open").alias("COUNT"),max("High").alias("MAXIMUM"),avg("High").alias("AVARAGE"),min("High").alias("MINIMUM")).show(n)
            elif choice==2:
                n=int(input("How many records you want :"))
                df3.groupBy("Index","Open","Low").agg(count("Open"),max("Low"),avg("Low"),min("Low").alias("average")).show(n)
            #   grouped_df = df2.groupBy("Index").agg(count("Open").alias("count"),sum("High").alias("total_sum"),avg("Low").alias("avg_value"))
            else:
                print("Invalid Choices")

        #8
        def avgvolume():
            user_year = int(input("Enter the year: "))
            df4 = df1.withColumn("year", year(df1["Date"]))
            filtered_df = df4.filter(df4["year"] == user_year)
            if filtered_df.count() > 0:
                average_volume_each_year = filtered_df.groupBy("year","Adj Close").agg(mean("Adj Close").alias("Average volume"))     
                average_volume_each_year.show()
            else:
                print("No records found for the specified year.")

        def fetchdatewise():           
            #9
            # fetch data from particular date
            start_date_input = input("Enter the start date (yyyy-mm-dd): ")
            end_date_input = input("Enter the end date (yyyy-mm-dd): ")

            try:
                # Convert user input to date format for comparison
                start_date = spark.sql("SELECT CAST('{}' AS DATE)".format(start_date_input)).collect()[0][0]
                end_date = spark.sql("SELECT CAST('{}' AS DATE)".format(end_date_input)).collect()[0][0]

                # Filter the DataFrame based on the user-entered date range
                filtered_df = df1.filter((col("Date") >= start_date) & (col("Date") <= end_date))

                # Check if any records exist for the user-entered date range
                if filtered_df.count() > 0:
                    filtered_df.show()
                else:
                    print("Invalid date format. Please enter the date in yyyy-mm-dd format.")

            except ParseException:
                print("Invalid date format. Please enter the date in yyyy-mm-dd format.")

        def currencywise():          
            #10
        
            user_currency = input("Enter the currency: ").upper()
            
            joined_df =df1.join(df2,df1["Index"] == df2["Index"], "inner")
            filtered_df = joined_df.filter(joined_df["Currency"] == user_currency)
            if filtered_df.count() > 0:
                n=int(input("How many records you want :"))
                filtered_df.show(n)
            else:
                print("No records found for the specified currency.")


        def regionwise():          
            #11
            #inner join - and fetch the data resion wise
            joined_df = df1.join(df2,df1["Index"] ==df2["Index"], "inner")
            user_region = input("Enter the region to fetch the data: ")
            filtered_df = joined_df.filter(joined_df["Region"] == user_region)
            if filtered_df.count() > 0:
                n=int(input("How many records you want :"))
                filtered_df.show(n)
            else:
                print("Region not found")
        def minimumstock():
            #12
            n=int(input("How many records you want :"))
            df5 = df1.withColumn("year", year(df1["Date"]))
            grouped_df = df5.groupBy("Index", "year").agg(
                min("Open").alias("Minimum Stock Open"),
            # max("Open").alias("MaxOpen"),
                min("Close").alias("Minimum Stock Close"),
                #max("Close").alias("MaxClose"),
                min("High").alias("Minimum Stock High"),
                #max("High").alias("MaxHigh"),
                min("Low").alias("Minimum Stock Low"),
                #max("Low").alias("MaxLow")
            )
            grouped_df.show(n)
        def choicewise():                   
            #13
            #from pyspark.sql.functions import *
            # Count Data Index Wise
            print("Enter 1 for couting data Index wise")
            print("Enter 2 for counting data Region wise")
            print("Enter 3 for counting data Currency wise")
            ch=int(input("Enter your choice"))
            if ch==1:
                df1.groupBy("Index").agg(count("Open"),count("Close"),count("High"),count("Low")).show()
            elif ch==2:
                df2.groupBy("Region").agg(count("Exchange"),count("Index"),count("Currency")).show()
            elif ch==3:
                df3.groupBy("Index").agg(count("Open"),count("Close"),count("High"),count("Low"),count("Adj Close"),count("CloseUSD")).show()
            else:
                print("Sorry you entered wrong input")
            
        
        #14
        def prediction():
            input1 = int(input("Enter high and low range : "))
            input2 = int(input("Enter open and close range : "))
            if(input1==input2):
                print("Both range are same thats'y ","Stock TIE")
            elif(input1>input2):
                print("Range  of High and Low is greater thats'y ","Stock BULL")
            elif(input1<input2):
                print("Range of Open and Close is greater thats'y ","Stock BEAR")
            else:
                print("Sorry! Wrong Input")
        #15
        def quit():
            k=input("Do you want Logout (y/n)")
            if k=='y':
                exit()
            else:
                print("Run again ")

        def Menuset():
            print("Press 1 to view stocks and schema")
            print("Press 2 to view stocks according to user choice")
            print("Press 3 to view stocks in sorted order")
            print("Press 4 to view counting of stocks region wise")
            print("Press 5 to view stock between given range")
            print("Press 6 to view mapping of stocks by index ")
            print("Press 7 to view minimum,maximum,average of stocks by index")
            print("Press 8 to view average volume each year")
            print("Press 9 to view stocks from particular dates")
            print("Press 10 to view stocks currency wise")
            print("Press 11 to view stocks region wise")
            print("Press 12 to view minimun value of stock each year")
            print("Press 13 to view counting of data indexwise")
            print("Press 14 to view stock prediction")
            print("Press 15 to Exit")
            try:
                userinput = int(input("please select an above option:"))
            except ValueError:
                exit("\n hi thats not a number")

            #userinput = int(input("enter your choice"))
            if (userinput == 1):
                datasetInfo()
            elif (userinput == 2):
                userchoiceToSelectRow()
            elif (userinput == 3):
                availabledatacount()
            elif (userinput == 4):
                specificDetailsSelection()
            elif (userinput == 5):
                rangewise()
            elif (userinput == 6):
                filtring()
            elif (userinput == 7):
                avgofhigh()
            elif (userinput == 8):
                avgvolume()
            elif (userinput == 9):
                fetchdatewise()
            elif (userinput == 10):
                currencywise()
            elif (userinput == 11):
                regionwise()
            elif (userinput==12):
                minimumstock()
            elif (userinput == 13):
                choicewise()
            elif (userinput==14):
                prediction()
            elif (userinput == 15):
                quit()
            else:
                print("enter correct choice")
        Menuset()
        def runagain():
            runagn = input("\n want to run again y/n:")
            while (runagn.lower() == 'y'):
                if (platform.system() == "windows"):
                    print(os.system('cls'))
                else:
                    print(os.system('clear'))
                Menuset()
       
        runagain()
    else:
        print( " --::--Login was Failed....Try Again...")
