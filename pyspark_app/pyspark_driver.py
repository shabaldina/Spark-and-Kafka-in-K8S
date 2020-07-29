from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import to_date, expr, current_date, date_sub, date_format

# These are the static paths to the input and output files
inputInfections = "hdfs://hadoop-hadoop-hdfs-nn:9000/input/infections.csv"
inputDAX = "hdfs://hadoop-hadoop-hdfs-nn:9000/input/quandl_fse.csv"

outputFileCorona = "hdfs://hadoop-hadoop-hdfs-nn:9000/result/corona"
outputFileDAX = "hdfs://hadoop-hadoop-hdfs-nn:9000/result/dax"

#create SparkSession
spark = (SparkSession.
            builder.
            appName("pyspark_driver").
            getOrCreate())

spark.sparkContext.setLogLevel('WARN')

#read infections info and return dataframe using an infered schema
corona_in = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ";") \
        .csv(inputInfections)

#for debugging
print(corona_in.printSchema())     
corona_in.show(10)

#select only necessary rows
corona = corona_in.select(to_date('dateRep', format='dd/MM/yyyy').alias('date'), 
                            corona_in.cases,
                            corona_in.deaths,
                            corona_in.countriesAndTerritories.alias('country'),
                            corona_in.continentExp.alias('continent'))

corona0 = corona.filter(corona.continent == "Europe")
corona0.show(50)

#calculate relative difference to previous day for number of cases and death
corona1 = corona0.withColumn("prev_date", date_sub(corona.date, 1))

#regester dataframe as temporary view for querieng and transforming it
corona1.createOrReplaceTempView("corona_out")
corona1.show(10)

corona_out1 = spark.sql("SELECT cor1.date, cor1.cases, cor1.deaths, cor1.country, cor1.continent, \
                        cor2.cases as cases_prev, cor2.deaths as dead_prev \
                        FROM corona_out as cor1  \
                        LEFT OUTER JOIN  corona_out as cor2 on cor2.date = cor1.prev_date and cor1.country=cor2.country ")

corona_out = corona_out1.withColumn("cases_rel_diff", expr("(cases-cases_prev)/cases_prev"))\
                        .withColumn("deaths_rel_diff", expr("(deaths-dead_prev)/dead_prev"))
#for debugging
corona_out.show(10)

#read DAX info and return dataframe using an infered schema
dax_in = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ";") \
        .csv(inputDAX)

#for debugging
print(dax_in.printSchema())     
dax_in.show()

#create dataframe with relevant fields and regester it as temporary view for querieng and transforming it
dax_in1 = dax_in.select(dax_in['Date'].cast('date'), 
                        dax_in.Share, dax_in.Open, dax_in.Close)
dax_in1.createOrReplaceTempView("dax_in")
dax_in1.show()

#calculate DAX value as a total ammount of all stock values for each date
dax = spark.sql( "SELECT Date, SUM(Open) as open_sum, SUM(Close) as close_sum \
                FROM dax_in GROUP BY Date ORDER BY Date")
dax.show()

#add a column to store difference between open and close values 
dax_out = dax.withColumn("abs_diff", expr("close_sum - open_sum"))

#for debugging
dax_out.show()

# write the results to hdfs
corona_out.write.format("csv").option("header", "true").mode("append").save(outputFileCorona)
dax_out.repartition(1).write.format("csv").option("header", "true").mode("append").save(outputFileDAX)

#close SparkSession
spark.stop()