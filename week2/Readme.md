# Week 2 Assignment Solution

// Initialization table from csv file
spark.read.option("header", "true").csv("/home/harmeet/knoldus_spark_sessions/code/week2/Fire_Department_Calls_for_Service.csv").createOrReplaceTempView("fire_service")

//Initialie data frames from csv file
val df = spark.read.option("header", "true").csv("/home/harmeet/knoldus_spark_sessions/code/week2/Fire_Department_Calls_for_Service.csv")


##### Q1. How many different types of calls were made to the Fire Department ?
#### Answer:
```
spark.sql("SELECT `Call Type` FROM fire_service GROUP BY `Call Type`").show(false)
```
##### Q2. How many incidents of each call type were there ?
#### Answer:
```
spark.sql("SELECT COUNT(DISTINCT(`Incident Number`)) as `Incident Count`, `Call Type` FROM fire_service GROUP BY `Call Type`").show(false)
```
##### Q3. How many years of Fire Service Calls in the data file ?
#### Answer:
```
case class FireService(callNumber: String, callDate: java.sql.Timestamp, city: String, neighbourhood: String)

val fireServiceDf = df.map(row => {
val dateFormat = new java.text.SimpleDateFormat("MM/dd/yyyy");
val parsedDate = dateFormat.parse(row.getAs[String](4));
val timestamp = new java.sql.Timestamp(parsedDate.getTime());
FireService(row.getAs[String](0), timestamp, row.getAs[String](16), row.getAs[String](31))
}).toDF

fireServiceDf.createOrReplaceTempView("fire_service")


spark.sql("SELECT (DATEDIFF(MAX(callDate), MIN(callDate)) / 365) AS Years FROM fire_service").show(false)
```
##### Q4. How many service calls were logged in the last 7 days ?
#### Answer:
```
fireServiceDf.select(fireServiceDf("callNumber")).where(max(fireServiceDf("callDate")) >= date_sub(max(fireServiceDf("callDate")), 7)).show(false)
```
##### Q5. Which neighbourhood in SF generated the most calls last year ?
#### Answer:
```
spark.sql("SELECT * FROM fire_service WHERE callDate BETWEEN (SELECT DATE_SUB(MAX(callDate), 7) FROM fire_service) AND (SELECT MAX(callDate) FROM fire_service) ORDER BY callDate DESC").show(false)
```

