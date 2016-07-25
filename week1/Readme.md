# Week 1 Assignment Solution
##### Q1. Create an RDD (Resilient Distributed Dataset) named pagecounts from the input file?
#### Answer:
```scala
val pagecounts = sc.textFile("/home/harmeet/knoldus_spark_sessions/week1/pagecounts")
```
##### Q2. Get the 10 records from the data and write the data which is getting printed/displayed ?
#### Answer:
```scala
pagecounts.take(10).foreach(println(_))
```
##### Q3. How many records in total are in the given data set ?
#### Answer:
```scala
pagecounts.count()
```
##### Q4. The second field is the “project code” and contains information about the language of the pages. For example, the project code “en” indicates an English page. Derive an RDD containingonly English pages from pagecounts ?
#### Answer:
```scala
pagecounts.map(line => line.split("\\s+").apply(1)).
	filter(title => title.contains("/en/"))
```
##### Q5. How many records are there for English pages ?
#### Answer:
```scala
pagecounts.map(line => line.split("\\s+").apply(1)).
	filter(title => title.contains("/en/")).count()
```
##### Q6. Find the pages that were requested more than 200,000 times in total ?
#### Answer:
```scala
pagecounts.filter(line => line.split("\\s+").length == 4).
	map(line => (line.split("\\s+").apply(1), line.split("\\s+").apply(2).toInt)).
	filter(request => request._2 > 200000).
	foreach(title => println(title._1))
```
