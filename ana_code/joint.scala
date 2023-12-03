//spark-shell --master yarn --deploy-mode client

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

val bech = spark.read.option("header",true).csv("project/input/bechdel.csv")
val imdb = spark.read.option("header",true).option("quote", "\"").option("escape", "\"").csv("project/input/movie_data.csv")

//formatting imdb csv 
val toDrop = Seq("_c0","Year","Released","Runtime","Actors","Plot","Language","Country","Poster","Metascore","Type","DVD","Production","Website","Response","Season","Episode","seriesID","totalSeasons","Awards","BoxOffice","Ratings")
val newimdb = imdb.drop(toDrop: _*)
//drop duplicate rows with same imdb id
val imdb = newimdb.dropDuplicates("imdbID")

//It wouldn't be meaningful if we don't look at its distribution across decades. Define a UDF to calculate the decade

val calculateDecade: Int => Int = year => (year / 10) * 10
val calculateDecadeUDF = udf(calculateDecade)

val bechDecade = bech.withColumn("decade", calculateDecadeUDF(col("year")))

val newimdb = imdb.withColumn("imdbID", expr("substring(imdbID, 3)"))
val nnimdb = newimdb.withColumn("imdbID", regexp_replace(col("imdbID"), "^0+", ""))

val joinedData = bechDecade.join(nnimdb, bechDecade("imdbID") === nnimdb("imdbID"), "inner")

//retain columns I need
val updatedBechdel = joinedData.select(
	bechDecade("Title"),
	bechDecade("year"),
	nnimdb("Director").as("Director"),
	nnimdb("Writer").as("Writer"),
    bechDecade("imdbID"),
    bechDecade("Bechdel Score"),
    bechDecade("dubious"),
    bechDecade("year submitted"),
    bechDecade("decade"),
    nnimdb("Rated").as("Rated"),
    nnimdb("imdbVotes").as("imdbVotes"),
    nnimdb("imdbRating").as("imdbRating"),
    nnimdb("Genre").as("Genre")
)


//output to csv
//updatedBechdel.coalesce(1).write.option("header", "true").csv("project/nnewoutput")

val joined = updatedBechdel

//find distribution of score
val scoredist = joined.groupBy("Bechdel Score").count()
/*
+-------------+-----+
|Bechdel Score|count|
+-------------+-----+
|            3| 5442|
|            0| 1058|
|            1| 2049|
|            2|  985|
+-------------+-----+
mean = 2.1339416823998323
*/
//submission distribution
val subdist = joined.groupBy("year submitted").count().orderBy("yearsubmitted")

/*
+--------------+-----+
|year submitted|count|
+--------------+-----+
|          2008|   66|
|          2009|  219|
|          2010| 1209|
|          2011| 1047|
|          2012|  745|
|          2013| 1126|
|          2014|  941|
|          2015|  598|
|          2016|  682|
|          2017|  544|
|          2018|  466|
|          2019|  345|
|          2020|  433|
|          2021|  604|
|          2022|  441|
|          2023|   68|
+--------------+-----+
*/

//bechdel distribution over the year
val score_distribution = joined.groupBy("year", "Bechdel Score").count()
val pivot_scores = (score_distribution
  .groupBy("year")
  .pivot("Bechdel Score", Seq(0, 1, 2, 3)) 
  .agg(coalesce(first("count"), lit(0)))
	.orderBy("year"))

//cast rating&votes to float and integer
val jointIntVotes = joined.withColumn("imdbVotes", col("imdbVotes").cast("int"))
val jointWithFloatRating = jointIntVotes.withColumn("imdbRating", col("imdbRating").cast("float"))
 
val joined = jointWithFloatRating
//need to fix min and max here. cuz entries all in string
val ratingDist = (joined.groupBy("Bechdel Score")
	.agg(
		count("imdbRating").as("Count"),
		min("imdbRating").as("MinRating"),
		max("imdbRating").as("MaxRating"),
		avg("imdbRating").as("AvgRating"),
		stddev("imdbRating").as("StdDevRating")))

val votesDist = (joined.groupBy("Bechdel Score")
	.agg(
		count("imdbVotes").as("Count"),
		min("imdbVotes").as("minVotes"),
		max("imdbVotes").as("MaxVotes"),
		avg("imdbVotes").as("AvgVotes"),
		stddev("imdbVotes").as("StdDevVotes")))

val newratingDist = (joined.groupBy("decade", "Bechdel Score")
  .agg(
    count("Bechdel Score").as("Count"),
    min("imdbRating").as("MinRating"),
    max("imdbRating").as("MaxRating"),
    avg("imdbRating").as("AvgRating"),
    stddev("imdbRating").as("StdDevRating")
  )
  .orderBy("decade", "Bechdel Score"))

val newvotesDist = (joined.groupBy("decade","Bechdel Score")
	.agg(
		count("imdbVotes").as("Count"),
		min("imdbVotes").as("minVotes"),
		max("imdbVotes").as("MaxVotes"),
		avg("imdbVotes").as("AvgVotes"),
		stddev("imdbVotes").as("StdDevVotes")).orderBy("decade", "Bechdel Score"))

//output to csv
//joined.coalesce(1).write.option("header", "true").csv("project/noutput")
















