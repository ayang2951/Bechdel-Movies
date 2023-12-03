
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction


val df = spark.read.option("header",true).csv("hw7/bechdel.csv")

//find distribution of score
val scoredist = df.groupBy("Bechdel Score").count()

//convert to int
val intscoredist = scoredist.withColumn("Bechdel Score", col("Bechdel Score").cast(IntegerType))

//find mean, which is 1.5
val meanscore = intscoredist.agg(avg("Bechdel Score")).first().getDouble(0)


//It wouldn't be meaningful if we don't look at its distribution across decades. Define a UDF to calculate the decade

val calculateDecade: Int => Int = year => (year / 10) * 10
val calculateDecadeUDF = udf(calculateDecade)

//Date formatting (previously in hw7, I have already fixed the submission date formatting for normalization)
val dfDecade = df.withColumn("decade", calculateDecadeUDF(col("year")))

val countByDecade = dfDecade.groupBy("decade").count().orderBy("decade")

countByDecade.show()

//let's find mean and median by decade
val statsByDecade = (dfDecade.groupBy("decade")
  .agg(
    avg("Bechdel Score").alias("mean_score"),
    percentile_approx(col("Bechdel Score"),  lit(0.5), lit(1000)).alias("median_score")
  )
  .orderBy("decade"))
statsByDecade.show()

//let's also take a look at the submission distribution every 3 years

val countbysub = df.groupBy("year submitted").count().orderBy("year submitted")


//year difference between movie production and user submission
val dfWithDiff = dfDecade.withColumn("year difference", col("year submitted") - col("year"))
val countDiff = dfWithDiff.groupBy("year difference").count().orderBy("year difference")
//from the distribution we can tell most users update movies recently made

