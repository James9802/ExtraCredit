import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder().appName("FilterMatchingDates").getOrCreate()

// Read CSV data as DataFrames
val dfJun = spark.read.csv("2009Jun.csv")
val dfAAPL = spark.read.csv("AAPL.csv")

// Rename columns
val dfJunDateName = dfJun.withColumnRenamed("_c0", "Date")
val dfAAPLDateName = dfAAPL.withColumnRenamed("_c0", "Price_Date")
val dfAAPLDateName2 = dfAAPLDateName.withColumnRenamed("_c2", "ID")
val dfJun_dt = dfJunDateName.withColumnRenamed("_c1", "Trend")
val dfAAPL_dt = dfAAPLDateName2.withColumnRenamed("_c1", "Price")

// Join Dataframes on date column (inner join to keep only matching rows)
val joinedDF = dfJun_dt.join(dfAAPL_dt, col("Date") === col("Price_Date"), "inner")
// val joinedDF = dfJun.alias("dfJun").join(dfAAPL.alias("dfAAPL"), col("dfJun._c0") === col("dfAAPL._c0"), "inner")

// Remove duplicate columns
val workingDF = joinedDF.drop("Price_Date")

// val workingDFWithStringDate = workingDF.withColumn("Date", col("Date").cast("String"))

// Convert Trend column to Int
val workingDFWithIntegerTrend = workingDF.withColumn("Trend", col("Trend").cast("Int"))

// Convert Price column to Double
val workingDFWithDoublePrice = workingDFWithIntegerTrend.withColumn("Price", col("Price").cast("Double"))

// Convert ID column to Int and obtain the comprehensive dataset ready for analysiss
val finalWorkingDF = workingDFWithDoublePrice.withColumn("ID", col("ID").cast("Int"))

// Compute moving averages and completes all data needed for analysis
val workingDFWithPMA = finalWorkingDF.withColumn("Price_MovingAverage", avg("Price").over(Window.orderBy(col("ID")).rowsBetween(-13, 0)))
val workingDFWithMA = workingDFWithPMA.withColumn("Trend_MovingAverage", avg("Trend").over(Window.orderBy(col("ID")).rowsBetween(-13, 0)))

// Compute when trend up positively
val trendDifferencesDF = workingDFWithMA.filter(col("Trend") - col("Trend_MovingAverage") >=0)

// Compute average trend up magnitude
val trendUPavg = trendDifferencesDF.select(avg(col("Trend") - col("Trend_MovingAverage"))).first().getDouble(0)

// Cutoff at 2 avg to get all MAJOR trend spike ups
val trendSpikes = trendDifferencesDF.filter(col("Trend") - col("Trend_MovingAverage") >= 2*trendUPavg)

val trendNormal = workingDFWithMA.filter(col("Trend") - col("Trend_MovingAverage") < 2*trendUPavg && col("Trend") - col("Trend_MovingAverage") > -2*trendUPavg)

// Filter when stock is not trending by setting cutoff at 0.5 avg
val notTrending = workingDFWithMA.filter(col("Trend") - col("Trend_MovingAverage") <= -2*trendUPavg)

// Compute average stock flunctuation in percentages over entire period
val priceFlunctationAvg = workingDFWithMA.select(avg(abs(col("Price") - col("Price_MovingAverage")) /col("Price_MovingAverage"))).first().getDouble(0)

// Compute the stock flunctuation only over trend spikes
val priceFlunctationDuringTrendSpike = trendSpikes.select(avg(abs(col("Price") - col("Price_MovingAverage")) /col("Price_MovingAverage"))).first().getDouble(0)

val priceFlunctationDuringNormal = trendNormal.select(avg(abs(col("Price") - col("Price_MovingAverage")) /col("Price_MovingAverage"))).first().getDouble(0)

// Compute when price flunctuation when stock is not trending
val priceFlunctationNotTrend = notTrending.select(avg(abs(col("Price") - col("Price_MovingAverage")) /col("Price_MovingAverage"))).first().getDouble(0)

trendSpikes.show()
trendNormal.show()
notTrending.show()

println("When AAPL is trending 2x or higher of average on google search, current price differ from 14-day moving average by "+ priceFlunctationDuringTrendSpike*100+ "%")
println("When AAPL is trending normal of average on google search, current price differ from 14-day moving average by "+ priceFlunctationDuringNormal*100+ "%")
println("When AAPL is trending -2x or lower of average on google search, current price differ from 14-day moving average by "+ priceFlunctationNotTrend*100+ "%")

// trendSpikes.write.option("header", "true").csv()
// trendNormal.write.option("header", "true").csv()
// notTrending.write.option("header", "true").csv()

// // Write filtered data to new CSV file
// filteredDF.write.csv(".csv")

spark.stop()
