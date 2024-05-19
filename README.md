##Project Names

Google trend and stock price(Apple) Correlation

## Partners

James Lee and Siyu Lai


## Directory 

All the csv files and code used for this analysis will be in directory Final_project. The access for this directory will be provided.



## Building and Running Instructions


Below is a step by step instructions of FinalCode.Scala, which will lead to the result of this analysis. The goal of this code is to analyze the correlation between stock prices(Apple) and Google trends. 
First, the application reads “2009Jun.csv” file and “AAPL.csv” file which are input data and can be found in the same directory as the code of this application.

Then, we rename columns which are defined as val dfJunDateName(Date), dfAAPLDateName(Price Date), dfAAPLDateName2(ID), dfJun_dt(Trend), and dfAAPL_dt(Price). 

Afterwards, we join Dataframes on date column to keep only matching rows, which is defined by val joinedDF and  remove duplicate columns by function workingDF.

Now, we have to convert trend column to int(val workingDFWithIntegerTrend), price column to double(val workingDFWithDoublePrice), and ID column to int(val finalWorkingDF). 

Also, we have to compute moving averages so it completes all data needed for analysis.(val workingDFWithPMA, val workingDFWithMA).

Now, to get the price difference based on Google trend, we have to go through following steps.

First, we have to compute when trend is up positively(defined by val trendDifferencesDF) and the average trend up magnitude(defined by val trendUPavg). Notice the function Window take only a singe partition which displays warning during each run, especially during val trendUPavg, because moving average is a rolling function so data can’t be partitioned. Then, we have to cutoff at 2 * average to get all MAJOR trend spike ups, which are defined as val trendSpikes and val trendNormal. Also, we have to filter when stock is not trending for better analysis on correlation between stock price and Google trend, so we cutoff at 0.5 * average. Then, we compute average stock flunctuation in percentages over entire period defined by priceFlunctationAvg. Furthermore, we compute the stock fluctuation only over trend spikes which are defined by val priceFlunctationDuringTrendSpike and priceFlunctationDuringNormal, and price fluctuation when stock is not trending by using function val priceFlunctationNotTrend. 
Lastly, we print the price difference between current price from 14-day moving average  when Apple is trending two times or higher, normal of average, or two times lower of average  on Google trend. 
Then, the result of our code will display after building and running these steps on Scala.




