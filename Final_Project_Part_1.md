# Final Project - Part 1

### Big data analysis with Hive and Scala on Spark

#### Question 1

In Oshkosh, which is more common: days where the temperature was really cold (-10 or lower) at any point during the day or days where the temperature was hot (95 or higher) at any point during the day?

#### Solution

```sql
DROP TABLE IF EXISTS oshkosh_tbl;
CREATE EXTERNAL TABLE IF NOT EXISTS oshkosh_tbl(year INT, month INT,
day INT, time STRING, temperature FLOAT, dewpoint FLOAT, humidity INT, sea_level_pressure FLOAT, visibility FLOAT, wind_direction STRING, wind_speed FLOAT, gust_speed FLOAT, precip FLOAT, events STRING, conditions STRING, wind_degrees INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/final/Oshkosh/';


SELECT
    'cold' as temp,
    COUNT(DISTINCT concat_ws('/', CAST(month AS STRING), CAST(day AS STRING), CAST(year AS STRING))) as num_days
FROM 
    oshkosh_tbl o
WHERE
    temperature <= -10 AND
    temperature != -9999

UNION

SELECT
    'hot' as temp,
    COUNT(DISTINCT concat_ws('/', CAST(month AS STRING), CAST(day AS STRING), CAST(year AS STRING))) as num_days
FROM 
    oshkosh_tbl o
WHERE
    temperature >= 95 AND
    temperature != -9999
```

#### Question 2

When I moved from Wisconsin to Iowa for school, the summers and winters seemed similar but the spring and autumn seemed much more tolerable. For this problem, we will be using meteorological seasons:

```
Winter - Dec, Jan, Feb
Spring - Mar, Apr, May
Summer - Jun, Jul, Aug
Fall - Sep, Oct, Nov
```
Compute the average temperature (sum all temperatures in the time period and divide by the number of readings) for each season for Oshkosh and Iowa City. What is the difference in average temperatures for each season for Oshkosh vs Iowa City?

#### Solution

```sql
DROP TABLE IF EXISTS oshkosh_tbl;
CREATE EXTERNAL TABLE IF NOT EXISTS oshkosh_tbl(year INT, month INT,
day INT, time STRING, temperature FLOAT, dewpoint FLOAT, humidity INT, sea_level_pressure FLOAT, visibility FLOAT, wind_direction STRING, wind_speed FLOAT, gust_speed FLOAT, precip FLOAT, events STRING, conditions STRING, wind_degrees INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/final/Oshkosh/';
DROP TABLE IF EXISTS iowacity_tbl;
CREATE EXTERNAL TABLE IF NOT EXISTS iowacity_tbl(year INT, month INT,
day INT, time STRING, temperature FLOAT, dewpoint FLOAT, humidity INT, sea_level_pressure FLOAT, visibility FLOAT, wind_direction STRING, wind_speed FLOAT, gust_speed FLOAT, precip FLOAT, events STRING, conditions STRING, wind_degrees INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/final/IowaCity/';


SELECT
    oshkosh.season as season,
    round(oshkosh.avg_temp - iowacity.avg_temp, 2) as avg_temp_diff
FROM
    (
    SELECT
        CASE 
            WHEN month IN (12, 1, 2) THEN 'Winter'
            WHEN month IN (3, 4, 5) THEN 'Spring'
            WHEN month IN (6, 7, 8) THEN 'Summer'
            WHEN month IN (9, 10, 11) THEN 'Fall'
            else 'NA'
        END AS season,
        sum(temperature)/count(temperature) as avg_temp
    FROM 
        oshkosh_tbl o
    WHERE
        temperature IS NOT NULL AND
        temperature != -9999
    GROUP BY
        CASE 
            WHEN month IN (12, 1, 2) THEN 'Winter'
            WHEN month IN (3, 4, 5) THEN 'Spring'
            WHEN month IN (6, 7, 8) THEN 'Summer'
            WHEN month IN (9, 10, 11) THEN 'Fall'
            else 'NA'
        END
    ) oshkosh
JOIN
    (
    SELECT
        CASE 
            WHEN month IN (12, 1, 2) THEN 'Winter'
            WHEN month IN (3, 4, 5) THEN 'Spring'
            WHEN month IN (6, 7, 8) THEN 'Summer'
            WHEN month IN (9, 10, 11) THEN 'Fall'
            else 'NA'
        END AS season,
        sum(temperature)/count(temperature) as avg_temp
    FROM 
        iowacity_tbl i
    WHERE
        temperature IS NOT NULL AND
        temperature != -9999
    GROUP BY
        CASE 
            WHEN month IN (12, 1, 2) THEN 'Winter'
            WHEN month IN (3, 4, 5) THEN 'Spring'
            WHEN month IN (6, 7, 8) THEN 'Summer'
            WHEN month IN (9, 10, 11) THEN 'Fall'
            else 'NA'
        END
    ) iowacity
ON
    oshkosh.season = iowacity.season
```
#### Question 3

For Oshkosh, what 7 day period was the hottest? By hottest I mean, the average temperature of all readings from 12:00am on day 1 to 11:59pm on day 7.


#### Solution

```sql
DROP TABLE IF EXISTS oshkosh_tbl;
CREATE EXTERNAL TABLE IF NOT EXISTS oshkosh_tbl(year INT, month INT,
day INT, time STRING, temperature FLOAT, dewpoint FLOAT, humidity INT, sea_level_pressure FLOAT, visibility FLOAT, wind_direction STRING, wind_speed FLOAT, gust_speed FLOAT, precip FLOAT, events STRING, conditions STRING, wind_degrees INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/final/Oshkosh/';


SELECT
    year,
    month,
    week,
    avg_temp
FROM 
    (
    SELECT
        year,
        month,
        CASE
            WHEN day >= 1 and day <= 7 THEN 'week_1'
            WHEN day >= 8 and day <= 14 THEN 'week_2'
            WHEN day >= 15 and day <= 21 THEN 'week_3'
            WHEN day >= 22 and day <= 28 THEN 'week_4'
            WHEN day >= 29 and day <= 31 THEN 'week_5'
        END as week,
        sum(temperature)/count(temperature) as avg_temp
    FROM 
        oshkosh_tbl o
    WHERE
        temperature IS NOT NULL AND 
        temperature != -9999
    GROUP BY
        year,
        month,
        CASE
            WHEN day >= 1 and day <= 7 THEN 'week_1'
            WHEN day >= 8 and day <= 14 THEN 'week_2'
            WHEN day >= 15 and day <= 21 THEN 'week_3'
            WHEN day >= 22 and day <= 28 THEN 'week_4'
            WHEN day >= 29 and day <= 31 THEN 'week_5'
        END
    ) sub
ORDER BY
    avg_temp DESC
LIMIT 1
```

#### Question 4

Solve this problem for Oshkosh only. For each day in the input file (e.g. February 1, 2004, May 11, 2010, January 29, 2007), determine the coldest time for that day. The coldest time for any given day is defined as the hour that has the coldest average. For example, a day may have had two readings during the 4am hour, one at 4:15am and one at 4:45am. The temperatures may have been 10.5 and 15.3. The average for 4am is 12.9. The 5am hour for that day may have had two readings at 5:14am and 5:35am and those readings were 11.3 and 11.5. The average for 5am is 11.4. 5am is thus considered colder. Once you have determined the coldest hour for each day, return the hour that has the most occurrences of the coldest average.

#### Solution

```sql
DROP TABLE IF EXISTS oshkosh_tbl;
CREATE EXTERNAL TABLE IF NOT EXISTS oshkosh_tbl(year STRING, month STRING,
day STRING, time STRING, temperature FLOAT, dewpoint FLOAT, humidity INT, sea_level_pressure FLOAT, visibility FLOAT, wind_direction STRING, wind_speed FLOAT, gust_speed FLOAT, precip FLOAT, events STRING, conditions STRING, wind_degrees INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/final/Oshkosh/';

SELECT
    hour_d
FROM
    (
    SELECT
        hour_d,
        occurrences
    FROM
        (
        SELECT
            hour_d,
            count(hour_d) as occurrences
        FROM
            (
            SELECT
                date_d,
                hour_d,
                rank() over(partition by date_d order by avg_temp) as ranking
            FROM
                (
                SELECT
                    to_date(concat_ws('-', year, month, day)) as date_d,
                    concat(split(time, ":")[0],split(time," ")[1]) as hour_d,
                    avg(temperature) as avg_temp
                FROM 
                    oshkosh_tbl o
                WHERE
                    temperature IS NOT NULL AND
                    temperature != -9999
                GROUP BY
                    to_date(concat_ws('-', year, month, day)),
                    concat(split(time, ":")[0],split(time," ")[1])
                ) sub
            ) sub2
        WHERE
            ranking == 1
        GROUP BY
            hour_d
        ) sub3
    ORDER BY
        occurrences DESC
    LIMIT 1
    ) sub4
```

#### Question 5

Which city had a time period of 24 hours or less that saw the largest temperature difference? Report the city, the temperature difference and the minimum amount of time it took to obtain that difference. Do not only consider whole days for this problem. The largest temperature difference may have been from 3pm on a Tuesday to 3pm on a Wednesday. The largest temperature difference could have been from 11:07am on a Tuesday to 4:03am on a Wednesday. Or the largest difference could have been from 3:06pm on a Wednesday to 7:56pm on that same Wednesday. For a concrete example, consider Iowa City on January 1, 2000 at 2:53pm through January 2, 2000 at 2:53pm. The maximum temperature in that 24 hour span was 54 and the minimum temperature in that 24 hour span was 36. Therefore, in that 24 hour span, the largest temperature difference was 18 degrees. If this were the final answer, you would output “Iowa City”, “18 degrees” and January 2, 2000 3:53am to January 2, 2000 10:53am.

#### Solution

```scala
%spark
import sqlContext.implicits._
import org.apache.spark.sql.expressions.Window

// Oshkosh

// Load data
val oshkosh_weather = spark.read.format("csv").option("header", true).option("inferSchema", true).load("/user/maria_dev/final/Oshkosh/OshkoshWeather.csv")
oshkosh_weather.createOrReplaceTempView("okw")

// Pull various columns. Create timestamp from dates.
val updated_view = spark.sqlContext.sql("SELECT to_timestamp(CONCAT(Year,'-',Month,'-',Day,' ',TimeCST), 'yyyy-MM-dd h:m a') as from_dt, to_timestamp(CONCAT(Year,'-',Month,'-',Day,' ',TimeCST), 'yyyy-MM-dd h:m a') + INTERVAL 24 HOURS as to_dt, CAST(to_timestamp(CONCAT(Year,'-',Month,'-',Day,' ',TimeCST), 'yyyy-MM-dd h:m a') AS Long) dt_long, TemperatureF as temp FROM okw WHERE TemperatureF != -9999")

// Setup window for window function
val window = Window.orderBy("dt_long").rangeBetween(0, 86400)

// Run query that takes max-min difference of 24 hour window for every row (time)
val temp_diff_view = updated_view.withColumn("dt_long", $"dt_long").withColumn("temp_diff", max($"temp").over(window)-min($"temp").over(window)).drop("dt_long").drop("temp")

// Rank temp differences
val rank_window = Window.orderBy('temp_diff.desc)
val ranked_output = temp_diff_view.withColumn("rank", rank over rank_window)

// Produce final answer
val final_output = ranked_output.filter($"rank" === 1).drop("rank").show(1)

// IowaCity

// Load data
val iowacity_weather = spark.read.format("csv").option("header", true).option("inferSchema", true).load("/user/maria_dev/final/IowaCity/IowaCityWeather.csv")
iowacity_weather.createOrReplaceTempView("iow")

// Pull various columns. Create timestamp from dates.
val updated_view_io = spark.sqlContext.sql("SELECT to_timestamp(CONCAT(Year,'-',Month,'-',Day,' ',TimeCST), 'yyyy-MM-dd h:m a') as from_dt, to_timestamp(CONCAT(Year,'-',Month,'-',Day,' ',TimeCST), 'yyyy-MM-dd h:m a') + INTERVAL 24 HOURS as to_dt, CAST(to_timestamp(CONCAT(Year,'-',Month,'-',Day,' ',TimeCST), 'yyyy-MM-dd h:m a') AS Long) dt_long, TemperatureF as temp FROM iow WHERE TemperatureF != -9999")

// Setup window for window function
val window_io = Window.orderBy("dt_long").rangeBetween(0, 86400)

// Run query that takes max-min difference of 24 hour window for every row (time)
val temp_diff_view_io = updated_view_io.withColumn("dt_long", $"dt_long").withColumn("temp_diff", max($"temp").over(window_io)-min($"temp").over(window_io)).drop("dt_long").drop("temp")

// Rank temp differences
val rank_window_io = Window.orderBy('temp_diff.desc)
val ranked_output_io = temp_diff_view_io.withColumn("rank", rank over rank_window_io)

// Produce final answer
val final_output_io = ranked_output_io.filter($"rank" === 1).drop("rank").show(1)
```

#### Question 6

As a runner, I want to know when is the best time and place to run. For each month, provide the hour (e.g. 7am, 5pm, etc) and city that is the best time to run. The best time and place to run will be defined as the time where the temperature is as close to 50 as possible. For this problem, you are averaging all temperatures with the same city and same hour and checking how far that average is from 50 degrees. If there is a tie, a tiebreaker will be the least windy hour on average. If there is still a tie, both hours and cities are reported.

#### Solution

```scala
%spark
import sqlContext.implicits._
import org.apache.spark.sql.expressions.Window

// Load data
val oshkosh_weather = spark.read.format("csv").option("header", true).option("inferSchema", true).load("/user/maria_dev/final/Oshkosh/OshkoshWeather.csv")
val iowacity_weather = spark.read.format("csv").option("header", true).option("inferSchema", true).load("/user/maria_dev/final/IowaCity/IowaCityWeather.csv")

iowacity_weather.createOrReplaceTempView("iow")
oshkosh_weather.createOrReplaceTempView("okw")

// Pull various columns. Create timestamp from dates.
val ok_view = spark.sqlContext.sql("SELECT month, CONCAT(split(TimeCST, ':')[0], split(TimeCST, ' ')[1]) as hour, 'Oshkosh' as city, abs(avg(TemperatureF)-50) as temp_diff, avg(`Wind SpeedMPH`) as avg_wind FROM okw WHERE TemperatureF IS NOT NULL AND TemperatureF != -9999 AND `Wind SpeedMPH` IS NOT NULL AND `Wind SpeedMPH` != -9999 AND `Wind SpeedMPH` != 'Calm' GROUP BY month, CONCAT(split(TimeCST, ':')[0], split(TimeCST, ' ')[1]), 'Oshkosh'")

val io_view = spark.sqlContext.sql("SELECT month, CONCAT(split(TimeCST, ':')[0], split(TimeCST, ' ')[1]) as hour, 'IowaCity' as city, abs(avg(TemperatureF)-50) as temp_diff, avg(`Wind SpeedMPH`) as avg_wind FROM iow WHERE TemperatureF IS NOT NULL AND TemperatureF != -9999 AND `Wind SpeedMPH` IS NOT NULL AND `Wind SpeedMPH` != -9999 AND `Wind SpeedMPH` != 'Calm' GROUP BY month, CONCAT(split(TimeCST, ':')[0], split(TimeCST, ' ')[1]), 'IowaCity'")

val unioned_data = ok_view.union(io_view)
unioned_data.createOrReplaceTempView("combined")

val combined_view = spark.sqlContext.sql("SELECT month, hour, city, round(temp_diff, 2) as temp_diff, round(avg_wind,2) as avg_wind FROM combined")

// Setup window for window function
val window_temp = Window.partitionBy("month").orderBy("temp_diff")
val ranked_data = combined_view.withColumn("temp_rank", rank over window_temp)

val window_wind = Window.partitionBy("month").orderBy("avg_wind")
val filtered_data = ranked_data.where($"temp_rank" === 1).withColumn("wind_rank", rank over window_wind).orderBy($"month").drop("temp_diff").drop("avg_wind").drop("temp_rank").drop("wind_rank").show()
```