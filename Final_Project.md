# Final Project
Hive, Scala on Spark, and Java

### Problem 1

1. In Oshkosh, which is more common: days where the temperature was really cold (-10 or lower) at any point during the day or days where the temperature was hot (95 or higher) at any point during the day?

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

2. When I moved from Wisconsin to Iowa for school, the summers and winters seemed similar but the spring and autumn seemed much more tolerable. For this problem, we will be using meteorological seasons:

```
Winter - Dec, Jan, Feb
Spring - Mar, Apr, May
Summer - Jun, Jul, Aug
Fall - Sep, Oct, Nov
```
Compute the average temperature (sum all temperatures in the time period and divide by the number of readings) for each season for Oshkosh and Iowa City. What is the difference in average temperatures for each season for Oshkosh vs Iowa City?

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

3. For Oshkosh, what 7 day period was the hottest? By hottest I mean, the average temperature of all readings from 12:00am on day 1 to 11:59pm on day 7.

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

4. Solve this problem for Oshkosh only. For each day in the input file (e.g. February 1, 2004, May 11, 2010, January 29, 2007), determine the coldest time for that day. The coldest time for any given day is defined as the hour that has the coldest average. For example, a day may have had two readings during the 4am hour, one at 4:15am and one at 4:45am. The temperatures may have been 10.5 and 15.3. The average for 4am is 12.9. The 5am hour for that day may have had two readings at 5:14am and 5:35am and those readings were 11.3 and 11.5. The average for 5am is 11.4. 5am is thus considered colder. Once you have determined the coldest hour for each day, return the hour that has the most occurrences of the coldest average.

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

5. Which city had a time period of 24 hours or less that saw the largest temperature difference? Report the city, the temperature difference and the minimum amount of time it took to obtain that difference. Do not only consider whole days for this problem. The largest temperature difference may have been from 3pm on a Tuesday to 3pm on a Wednesday. The
largest temperature difference could have been from 11:07am on a Tuesday to 4:03am on a Wednesday. Or the largest difference could have been from 3:06pm on a Wednesday to 7:56pm on that same Wednesday. For a concrete example, consider Iowa City on January 1, 2000 at 2:53pm through January 2, 2000 at 2:53pm. The maximum temperature in that 24 hour span was 54 and the minimum temperature in that 24 hour span was 36. Therefore, in that 24 hour span, the largest temperature difference was 18 degrees. If this were the final answer, you would output “Iowa City”, “18 degrees” and January 2, 2000 3:53am to January 2, 2000 10:53am.

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

6. As a runner, I want to know when is the best time and place to run. For each month, provide the hour (e.g. 7am, 5pm, etc) and city that is the best time to run. The best time and place to run will be defined as the time where the temperature is as close to 50 as possible. For this problem, you are averaging all temperatures with the same city and same hour and checking how far that average is from 50 degrees. If there is a tie, a tiebreaker will be the least windy hour on average. If there is still a tie, both hours and cities are reported.

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

## Problem 2

The solution to this problem must be done using Java threads. Due to budget cutbacks, the postal services at UW-Oshkosh can only afford 1 mail deliverer. Even worse, that deliverer is a student who works part-time. Postal services wants to minimize the amount of time that student has to work in order to save money. Because of this, they are interested in the fastest way to visit all buildings and return back to the Campus Services Building, BldgOne in the example below. There are obvious routes that are terrible (e.g. going from one side of campus to the other and then back) but the optimal route is not obvious. Your goal is to read in a file that gives the time in seconds to get from a building to every other building and determine the best possible route such that you start at the building listed on the first line, visit all other buildings and end at the building listed on the first line. The building names in the example below are arbitrary and can be called anything. The input file you will read in is called input2.txt​ and will be formatted in the following manner:

```
BldgOne : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive) 
BldgTwo : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive) 
BldgThree : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive) 
BldgFour : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive)
BldgFive : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive)
```

Take the first line for example. t(BldgTwo) will be an integer value denoting the number of seconds it takes to get from BldgOne to BldgTwo. On the first line, t(BldgOne) will be 0. In other words, it takes 0 time to get from BldgOne to BldgOne. The input will always be formatted in this manner. If another building is constructed, it will be added to the end and the file will be updated accordingly. For example, if BldgSix were constructed, the time to BldgSix will be added at the end of every list and BldgSix will be added to the end of the file. The time from BldgOne to BldgThree may not be the same as the time from BldgThree to BldgOne. There may be one way streets; it may be uphill, etc. A sample input file is shown below:

```
BldgA : 0 5 7
BldgB : 4 0 3
BldgC : 6 4 0
```

___Your goal is this___: for the best route possible, print out the total time taken to start with the building on the first line, visit all buildings and then return to the building on the first line. You must also output the order in which you visited the buildings using the name of the buildings as defined in the input file. In the above example, there are only 2 possible routes: BldgA -> BldgB -> BldgC -> BldgA which is 5 + 3 + 6 == 14. The other route is: BldgA -> BldgC -> BldgB -> BldgA which is 7 + 4 + 4 == 15. Therefore, the output would be:

```
14 BldgA BldgB BldgC
```

## Solution in Python

```python
#!/usr/bin/env python3

import sys
from itertools import permutations
import timeit

start_time = timeit.default_timer()

buildings_dict = {}
building_id_map_list = []
building_id_map_dict = {}

# read in file and create list of buildings and dict of buildings + times
with open(sys.argv[1],"r") as file:
    for i, line in enumerate(file):
        line = line.split(" : ")
        building = line[0]
        travel_times = line[1].strip("\n").split(" ")
        buildings_dict[i] = list(map(int, travel_times))
        building_id_map_list.append((i, building))
        building_id_map_dict[i] = building

# generate permutations of routes
perm_list = list(permutations([i for i, name in building_id_map_list[1:]]))
all_routes = []
for i in range(len(perm_list)):
    perm = list(perm_list[i])
    perm.insert(0,building_id_map_list[0][0])
    perm.append(building_id_map_list[0][0])
    all_routes.append(perm)

# create new list to contain route times
route_times = []

# loop through every route (permutation) in all_routes and set total_time to 0
for i, route in enumerate(all_routes):
    total_time = 0

    # loop through sequence of indices for every route and define current + next building
    for j in range(1, len(route)):
        current_building = route[j-1]
        next_building = route[j]

        # get time to go from current building to next building
        time_to_next_bld = buildings_dict[current_building][next_building]
        total_time += time_to_next_bld
    
    # append route index and time to route_times
    route_times.append((i, total_time))


# get minimum travel time
min_time = min(route_times, key = lambda route: route[1])

# get route
route = ' '.join([str(building_id_map_dict[i]) for i in all_routes[min_time[0]][:-1]])

# subset route time
time = min_time[1]

print(f"{time} {route}")

stop_time = timeit.default_timer()

print('Time elapsed: ', stop_time - start_time)
```

## Solution in Java

```java
import java.util.*;
import java.io.*;
public class RouteOptimizer extends Thread {
    // private ListIterator<List<Integer>> iterator;
    private List<List<Integer>> perm_subset;
    private TreeMap<Integer, ArrayList<Integer>> buildings_map;
    private TreeMap<Integer, Integer> route_times;
    private int start;
    
    public RouteOptimizer(List<List<Integer>> perm_subset, TreeMap<Integer, ArrayList<Integer>> buildings_map, int start) throws Exception {
        this.buildings_map = buildings_map;
        this.route_times = new TreeMap<>();
        this.start = start;
        this.perm_subset = perm_subset;
    }

    public void run() {
        // for each route in an array, compute the time of the route and assign the route an id. add this to route_times array.

        int i = start;
        for (List<Integer> route : perm_subset) {
            int total_time = 0;

            for (int j = 1; j < route.size(); j++) {
                int current_building_index = route.get(j-1);
                int next_building_index = route.get(j);
                ArrayList<Integer> building_times = buildings_map.get(current_building_index);
                int time_to_next_bld = building_times.get(next_building_index);
                total_time += time_to_next_bld;
            }

            route_times.put(i, total_time);
            i += 1;
        }
    }

    public TreeMap<Integer, Integer> get_route_times() {
        return route_times;
    }

    // function to generate permutations
    public static List<List<Integer>> get_permutations(List<Integer> original) {
        if (original.size() == 0) {
            List<List<Integer>> result = new ArrayList<List<Integer>>(); 
            result.add(new ArrayList<Integer>()); 
            return result; 
        }

        Integer firstElement = original.remove(0);
        List<List<Integer>> returnValue = new ArrayList<List<Integer>>();
        List<List<Integer>> permutations = get_permutations(original);

        for (List<Integer> smallerPermutated : permutations) {
            for (int index=0; index <= smallerPermutated.size(); index++) {
                List<Integer> temp = new ArrayList<Integer>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    public static void main(String[] args) throws Exception {
        // Start timer
        long startTime = System.currentTimeMillis();
        
        TreeMap<Integer, ArrayList<Integer>> buildings_map = new TreeMap<>();
        TreeMap<Integer, String> buildings_id_map = new TreeMap<>();
        List<Integer> input_list = new ArrayList<>();

        // Read in file
        File file = new File("input2.txt");

        // read in file
        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            {
                int building_id = 0;
                for(String line;  (line = br.readLine()) != null; building_id++ ) {
                    // split file into list by colon
                    ArrayList<String> line_list = new ArrayList<String>(Arrays.asList(line.split(" : ")));
                    String building_name = line_list.get(0);

                    // convert (string) travel times to list
                    ArrayList<String> travel_times_str = new ArrayList<String>(Arrays.asList(line_list.get(1).replace("\n", "").split(" ")));
                    ArrayList<Integer> travel_times = new ArrayList<Integer>();
                    
                    // convert travel times to int
                    for(String time : travel_times_str) travel_times.add(Integer.valueOf(time));
                    
                    // write building key + travel times to TreeMap
                    buildings_map.put(building_id, travel_times);
                    buildings_id_map.put(building_id, building_name);
                    if(building_id != 0) {
                        input_list.add(building_id);
                    }
                }
            }

        }

        // get permutations
        List<List<Integer>> permutations = get_permutations(input_list);

        // loop through permutations and add start/end building
        for (int i = 0; i < permutations.size(); i++) {
            List<Integer> perm = permutations.get(i);
            perm.add(0,0);
            perm.add(0);
            permutations.set(i, perm);
        }

        // section start: split the list of permutations into chunks, and assign each chunk to a thread for mapping route to time
        int num_permutations = permutations.size();
        int num_threads = 1;

        if(num_permutations >= 100) {
            num_threads = 20;
        }
        
        RouteOptimizer[] agents = new RouteOptimizer[num_threads];
        
        int count = 1;
        int start = 0;
        int perm_per_thread = num_permutations / num_threads;
        int thread_id = 0;
        int len_test = 0;

        for (int i = 0; i < num_permutations; i++) {
            if(count == perm_per_thread) {
                if((i + 1) == num_permutations) i += 1;
                List<List<Integer>> perm_subset = permutations.subList(start, i);
                agents[thread_id] = new RouteOptimizer(perm_subset, buildings_map, start);
                agents[thread_id].start();
                start = i;
                count = 0;
                thread_id += 1;
            }
            count += 1;
        }
        // section end

        TreeMap<Integer, Integer> all_routes = new TreeMap<>();

        // section start: get mapped list of route times for each thread
        for(RouteOptimizer agent : agents) {
            if(agent.isAlive()) {
                agent.join();
            }

            TreeMap<Integer, Integer> thread_routes = agent.get_route_times();

            for (Map.Entry<Integer, Integer> entry : thread_routes.entrySet()) {
                int i = entry.getKey();
                int time = entry.getValue();
                all_routes.put(i, time);
            }
        }

        // get minimum route time
        Integer min_time = all_routes.values().stream().min(Integer::compare).get();

        // get route for minimum route time
        for (Map.Entry<Integer, Integer> entry : all_routes.entrySet()) {
            if (entry.getValue().equals(min_time)) {
                FileWriter fileWriter = new FileWriter("output2.txt");
                PrintWriter output = new PrintWriter(fileWriter);

                Integer min_index = entry.getKey();             
                List<Integer> route_indices = permutations.get(min_index);
                String optimal_route = Integer.toString(min_time);
                
                // print results
                for (int i = 0; i < route_indices.size()-1; i++) {
                    String next_building = buildings_id_map.get(route_indices.get(i));
                    optimal_route = optimal_route + " " + next_building;
                }
                output.print(optimal_route);
                output.close();
                break;
            }
        }       

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println(elapsedTime);
    }   
}
```

## Problem 3

### About the data

The [dataset](http://stat-computing.org/dataexpo/2009/the-data.html) I used for this problem "consists of flight arrival and departure details for all commercial flights within the USA, from October 1987 to April 2008"<sup>[1]</sup>. I originally found this dataset from [Hadoop Illuminated](https://hadoopilluminated.com/hadoop_illuminated/Public_Bigdata_Sets.html) <sup>[3]</sup>.

- <sup>[1]</sup> Data source, http://stat-computing.org/dataexpo/2009/the-data.html
- <sup>[2]</sup> Data info, http://stat-computing.org/dataexpo/2009/
- <sup>[3]</sup> Hadoop Illuminated, https://hadoopilluminated.com/hadoop_illuminated/Public_Bigdata_Sets.html

### Data format

All of the source data files are compressed CSV files. There are 22 of them in total. Compressed - their collective size is about 1.6gb. Uncompressed - their size is about 12 gb. I downloaded them onto EC2, uncompressed them, and then transferred these files to S3. The core data was accompanied by supplementary files that I used for mapping (airports and carriers). 

### S3 storage

The data is stored on S3 at the following locations:

* **Flight data**: s3a://ebfp3/flight_data/
* **Supplemental data** (carriers and airports): s3a://ebfp3/supp_data/

### Questions and Answers
---

#### Question 1
* Since 1987, which airline has most departure delays per year *for all 22 years of collected data*? Said differently: which airline was the most late the most often?

#### Code

```scala
import org.apache.spark.sql.expressions.Window;

// ------------ Setup ------------

val data = spark.read.format("csv").option("header", "true").option("inferSchema", true).load("s3a://ebfp3/flight_data/*.csv");

val carriers = spark.read.format("csv").option("header", "true").option("inferSchema", true).load("s3a://ebfp3/supp_data/carriers.csv");

val airports = spark.read.format("csv").option("header", "true").option("inferSchema", true).load("s3a://ebfp3/supp_data/airports.csv");

data.write.parquet("data_p");
carriers.write.parquet("carriers_p");
airports.write.parquet("airports_p");

val df_p = spark.read.option("header","true").option("inferSchema", true).parquet("data_p");
val df_carriers = spark.read.option("header","true").option("inferSchema", true).parquet("carriers_p");
val df_airports = spark.read.option("header","true").option("inferSchema", true).parquet("airports_p");

df_p.createOrReplaceTempView("ap");
df_carriers.createOrReplaceTempView("ap_carriers");
df_airports.createOrReplaceTempView("ap_airports");

// ------------ Question 1 ------------

val view_data = spark.sqlContext.sql("SELECT Year, UniqueCarrier, sum(cast(DepDelay as INT)) as sum_dep_delay FROM ap GROUP BY Year, UniqueCarrier");

val view_carriers = spark.sqlContext.sql("SELECT Code, Description FROM ap_carriers");

val joined_data = view_data.join(view_carriers, view_data.col("UniqueCarrier") === view_carriers.col("Code")).drop("Code");

val window = Window.partitionBy("year").orderBy($"sum_dep_delay".desc);
val ranked_data = joined_data.withColumn("delay_rank", rank over window).where($"delay_rank" === 1).orderBy($"year").drop("delay_rank");

val final_output = ranked_data.groupBy($"Description").agg(count($"Description").alias("Count")).sort($"Count".desc);
```

#### Answer 1

```
+--------------------+-----+
|        Description |Count|
+--------------------+-----+
|Southwest Airline...|    9|
|Delta Air Lines Inc.|    7|
|United Air Lines ...|    3|
|US Airways Inc. (...|    2|
|Continental Air L...|    1|
+--------------------+-----+
```

Southwest Airlines had the highest frequency of "most departure delays per year". The reverse analysis of most frequently having the *least delays per year* was Alaska Airlines.

#### Question 2 
* For each year, which airline flew the least of amount of miles compared to the yearly average (of miles flown by all airlines). Display the year, month, carrier, and % difference.

#### Code 2
```scala
// ------------ Question 2 ------------

val view_data = spark.sqlContext.sql("SELECT Year, Month, UniqueCarrier, sum(cast(Distance as INT)) as sum_distance FROM ap GROUP BY Year, Month, UniqueCarrier");

val view_carriers = spark.sqlContext.sql("SELECT Code, Description FROM ap_carriers");

val joined_data = view_data.join(view_carriers, view_data.col("UniqueCarrier") === view_carriers.col("Code")).drop("Code");

val window = Window.partitionBy("year");
val data_with_avg = joined_data.withColumn("yearly_avg", avg($"sum_distance") over window).withColumn("percent_diff", round(($"sum_distance" - $"yearly_avg") / $"yearly_avg", 4)).drop($"yearly_avg");

val window2 = Window.partitionBy("year").orderBy("percent_diff");
val ranked_data = data_with_avg.withColumn("ranking", rank over window2).where($"ranking" === 1).drop("UniqueCarrier").drop("sum_distance").drop("ranking").sort($"Year");
```

#### Answer 2

```
+----+-----+--------------------+------------+
|Year|Month|         Description|percent_diff|
+----+-----+--------------------+------------+
|1987|   10|Pan American Worl...|      -0.837|
|1988|    4|Pacific Southwest...|     -0.9357|
|1989|    5|Eastern Air Lines...|     -0.9588|
|1990|    2|Alaska Airlines Inc.|     -0.8394|
|1991|   11|Pan American Worl...|     -0.9001|
|1992|    2|Alaska Airlines Inc.|     -0.8515|
|1993|    2|Alaska Airlines Inc.|      -0.877|
|1994|    2|Alaska Airlines Inc.|     -0.8293|
|1995|    2|Alaska Airlines Inc.|     -0.7818|
|1996|   11|Alaska Airlines Inc.|     -0.7592|
|1997|    2|Alaska Airlines Inc.|     -0.7733|
|1998|    2|Alaska Airlines Inc.|     -0.7729|
|1999|    2|Alaska Airlines Inc.|     -0.7658|
|2000|   11| Aloha Airlines Inc.|     -0.9768|
|2001|    2| Aloha Airlines Inc.|     -0.9708|
|2002|    2|Alaska Airlines Inc.|     -0.7308|
|2003|   11|Hawaiian Airlines...|     -0.8977|
|2004|   10|Hawaiian Airlines...|     -0.9022|
|2004|   11|Hawaiian Airlines...|     -0.9022|
|2004|    9|Hawaiian Airlines...|     -0.9022|
|2005|    2|Hawaiian Airlines...|     -0.9066|
|2006|    4| Aloha Airlines Inc.|     -0.9308|
|2007|    2| Aloha Airlines Inc.|     -0.9333|
|2008|    2| Aloha Airlines Inc.|     -0.9275|
+----+-----+--------------------+------------+
```

**Interpretting the results**: in 1987, Pan American flew 83.7% less miles than the average mileage flown by all airlines for that year. By 1991, that figure was 90% for Pan American. In another case: in 1989 Eastern Airlines flew 95.9% less miles than the yearly average. Not surprisingly - both airlines went out of business by the end of 1991. 

Note that Alaska Airlines also had an underutilized fleet in the 90s. However, this airline changed it's pricing structure to increase competitiveness (by discounting fares drastically), and began to win over market share as a result. 

#### Question 3
* For every airport, which months of the year have the most delays with exception of November and December? Delays are defined as CarrierDelay + WeatherDelay + NASDelay + SecurityDelay + LateAircraftDelay.
    * Output the airport, city, month, sum of delays. 
    * Output only top 20 airports

#### Code 3
```scala
// ------------ Question 3 ------------

val view_data = spark.sqlContext.sql("SELECT Origin, Month, sum(cast(CarrierDelay as INT)+ cast(WeatherDelay as INT) + cast(NASDelay as INT) + cast(SecurityDelay as INT) + cast(LateAircraftDelay as INT)) as sum_delays FROM ap WHERE Month != 11 and Month != 12 GROUP BY Origin, Month");

val view_airports = spark.sqlContext.sql("SELECT iata, airport, city FROM ap_airports");

val joined_data = view_data.join(view_airports, view_data.col("Origin") === view_airports.col("iata")).drop("Origin","iata").filter("sum_delays is not null");

val window = Window.partitionBy("airport").orderBy($"sum_delays".desc);
val ranked_data = joined_data.withColumn("ranking", rank over window).where($"ranking" === 1).sort($"sum_delays".desc).drop("ranking").select("airport","city","Month","sum_delays");
```

#### Answer 3

```
+--------------------+-----------------+-----+----------+
|             airport|             city|Month|sum_delays|
+--------------------+-----------------+-----+----------+
|William B Hartsfi...|          Atlanta|    7|   4389913|
|Chicago O'Hare In...|          Chicago|    7|   3670572|
|Dallas-Fort Worth...|Dallas-Fort Worth|    6|   2815249|
|         Newark Intl|           Newark|    7|   1856703|
|George Bush Inter...|          Houston|    6|   1621504|
|         Denver Intl|           Denver|    6|   1407076|
| John F Kennedy Intl|         New York|    7|   1400956|
|   Philadelphia Intl|     Philadelphia|    7|   1374650|
|           LaGuardia|         New York|    7|   1208416|
|Detroit Metropoli...|          Detroit|    6|   1168501|
|Los Angeles Inter...|      Los Angeles|    7|   1163292|
|McCarran Internat...|        Las Vegas|    7|   1146545|
|Gen Edw L Logan Intl|           Boston|    7|   1138799|
|Phoenix Sky Harbo...|          Phoenix|    7|   1087779|
|Washington Dulles...|        Chantilly|    6|   1049662|
|Minneapolis-St Pa...|      Minneapolis|    6|    989554|
|Orlando Internati...|          Orlando|    7|    962422|
|Charlotte/Douglas...|        Charlotte|    6|    961091|
|Baltimore-Washing...|        Baltimore|    6|    896289|
|San Francisco Int...|    San Francisco|    6|    841711|
+--------------------+-----------------+-----+----------+
```

Clearly, the summer months of June and July are the busiest time of year to travel (with exception of Nov, Dec), as evidenced by the number of delays. Hartsfield Intl. airport, being the busiest airport in the world by passenger traffic, has the most delays.

#### Question 4
* What is the most popular route of each airline? Route is defined as a combo of origin and destination. The time frame includes all years present in data. 
    * Output airline, origin, destination, and count of flights.

#### Code 4

```scala
// ------------ Question 4 ------------

val view_data = spark.sqlContext.sql("SELECT UniqueCarrier as Carrier, Origin, Dest, count(*) as count_flights FROM ap GROUP BY UniqueCarrier, Origin, Dest");

val view_carriers = spark.sqlContext.sql("SELECT Code, Description as Airline FROM ap_carriers");

val joined_data = view_data.join(view_carriers, view_data.col("Carrier") === view_carriers.col("Code")).drop("Code","Carrier");

val window = Window.partitionBy("Airline").orderBy($"count_flights".desc);
val ranked_data = joined_data.withColumn("ranking", rank over window).where($"ranking" === 1).sort($"count_flights".desc).drop("ranking").select("Airline","Origin","Dest","Count_flights");
```

#### Answer 4

```
+--------------------+------+----+-------------+
|             Airline|Origin|Dest|Count_flights|
+--------------------+------+----+-------------+
|Southwest Airline...|   HOU| DAL|       230971|
|United Air Lines ...|   SFO| LAX|       191983|
|American Airlines...|   ORD| DFW|       136731|
|Continental Air L...|   BOS| EWR|       111611|
|Delta Air Lines Inc.|   ATL| LGA|       107065|
|US Airways Inc. (...|   BOS| PHL|       106837|
|Alaska Airlines Inc.|   ANC| SEA|       104826|
|Northwest Airline...|   MSP| DTW|       102806|
|America West Airl...|   LAS| PHX|        89209|
|American Eagle Ai...|   SAN| LAX|        62283|
|Trans World Airwa...|   MCI| STL|        49293|
|Skywest Airlines ...|   SAN| LAX|        41894|
|Hawaiian Airlines...|   OGG| HNL|        37185|
|AirTran Airways C...|   MCO| ATL|        24152|
|     JetBlue Airways|   JFK| FLL|        23936|
| Aloha Airlines Inc.|   OGG| HNL|        22031|
|Expressjet Airlin...|   IAH| DAL|        21904|
|Pan American Worl...|   LGA| BOS|        21891|
|Atlantic Southeas...|   ATL| PFN|        16099|
|         Comair Inc.|   DCA| BOS|        12449|
|  Mesa Airlines Inc.|   PHX| TUS|        11110|
|Eastern Air Lines...|   BOS| DCA|        10494|
|ATA Airlines d/b/...|   LGA| MDW|        10425|
|    Independence Air|   JFK| IAD|         8909|
|Frontier Airlines...|   DEN| LAS|         8655|
|Piedmont Aviation...|   CLT| GSO|         6256|
|Pacific Southwest...|   SFO| LAX|         4149|
|Pinnacle Airlines...|   CVG| DTW|         3856|
|Midway Airlines I...|   DTW| MDW|         2539|
+--------------------+------+----+-------------+
```