# Project 2: Apache Pig

1. Output the ​birth city​ of the player who had the most runs batted in (RBI) in his career.

```pig
batters_data = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' using PigStorage(',');
master_data = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
player_table = FOREACH batters_data GENERATE $0 AS id, $11 AS rbi;
master_table = FOREACH master_data GENERATE $0 AS id, $6 AS birthcity;
joined_data = JOIN player_table BY id, master_table BY id;
joined_data2 = FOREACH joined_data GENERATE $0 AS id, $3 AS birthcity, $1 AS rbi;
filtered_joined_data = FILTER joined_data2 BY rbi > 0;
joined_grouped = GROUP filtered_joined_data BY id;
player_sum_rbi =
    FOREACH joined_grouped {
        b = filtered_joined_data.birthcity;
        s = DISTINCT b;
        GENERATE group, FLATTEN(s), SUM(filtered_joined_data.rbi) AS sum_rbi;
    };
max_rbi_grouped = GROUP player_sum_rbi ALL;
total_max_rbi = FOREACH max_rbi_grouped GENERATE MAX(player_sum_rbi.sum_rbi) AS max_rbi_hits;
filter_player = FILTER player_sum_rbi BY sum_rbi == total_max_rbi.max_rbi_hits;
birth_city = FOREACH filter_player GENERATE birthcity;
limit_data = LIMIT birth_city 4;
DUMP limit_data;
```

2. Output the​ top three birthMonth/birthYear t​hat had the most players born. I am only looking for month and year combinations. For instance, how many were born in February, 1956, how many were born in March, 1975, and so on. Print out the top three ​mm/yyyy​ combinations. You must report the information in mm/yyyy form (i.e. it is ok to print out 5 instead of 05).

```pig
master_data = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
master_table = FOREACH master_data GENERATE $0 AS id, $1 AS birthyear, $2 AS birthmonth, CONCAT($2,'/',$1) AS month_year;
master_table = FILTER master_table BY $1 IS NOT NULL AND $2 IS NOT NULL;
grouped_data = GROUP master_table BY month_year;
count_births = FOREACH grouped_data GENERATE group AS month_year, COUNT(master_table.id) AS count_births;
grouped_births = GROUP count_births BY count_births;
grouped_births_fields = FOREACH grouped_births GENERATE group AS count_births, count_births.month_year AS month_year;
order_births = ORDER grouped_births_fields BY count_births DESC;
top_3 = LIMIT order_births 3;
result = FOREACH top_3 GENERATE month_year;
DUMP result;
```

3. There are 2 people that had unique heights. Who are they? You should output their first and last names.

```pig
master_data = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
master_table = FOREACH master_data GENERATE $0 AS id, $17 AS height, CONCAT($13,' ',$14) AS name;
master_table = FILTER master_table BY height IS NOT NULL;
grouped_heights = GROUP master_table BY height;
count_table = FOREACH grouped_heights GENERATE group AS height, master_table.name AS name, COUNT(master_table.id) AS num_heights;
filtered_table = FILTER count_table BY num_heights == 1 AND height != 'height';
result = FOREACH filtered_table GENERATE name;
DUMP result;
```

4. Which ​team​, after 1950, had the most errors in any 1 season. A season is denoted by the year.

```pig
fielding_data = LOAD 'hdfs:/user/maria_dev/pigtest/Fielding.csv' using PigStorage(',');
fielding_table = FOREACH fielding_data GENERATE $1 AS year, $2 AS team, $10 AS errors;
fielding_table = FILTER fielding_table BY year > 1950 AND errors IS NOT NULL;
ft_group = GROUP fielding_table BY (team, year);
team_sum_errors = FOREACH ft_group GENERATE group, SUM(fielding_table.errors) AS sum_errors;
team_sum_errors_group = GROUP team_sum_errors ALL;
team_max_errors = FOREACH team_sum_errors_group GENERATE MAX(team_sum_errors.sum_errors) AS max_errors;
max_errors_filtered = FILTER team_sum_errors BY sum_errors == team_max_errors.max_errors;
result = FOREACH max_errors_filtered GENERATE group.team;
DUMP result;
```

5. Output the ​playerID​ and ​team​ of the player who had the most errors with any 1 team in all seasons combined. Only consider seasons after 1950.

```pig
fielding_data = LOAD 'hdfs:/user/maria_dev/pigtest/Fielding.csv' using PigStorage(',');
fielding_table = FOREACH fielding_data GENERATE $0 AS id, $1 AS year, $2 AS team, $10 AS errors;
fielding_table = FILTER fielding_table BY year > 1950 AND errors IS NOT NULL;
ft_group = GROUP fielding_table BY (id, team);
sum_player_errors = FOREACH ft_group GENERATE group, SUM(fielding_table.errors) AS sum_errors;
ordered = ORDER sum_player_errors BY sum_errors DESC;
top_1 = LIMIT ordered 1;
result = FOREACH top_1 GENERATE group;
DUMP result;
```

6. A player who hits well and doesn’t commit a lot of errors is obviously a player you want on your team. Output the ​playerID’s​ of the top 3 players from 2005 through 2009 (including 2005 and 2009) who maximized the following criterion:

	(number of hits (H) / number of at bats (AB)) – (number of errors (E) / number of games (G))

	The above equation might be skewed by a player who only had 3 at bats but got two hits. To account for that, only consider players who had at least 40 at bats and played in at least 20 games ​over that entire 5 year span​. You should note that both files contain a “number of games” column. ​The 20 game minimum and the games values that you are using must come from the Fielding.csv file. For this problem, be sure to ignore rows in the Fielding.csv file that are in the file for ​informational p​ urposes only. An ​informational r​ ow contains no data in the 7th-17th columns (start counting at column 1). In other words, if all of the 7th, 8th, 9th, ... 16th and 17th columns are empty, the row is informational and should be ignored.

```pig
batters_data = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' using PigStorage(',');
fielding_data = LOAD 'hdfs:/user/maria_dev/pigtest/Fielding.csv' using PigStorage(',');
fielding_table = FOREACH fielding_data GENERATE $0 AS id, $1 AS year, $5 AS G, $10 AS error, $7 AS a, $8 AS b, $9 AS c, $10 AS d, $11AS e, $12AS f, $13AS g, $14AS h, $15 AS i, $16 AS j, $17 AS k;
fielding_table = FILTER fielding_table BY (year >= 2005) AND (year <= 2009) AND G >= 20 AND NOT (a IS NULL AND b IS NULL AND c IS NULL AND d IS NULL AND e IS NULL AND f IS NULL AND g IS NULL AND h IS NULL AND i IS NULL AND j IS NULL AND k IS NULL);
batting_table = FOREACH batters_data GENERATE $0 AS id, $1 AS year, $5 AS AB, $7 AS H;
batting_table = FILTER batting_table BY (year >= 2005) AND (year <= 2009) AND AB >= 40;
joined_data = JOIN fielding_table BY id, batting_table BY id;
full_table = FOREACH joined_data GENERATE fielding_table::id AS id, fielding_table::year AS year, H AS H, AB AS AB, error AS error, G AS G;
grouped_table = GROUP full_table BY id;
joined_table = FOREACH grouped_table GENERATE group AS id, full_table.year AS year, (SUM(full_table.H)/SUM(full_table.AB))-(SUM(full_table.error)/SUM(full_table.G)) AS calc;
ordered_table = ORDER joined_table BY calc DESC; 
top_3 = LIMIT ordered_table 3;
result = FOREACH top_3 GENERATE id;
DUMP result;

```

7. Sum up the number of doubles and triples for each birthCity/birthState combination. Output the ​top 5 birthCity/birthState ​combinations that produced the players who had the most doubles and triples combined (i.e., combine the doubles and triples for all players with that city/state combination). Some caveats:
* A ​birthState​ is any non-empty value in the birthState column.
* The ​birthCity m​ ust start with a vowel (i.e an A, E, I, O or U).

```pig
batters_data = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' using PigStorage(',');
master_data = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
masters_table = FOREACH master_data GENERATE $0 AS id, $5 AS birthstate, $6  AS birthcity;
masters_table = FILTER masters_table BY birthstate IS NOT NULL AND birthcity IS NOT NULL AND (STARTSWITH(birthcity, 'A') OR STARTSWITH(birthcity, 'E') OR STARTSWITH(birthcity, 'I') OR STARTSWITH(birthcity, 'O') OR STARTSWITH(birthcity, 'U'));
batters_table = FOREACH batters_data GENERATE $0 AS id, (int)$8 AS two, (int)$9 AS three;
batters_table = FILTER batters_table BY two IS NOT NULL AND three IS NOT NULL;
joined_tables = JOIN masters_table BY id, batters_table BY id;
joined_tables_exp = FOREACH joined_tables GENERATE birthcity, birthstate, two, three;
grouped_tables = GROUP joined_tables_exp BY (birthcity, birthstate);
summary = FOREACH grouped_tables GENERATE group AS city_state, SUM(joined_tables_exp.two)+SUM(joined_tables_exp.three) AS sum_hits;
order_summary = ORDER summary BY sum_hits DESC;
top_5 = LIMIT order_summary 5;
city_state_summary = FOREACH top_5 GENERATE city_state;
DUMP city_state_summary;
```

8. Output the ​birthMonth/birthState​ combination that produced the worst players.

The worst players are defined by the lowest of: (number of hits (H) / number of at bats (AB)) To ensure a small number of people who hardly played don’t skew the data, make sure that:

* at least 10 people came from the same state and were born in the same month and
* the sum of the at-bats for all of the players from the same birthMonth/birthState​ ​exceeds 1500. 

	For this problem, the year does not matter. A player born in December, 1970 in Michigan and a player born in December, 1982 in Michigan are in the same group because they were both born in December and were born in Michigan. A birthState​ is any non-empty value in the birthState column. In terms of condition a., you should count a player as one of your 10 players even if the player has no at-bats and/or no hits. You should ignore all players who do not have a birthMonth or who do not have a birthState.

```pig
batters_data = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' using PigStorage(',');
master_data = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
batters_table = FOREACH batters_data GENERATE $0 AS id, (int)$5 AS AB, (int)$7 AS H;
batters_table = FILTER batters_table BY AB IS NOT NULL AND H IS NOT NULL;
masters_table = FOREACH master_data GENERATE $0 AS id, $5 AS birthstate, $2  AS birthmonth;
masters_table = FILTER masters_table BY birthstate IS NOT NULL AND birthmonth IS NOT NULL;
joined_tables = JOIN masters_table BY id, batters_table BY id;
joined_tables_exp = FOREACH joined_tables GENERATE batters_table::id AS id, birthmonth, birthstate, AB, H AS H;
grouped_tables = GROUP joined_tables_exp BY (birthmonth, birthstate);
summary = FOREACH grouped_tables {
		AB = SUM(joined_tables_exp.AB);
		H = SUM(joined_tables_exp.H);
        id = joined_tables_exp.id;
        distinct_id = DISTINCT id;
        GENERATE group AS month_state, COUNT(distinct_id) AS count, AB AS AB, H AS H, (float)H/(float)AB AS calc;
    };
filtered = FILTER summary BY count >= 10 AND AB > 1500;
ordered = ORDER filtered BY calc ASC;
output_combo = FOREACH ordered GENERATE month_state;
top_1 = LIMIT output_combo 1;
DUMP top_1;
```