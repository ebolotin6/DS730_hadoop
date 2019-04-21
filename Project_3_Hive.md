# Project 3: Apache Hive

1. Output the ​birth city​ of the player who had the most runs batted in (RBI) in his career.

```sql
DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(id STRING, year INT,
team STRING, league STRING, games INT, ab INT, runs INT, hits
INT, doubles INT, triples INT, homeruns INT, rbi INT, sb INT, cs
INT, walks INT, strikeouts INT, ibb INT, hbp INT, sh INT, sf
INT, gidp INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/hivetest/batting';
DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE IF NOT EXISTS master(id STRING, byear INT,
bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity
STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate
STRING, dcity STRING, fname STRING, lname STRING, name STRING,
weight INT, height INT, bats STRING, throws STRING, debut
STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT
DELIMITED FIELDS TERMINATED BY ',' LOCATION
'/user/maria_dev/hivetest/master';

SELECT
    bcity
FROM
    (
    SELECT
        bcity as bcity,
        ab as ab
    FROM
        (
        SELECT
            m.id as id,
            m.bcity as bcity,
            SUM(b.ab) as ab
        FROM
            master m
        JOIN
            batting b
        ON
            m.id = b.id
        GROUP BY
            m.id,
            m.bcity
        ) sub
    ORDER BY
        ab DESC
    ) sub2
LIMIT 1;
```

2. Output the ​top three birthdates​ that had the most players born. I am only looking for day and month combinations. For instance, how many were born on February 3r​ d​, how many were born on March 8t​ h​, how many were born on July 20t​h​... print out the top three ​mm/dd​ combinations. You must output the information in mm/dd form (it is ok to print out 5 instead of 05).

```sql
DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE IF NOT EXISTS master(id STRING, byear INT,
bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity
STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate
STRING, dcity STRING, fname STRING, lname STRING, name STRING,
weight INT, height INT, bats STRING, throws STRING, debut
STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT
DELIMITED FIELDS TERMINATED BY ',' LOCATION
'/user/maria_dev/hivetest/master';

SELECT
    month_day
FROM
    (
    SELECT
        month_day,
        count_bdays
    FROM
        (
        SELECT
            concat_ws('/', CAST(bmonth as STRING), CAST(bday AS STRING)) as month_day,
            count(id) as count_bdays
        FROM
            master
        WHERE
            bday IS NOT NULL AND bmonth IS NOT NULL
        GROUP BY
            concat_ws('/', CAST(bmonth as STRING), CAST(bday AS STRING))
        ) sub
    ORDER BY
        count_bdays DESC
    ) sub2
LIMIT 3
```

3. Output the ​second most common weight​.

```sql
DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE IF NOT EXISTS master(id STRING, byear INT,
bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity
STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate
STRING, dcity STRING, fname STRING, lname STRING, name STRING,
weight INT, height INT, bats STRING, throws STRING, debut
STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT
DELIMITED FIELDS TERMINATED BY ',' LOCATION
'/user/maria_dev/hivetest/master';

SELECT
    weight
FROM
    (
    SELECT
        weight,
        count_weight,
        row_number() over(order by count_weight DESC) as row_num
    FROM
        (
        SELECT
            weight,
            count(weight) count_weight
        FROM
            master
        WHERE
            weight IS NOT NULL
        GROUP BY
            weight
        ) sub
    ORDER BY
        count_weight DESC
    ) sub2
WHERE
    row_num = 2ACH filtered_table GENERATE name;
DUMP result;
```

4. Output the​ team t​hat had the most errors in 2001

```sql
DROP TABLE IF EXISTS fielding;
CREATE EXTERNAL TABLE IF NOT EXISTS fielding(id STRING, year INT,
team STRING, igid STRING, position STRING, G INT, GS INT, InnOuts
INT, PO INT, A INT, errors INT, DP INT, PB INT, WB
INT, SB INT, CS INT, ZR INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/hivetest/fielding';

SELECT
    team
FROM
    (
    SELECT
        team,
        count_errors,
        rank() over(order by count_errors desc) as ranking
    FROM
        (
        SELECT
            team,
            SUM(errors) count_errors
        FROM
            fielding
        WHERE
            year = 2001
        GROUP BY
            team
        ) sub
    ORDER BY
        count_errors DESC
    ) sub2
WHERE
    ranking = 1;
```

5. Output the ​playerID​ of the player who had the most errors in all seasons
combined.

```sql
DROP TABLE IF EXISTS fielding;
CREATE EXTERNAL TABLE IF NOT EXISTS fielding(id STRING, year INT,
team STRING, igid STRING, position STRING, G INT, GS INT, InnOuts
INT, PO INT, A INT, errors INT, DP INT, PB INT, WB
INT, SB INT, CS INT, ZR INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/hivetest/fielding';

SELECT
    id
FROM
    (
    SELECT
        id,
        sum_errors
    FROM
        (
        SELECT
            id,
            SUM(errors) as sum_errors
        FROM
            fielding
        GROUP BY
            id
        ) sub
    ORDER BY
        sum_errors DESC
    ) sub2
LIMIT 1;
```

6. A player who hits well and doesn’t commit a lot of errors is obviously a player you want on your team. Output the ​playerID’s​ of the top 3 players from 2005 through 2009 (including 2005 and 2009) who maximized the following criterion:

	(number of hits (H) / number of at bats (AB)) – (number of errors (E) / number of games (G))

	The above equation might be skewed by a player who only had 3 at bats but got two hits. To account for that, only consider players who had at least 40 at bats and played in at least 20 games ​over that entire 5 year span​. You should note that both files contain a “number of games” column. ​The 20 game minimum and the games values that you are using must come from the Fielding.csv file. For this problem, be sure to ignore rows in the Fielding.csv file that are in the file for ​informational p​ urposes only. An ​informational r​ ow contains no data in the 7th-17th columns (start counting at column 1). In other words, if all of the 7th, 8th, 9th, ... 16th and 17th columns are empty, the row is informational and should be ignored.

```sql
DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(id STRING, year INT,
team STRING, league STRING, games INT, ab INT, runs INT, hits
INT, doubles INT, triples INT, homeruns INT, rbi INT, sb INT, cs
INT, walks INT, strikeouts INT, ibb INT, hbp INT, sh INT, sf
INT, gidp INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/hivetest/batting';
DROP TABLE IF EXISTS fielding;
CREATE EXTERNAL TABLE IF NOT EXISTS fielding(id STRING, year INT,
team STRING, igid STRING, position STRING, G INT, GS INT, InnOuts
INT, PO INT, A INT, errors INT, DP INT, PB INT, WB
INT, SB INT, CS INT, ZR INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/hivetest/fielding';


SELECT
    id
FROM
    (
    SELECT
        id,
        calc,
        rank() over(order by calc desc) as ranking
    FROM
        (
        SELECT
            f.id,
            round(((SUM(hits)/SUM(ab))-(SUM(errors)/SUM(G))),3) as calc
        FROM
            fielding f
        JOIN
            batting b
        ON
            f.id = b.id
        WHERE
            f.year >= 2005 AND 
            f.year <= 2009 AND
            b.year >= 2005 AND 
            b.year <= 2009 AND
            NOT (f.GS IS NULL AND
            f.InnOuts IS NULL AND
            f.PO IS NULL AND
            f.A IS NULL AND
            f.errors IS NULL AND
            f.DP IS NULL AND
            f.PB IS NULL AND
            f.WB IS NULL AND
            f.SB IS NULL AND
            f.CS IS NULL AND
            f.ZR IS NULL)
        GROUP BY
            f.id
        HAVING
            SUM(ab) >= 40 AND
            SUM(G) >= 20
            ) sub
    ORDER BY
        calc DESC
    ) sub2
WHERE
    ranking <= 3;
```

7. Sum up the number of doubles and triples for each birthCity/birthState combination. Output the ​top 5 birthCity/birthState ​combinations that produced the players who had the most doubles and triples (i.e. combine the doubles and triples for all players with that city/state combination). A ​birthState​ is any non-empty value in the birthState column.

```sql
DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(id STRING, year INT,
team STRING, league STRING, games INT, ab INT, runs INT, hits
INT, doubles INT, triples INT, homeruns INT, rbi INT, sb INT, cs
INT, walks INT, strikeouts INT, ibb INT, hbp INT, sh INT, sf
INT, gidp INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/hivetest/batting';
DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE IF NOT EXISTS master(id STRING, byear INT,
bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity
STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate
STRING, dcity STRING, fname STRING, lname STRING, name STRING,
weight INT, height INT, bats STRING, throws STRING, debut
STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT
DELIMITED FIELDS TERMINATED BY ',' LOCATION
'/user/maria_dev/hivetest/master';

SELECT
    city_state
FROM
    (
    SELECT
        city_state,
        calc
    FROM
        (
        SELECT
            concat_ws('/', bcity, bstate) AS city_state,
            (SUM(b.doubles) + SUM(b.triples)) as calc
        FROM
            master m
        JOIN
            batting b
        ON
            m.id = b.id
        WHERE
            bstate IS NOT NULL AND 
            bcity is NOT NULL
        GROUP BY
            concat_ws('/', bcity, bstate)
        ) sub
    ORDER BY
        calc DESC
        ) sub2
LIMIT 5;
```

8. Output the ​birthMonth/birthState​ combination that produced the worst players. The worst players are defined by the lowest of:

    (number of hits (H) / number of at bats (AB))

To ensure 1 player who barely played does not skew the data, make sure that:

* at least 5 people came from the same state and were born in the same
month and
* the sum of the at-bats for all of the players from the same month/state
exceeds 100.

    For this problem, the year does not matter. A player born in December, 1970 in Michigan and a player born in December, 1982 in Michigan are in the same group because they were both born in December and were born in Michigan. A birthState​ is any non-empty value in the birthState column. In terms of condition a., you should count a player as one of your 5 players even if the player has no at-bats and/or no hits. You should ignore all players who do not have a birthMonth or who do not have a birthState.

```sql
DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(id STRING, year INT,
team STRING, league STRING, games INT, ab INT, runs INT, hits
INT, doubles INT, triples INT, homeruns INT, rbi INT, sb INT, cs
INT, walks INT, strikeouts INT, ibb INT, hbp INT, sh INT, sf
INT, gidp INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/hivetest/batting';
DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE IF NOT EXISTS master(id STRING, byear INT,
bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity
STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate
STRING, dcity STRING, fname STRING, lname STRING, name STRING,
weight INT, height INT, bats STRING, throws STRING, debut
STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT
DELIMITED FIELDS TERMINATED BY ',' LOCATION
'/user/maria_dev/hivetest/master';

SELECT
    month_state
FROM
    (
    SELECT
        month_state,
        calc,
        rank() over(order by calc ASC)
    FROM
        (
        SELECT
            concat_ws('/', CAST(bmonth AS STRING), bstate) AS month_state,
            COUNT(DISTINCT b.id) AS count_id,
            SUM(b.ab) AS count_ab,
            (SUM(b.hits)/SUM(b.ab)) as calc
        FROM
            master m
        JOIN
            batting b
        ON
            m.id = b.id
        WHERE
            bstate IS NOT NULL AND 
            bmonth is NOT NULL
        GROUP BY
            concat_ws('/', CAST(bmonth AS STRING), bstate)
        ) sub
    WHERE
        count_ab > 100 AND 
        count_id >= 5
    ORDER BY
        calc ASC
    ) sub2
LIMIT 1;
```