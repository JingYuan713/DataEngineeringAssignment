CREATE DATABASE AirflowDB;

USE AirflowDB;

SELECT @@SERVERNAME;

SELECT * FROM commits;


-- Top 5 committers
SELECT TOP 5 committer_name, COUNT(*) AS commit_count FROM commits
GROUP BY committer_name
ORDER BY commit_count DESC;


-- Committer with longest commit streak by day
WITH commit_days AS (SELECT DISTINCT committer_name, CAST(commit_date AS DATE) AS commit_day FROM commits),
	 streaks AS (SELECT committer_name, commit_day, 
				 DATEDIFF(DAY, ROW_NUMBER() OVER (PARTITION BY committer_name ORDER BY commit_day), commit_day) AS streak_group FROM commit_days)
SELECT committer_name, COUNT(*) AS longest_streak FROM streaks
GROUP BY committer_name, streak_group
ORDER BY longest_streak DESC
OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;


-- Heatmap
SELECT day_of_week,
    ISNULL([01-03], 0) AS '01-03',
    ISNULL([04-06], 0) AS '04-06',
    ISNULL([07-09], 0) AS '07-09',
    ISNULL([10-12], 0) AS '10-12',
    ISNULL([13-15], 0) AS '13-15',
    ISNULL([16-18], 0) AS '16-18',
    ISNULL([19-21], 0) AS '19-21',
    ISNULL([22-00], 0) AS '22-00'
FROM (
    SELECT DATENAME(WEEKDAY, commit_date) AS day_of_week,
        CASE 
            WHEN DATEPART(HOUR, commit_date) BETWEEN 1 AND 3 THEN '01-03'
            WHEN DATEPART(HOUR, commit_date) BETWEEN 4 AND 6 THEN '04-06'
            WHEN DATEPART(HOUR, commit_date) BETWEEN 7 AND 9 THEN '07-09'
            WHEN DATEPART(HOUR, commit_date) BETWEEN 10 AND 12 THEN '10-12'
            WHEN DATEPART(HOUR, commit_date) BETWEEN 13 AND 15 THEN '13-15'
            WHEN DATEPART(HOUR, commit_date) BETWEEN 16 AND 18 THEN '16-18'
            WHEN DATEPART(HOUR, commit_date) BETWEEN 19 AND 21 THEN '19-21'
            ELSE '22-00'
        END AS time_block
    FROM commits) AS source_table
PIVOT (
    COUNT(time_block) FOR time_block IN ([01-03], [04-06], [07-09], [10-12], [13-15], [16-18], [19-21], [22-00])) AS pivot_table
ORDER BY 
    CASE 
        WHEN day_of_week = 'Monday' THEN 1
        WHEN day_of_week = 'Tuesday' THEN 2
        WHEN day_of_week = 'Wednesday' THEN 3
        WHEN day_of_week = 'Thursday' THEN 4
        WHEN day_of_week = 'Friday' THEN 5
        WHEN day_of_week = 'Saturday' THEN 6
        WHEN day_of_week = 'Sunday' THEN 7
    END;
