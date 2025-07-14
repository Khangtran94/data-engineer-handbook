-- TASK 1: `films`: An array of `struct`
CREATE TYPE film_stats AS(
	year INTEGER,
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT)

-- `quality_class`: This field represents an actor's performance quality, 
	
CREATE TYPE quality_check AS ENUM('star','good','average','bad')

-- **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
CREATE TABLE IF NOT EXISTS actors (
	actor TEXT,
	actorid TEXT,
	films film_stats[],
	quality_class quality_check,
	is_active BOOLEAN,
	current_year INTEGER,
	PRIMARY KEY(actorid,current_year))

-- TASK 2: Cumulative table generation query:** Write a query that populates the `actors` table one year at a time.
-- min year = 1970, max year = 2021
-- DROP TABLE actors

DO $$
DECLARE
    yr INTEGER;
BEGIN
    FOR yr IN SELECT * FROM generate_series(1971, 2021) LOOP
	-- past cte: get information from past year
        WITH past AS (
            SELECT * 
            FROM actors
            WHERE current_year = yr - 1
        ),
	-- now cte: note that using array_agg to combine those value as array
        now AS (
            SELECT actor, actorid,
                   ARRAY_AGG(ROW(year, film, votes, rating, filmid)::film_stats) AS films,
                   year
            FROM actor_films
            WHERE year = yr
            GROUP BY actor, actorid, year
        ),
	-- avg_rating: calculate score for each actor each year
        avg_rating AS (
            SELECT actor, year, AVG(rating) AS avg_score
            FROM actor_films
            WHERE year = yr
            GROUP BY actor, year
        )
        INSERT INTO actors
        SELECT 
            COALESCE(pa.actor, n.actor) AS actor,
            COALESCE(pa.actorid, n.actorid) AS actorid,
            CASE 
                WHEN pa.films IS NULL THEN n.films
                WHEN n.year IS NULL THEN pa.films 
                ELSE pa.films || n.films  
            END AS films,
            CASE 
                WHEN ar.avg_score > 8.0 THEN 'star'
                WHEN ar.avg_score > 7.0 THEN 'good'
                WHEN ar.avg_score > 6.0 THEN 'average'
                ELSE 'bad' 
            END::quality_check AS quality_class,
            (CASE WHEN n.actorid IS NOT NULL THEN TRUE ELSE FALSE END) AS is_active,
            yr AS current_year
        FROM now AS n
        FULL OUTER JOIN past AS pa ON pa.actorid = n.actorid
        LEFT JOIN avg_rating AS ar 
            ON ar.actor = COALESCE(pa.actor, n.actor)
           AND ar.year = yr;

    END LOOP;
END $$;

-- Task 3: **DDL for `actors_history_scd` table:**
-- DROP TABLE actors_history_scd

CREATE TABLE IF NOT EXISTS actors_history_scd(
	actor TEXT,
	actorid TEXT,
	is_active BOOLEAN,
	quality_class quality_check,
	start_date INTEGER,
	end_date INTEGER,
	current_year INTEGER,
	PRIMARY KEY(actorid, start_date))

-- Task 4: **Backfill query for `actors_history_scd`
	-- previous + window function to check status update or not
WITH previous AS(
	SELECT 	actor, 
			actorid, 
			current_year,
			quality_class, 
			LAG(quality_class) OVER(PARTITION BY actor ORDER BY current_year) AS previous_quality_class,
			is_active, 
			LAG(is_active) OVER(PARTITION BY actor ORDER BY current_year) AS previous_active
	FROM actors
	WHERE current_year < 2021),
	-- 2021 = max year in the data => backfill will scan all and dont care about incremental
	-- so if want to update just easy update value of current_year
indicator_identify AS(
	SELECT *,
			CASE WHEN quality_class <> previous_quality_class THEN 1
				WHEN is_active <> previous_active THEN 1
				ELSE 0 END AS change_indicator
	FROM previous),
	-- groupby to merge same rows => streak
streak_identify AS(
	SELECT *,
		SUM(change_indicator) OVER(PARTITION BY actor ORDER BY current_year) AS streak	
	FROM indicator_identify)

INSERT INTO actors_history_scd
SELECT actor,actorid, is_active, quality_class,
		MIN(current_year) AS start_date,
		MAX(current_year) AS end_date,
		2020 AS current_year
FROM streak_identify
GROUP BY actor, actorid, quality_class, is_active,streak
ORDER BY actor, streak

-- Task 5: **Incremental query for `actors_history_scd`:
SELECT * FROM actors_history_scd
CREATE TYPE scd_type AS(
	quality_class quality_check,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER
)

	-- last year: select information from last recent year (get from scd)
WITH last_year_scd AS(
	SELECT *
	FROM actors_history_scd
	WHERE current_year = 2020
		AND end_date = 2020),
		
	-- historical_scd: information before the last year (get from scd)
	historical_scd AS(
	SELECT actor, actorid, is_active, quality_class, start_date, end_date
	FROM actors_history_scd
	WHERE current_year = 2020
		AND end_date < 2020),

	-- this year: information only this year. get from data
	this_year AS(
	SELECT *
	FROM actors
	WHERE current_year = 2021),

-- because unchange => INNER JOIN 
	unchange AS(
	SELECT this.actor,
		this.actorid,
		this.is_active, 
		this.quality_class,
		ls.start_date,
		this.current_year AS end_date
	FROM this_year AS this
	INNER JOIN last_year_scd AS ls
	ON this.actor = ls.actor
	WHERE this.quality_class = ls.quality_class AND this.is_active = ls.is_active),

-- change => LEFT JOIN
	change AS(
	SELECT this.actor,
		this.actorid,
		UNNEST(ARRAY[ROW(
			ls.quality_class,
			ls.is_active,
			ls.start_date,
			ls.end_date)::scd_type,
			ROW(
			this.quality_class,
			this.is_active,
			this.current_year,
			this.current_year)::scd_type]) AS change_records
	FROM this_year AS this
	LEFT JOIN last_year_scd AS ls
	ON this.actor = ls.actor
	WHERE (this.quality_class <> ls.quality_class OR this.is_active <> ls.is_active)
		OR ls.actor IS NULL),  -- add new actor appear in this year

	unnest_change AS(
	SELECT 	actor, 
			actorid,
			(change_records::scd_type).is_active,
			(change_records::scd_type).quality_class,
			(change_records::scd_type).start_date,
			(change_records::scd_type).end_date
	FROM change),

	-- new information => when last is null
	new_records AS(
	SELECT this.actor,
		this.actorid,
		this.is_active, 
		this.quality_class,
		this.current_year AS start_date,
		this.current_year AS end_date
	FROM this_year AS this
	LEFT JOIN last_year_scd AS ls
	ON this.actor = ls.actor
	WHERE ls.actor IS NULL)
	
-- COMBINE all together from the past + unchange + change + new
SELECT *, 2021 AS current_season
FROM (SELECT * FROM historical_scd
		UNION ALL
	SELECT * FROM unchange
	UNION ALL
	SELECT * FROM unnest_change
	UNION ALL
	SELECT * FROM new_records) AS a

