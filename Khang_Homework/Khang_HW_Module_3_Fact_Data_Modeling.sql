-- TASK 1: A query to deduplicate `game_details` from Day 1 so there's no duplicates
-- Table to store information after deduplicate
CREATE TABLE IF NOT EXISTS FCT_GAME_DETAILS (
	DIM_GAME_DATE DATE,
	DIM_SEASON INTEGER,
	DIM_TEAM_ID INTEGER,
	DIM_PLAY_AT_HOME BOOLEAN,
	DIM_PLAYER_ID INTEGER,
	DIM_PLAYER_NAME TEXT,
	DIM_START_POSITION TEXT,
	DIM_DID_NOT_PLAY BOOLEAN,
	DIM_DID_NOT_DRESS BOOLEAN,
	DIM_NOT_WITH_TEAM BOOLEAN,
	M_MINUTES REAL,
	M_FGM INTEGER,
	M_FGA INTEGER,
	M_FG3M INTEGER,
	M_FG3A INTEGER,
	M_FTM INTEGER,
	M_FTA INTEGER,
	M_OREB INTEGER,
	M_DREB INTEGER,
	M_AST INTEGER,
	M_STL INTEGER,
	M_BLK INTEGER,
	M_TURNOVERS INTEGER,
	M_PF INTEGER,
	M_PTS INTEGER,
	M_PLUS_MINUS INTEGER,
	PRIMARY KEY (DIM_GAME_DATE, DIM_TEAM_ID, DIM_PLAYER_ID)
)

INSERT INTO
	FCT_GAME_DETAILS

-- cte dedup: combine game vs game_details to get check information
	
WITH
	DEDUP AS (
		SELECT
			GA.GAME_DATE_EST,
			GA.SEASON,
			GA.HOME_TEAM_ID,
			GD.TEAM_ID AS TEAM_ID_GD,
			GD.*,
			ROW_NUMBER() OVER (
				PARTITION BY
					GA.GAME_ID,
					TEAM_ID,
					PLAYER_ID
				ORDER BY
					GA.GAME_DATE_EST
			) AS ROW_NUM
			-- order by so game_date start from date 1
			-- calculate row num to check duplicate rows
		FROM
			GAME_DETAILS AS GD
			INNER JOIN GAMES AS GA ON GD.GAME_ID = GA.GAME_ID
	)
SELECT
	GAME_DATE_EST AS DIM_GAME_DATE,
	SEASON AS DIM_SEASON,
	TEAM_ID AS DIM_TEAM_ID,
	TEAM_ID = HOME_TEAM_ID AS DIM_PLAY_AT_HOME,
	PLAYER_ID AS DIM_PLAYER_ID,
	PLAYER_NAME AS DIM_PLAYER_NAME,
	START_POSITION AS DIM_START_POSITION,
	COALESCE(POSITION('DNP' IN COMMENT), 0) > 0 AS DIM_DID_NOT_PLAY,
	COALESCE(POSITION('DND' IN COMMENT), 0) > 0 AS DIM_DID_NOT_DRESS,
	COALESCE(POSITION('NWT' IN COMMENT), 0) > 0 AS DIM_NOT_WITH_TEAM,
	CAST(SPLIT_PART(MIN, ':', 1) AS REAL) + CAST(SPLIT_PART(MIN, ':', 2) AS REAL) / 60 AS M_MINUTES,
	FGM AS M_FGM,
	FGA AS M_FGA,
	FG3M AS M_FG3M,
	FG3A AS M_FG3A,
	FTM AS M_FTM,
	FTA AS M_FTA,
	OREB AS M_OREB,
	DREB AS M_DREB,
	AST AS M_AST,
	STL AS M_STL,
	BLK AS M_BLK,
	"TO" AS M_TURNOVERS,
	PF AS M_PF,
	PTS AS M_PTS,
	PLUS_MINUS AS M_PLUS_MINUS
FROM
	DEDUP
	--  filter row_num = 1 => assure no duplicate
WHERE
	ROW_NUM = 1;

-- TASK 2: A DDL for an `user_devices_cumulated` table
CREATE TABLE IF NOT EXISTS user_devices_cumulated(
	user_id TEXT,
	browser_type TEXT,
	device_activity_datelist DATE[],
	date_check DATE, 
	PRIMARY KEY(user_id, browser_type, date_check))

-- DROP TABLE user_devices_cumulated
-- TASK 3: A cumulative query to generate device_activity_datelist from events
with check_events AS(
	SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY url, referrer, user_id, device_id, host, event_time
               ORDER BY event_time) AS rn
	FROM events),
	-- dedup table events
	dedup_event AS(SELECT * FROM check_events WHERE rn = 1),

	check_device AS(
	SELECT *,
		ROW_NUMBER() OVER(
		PARTITION BY device_id, browser_type, browser_version_major, browser_version_minor, browser_version_patch,
		device_type, device_version_major, device_version_minor, device_version_patch, 
		os_type, os_version_major, os_version_minor, os_version_patch ORDER BY device_id) AS rn
		FROM devices),
	-- dedup table devices
	dedup_device AS(SELECT * FROM check_device WHERE rn = 1),

	-- change data type when joining together
	full_fact AS(
	SELECT DISTINCT ON(user_id, browser_type, DATE(event_time))
			user_id::TEXT, browser_type, DATE(event_time) AS event_date
	FROM dedup_device AS dd
	INNER JOIN dedup_event AS de ON dd.device_id = de.device_id
	WHERE user_id IS NOT NULL
	ORDER BY user_id),

	-- cte yesterday from user devices cumulated table
	yesterday AS(
	SELECT * FROM user_devices_cumulated
	WHERE date_check = DATE('2022-12-31')),

	-- cte today from full fact
	today AS(
	SELECT * FROM full_fact
	WHERE event_date = '2023-01-01')

INSERT INTO user_devices_cumulated
SELECT COALESCE(td.user_id, y.user_id) AS user_id,
		COALESCE(td.browser_type, y.browser_type) AS browser_type,
		-- when yesterday device activity datelist is NULL => collect today
	CASE WHEN y.device_activity_datelist IS NULL THEN ARRAY[td.event_date]
	-- when today active = NULL => collect old list 
		WHEN td.event_date IS NULL THEN y.device_activity_datelist
	-- not null => combine together
		ELSE ARRAY[td.event_date] || y.device_activity_datelist
		END AS device_activity_datelist,
	COALESCE(td.event_date, y.date_check + INTERVAL '1 DAY')::DATE AS date_check
FROM today AS td
FULL OUTER JOIN yesterday AS y
ON td.user_id::TEXT = y.user_id
-- on conflict to avoid pk key constrain because skip days
ON CONFLICT (user_id,browser_type,date_check) DO NOTHING 

-- TASK 4: A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
WITH user_log AS(
	SELECT * 
	FROM user_devices_cumulated 
	WHERE date_check = '2023-01-31'),

	series AS(
	SELECT * 
	FROM generate_series(DATE('2023-01-01'),DATE('2023-01-31'), INTERVAL '1 day') AS day_series),

-- Does the array device_activity_datelist contain the date day_series?
--  2 power (32 - different day)
	place_holder_cte AS(
	SELECT 
	-- change to bigint then cast again to bit32
		CASE WHEN device_activity_datelist @> ARRAY[DATE(day_series)] 
			THEN CAST(POW(2, 32 - (date_check - DATE(day_series))) AS BIGINT)
			ELSE 0 END AS placeholder_int_value, 
		*
	FROM user_log
	CROSS JOIN series)

SELECT 	user_id, 
		CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS day_actives
		BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) AS day_count,
		-- if day count > 0 => active in this month
		BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_monthly_active,
		-- first 7 days = 1 => check active in this week 
		BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_weekly_active,
		-- check daily active
		BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_daily_active
FROM place_holder_cte
GROUP BY user_id

-- TASK 5:A DDL for hosts_cumulated table
CREATE TABLE IF NOT EXISTS hosts_cumulated(
	host TEXT,
	host_activity_datelist DATE[],
	date_current DATE,
	PRIMARY KEY(host, date_current))

-- TASK 6: The incremental query to generate host_activity_datelist
with host_fact AS(
	SELECT DISTINCT ON(host, DATE(event_time))
	host, DATE(event_time) AS event_date
	FROM events),

	yesterday_host AS(
	SELECT * FROM hosts_cumulated
	WHERE date_current = DATE('2023-01-30')),

	today_host AS(
	SELECT * FROM host_fact
	WHERE event_date = '2023-01-31')

INSERT INTO hosts_cumulated
SELECT COALESCE(td.host, ye.host) AS host,
		-- when yesterday is NULL => collect today
		CASE WHEN ye.host_activity_datelist IS NULL THEN ARRAY[td.event_date]
		-- when today IS NULL => old record
			WHEN td.event_date IS NULL THEN ye.host_activity_datelist
		-- both not null => combine together
			ELSE ARRAY[td.event_date] || ye.host_activity_datelist
			END AS host_activity_datelist,
		COALESCE(td.event_date, ye.date_current + INTERVAL '1 DAY')::DATE AS date_current
FROM today_host AS td
FULL OUTER JOIN yesterday_host AS ye
ON td.host = ye.host

-- TASK 7: A monthly, reduced fact table DDL host_activity_reduced
CREATE TABLE host_activity_reduced(
	month DATE,
	host TEXT,
	hit_array REAL[],
	unique_visitors_array REAL[],
	PRIMARY KEY(month, host))

-- DROP TABLE host_activity_reduced
-- TASK 8: An incremental query that loads host_activity_reduced (day by day)
-- create array_metrics first
CREATE TABLE array_metrics(
	host TEXT,
	month_start DATE,
	site_hit_metric TEXT,
	hit_array REAL[],
	visitor_metric TEXT,
	unique_visitors_array REAL[],
	PRIMARY KEY(host, month_start, site_hit_metric, visitor_metric))

-- DROP TABLE array_metrics
-- cte daily AGGREGATE
with daily_aggregate AS(
	SELECT host,
			COUNT(1) AS dim_site_hits,
			COUNT(DISTINCT user_id) AS dim_unique_visitors,
			DATE(event_time) AS event_date
	FROM events
	WHERE DATE(event_time) = '2023-01-31' AND user_id IS NOT NULL
	GROUP BY host, DATE(event_time)),

-- cte yesterday array
	yesterday_array AS(
	SELECT *
	FROM array_metrics
	WHERE month_start = DATE('2023-01-01'))

INSERT INTO array_metrics
SELECT  COALESCE(da.host, ya.host) AS host,
		COALESCE(ya.month_start, DATE_TRUNC('month',da.event_date)::DATE) AS month_start,
		'site_hits' AS site_hit_metric,
	-- Update hit_array based on existing data and new daily aggregates
	-- when yesterday is OK => concat with daily aggregate. also set COALESCE in case today not active
		CASE WHEN ya.hit_array IS NOT NULL
			THEN ya.hit_array || ARRAY[COALESCE(da.dim_site_hits,0)]
	-- when yesterday is NULL => array fill 0 with specific day location also concate with today check coalesce too
			WHEN ya.hit_array IS NULL
			THEN ARRAY_FILL(0, ARRAY[COALESCE(event_date - DATE(DATE_TRUNC('month',event_date)),0)])
			|| ARRAY[COALESCE(da.dim_site_hits,0)] END AS hit_array,
		'unique_visitors' AS visitor_metric,
	-- same as hit_array
		CASE WHEN ya.unique_visitors_array IS NOT NULL
			THEN ya.unique_visitors_array || ARRAY[COALESCE(da.dim_unique_visitors,0)]
	-- when yesterday is NULL => array fill 0 with specific day location also concate with today check coalesce too
			WHEN ya.unique_visitors_array IS NULL
			THEN ARRAY_FILL(0, ARRAY[COALESCE(event_date - DATE(DATE_TRUNC('month',event_date)),0)])
			|| ARRAY[COALESCE(da.dim_unique_visitors,0)] END AS unique_visitors_array
FROM daily_aggregate AS da
FULL OUTER JOIN yesterday_array AS ya
ON da.host = ya.host
ON CONFLICT(host, month_start, site_hit_metric, visitor_metric)
DO UPDATE 
SET hit_array = EXCLUDED.hit_array, unique_visitors_array = EXCLUDED.unique_visitors_array

-- after run all insert into host_activity_reduced
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors_array)
SELECT month_start, host, hit_array, unique_visitors_array
FROM array_metrics;
