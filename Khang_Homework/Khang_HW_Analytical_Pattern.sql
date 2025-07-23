-- TASK 1: State Change Tracking for players
-- - A query that does state change tracking for `players`
--   - A player entering the league should be `New`
--   - A player leaving the league should be `Retired`
--   - A player staying in the league should be `Continued Playing`
--   - A player that comes out of retirement should be `Returned from Retirement`
--   - A player that stays out of the league should be `Stayed Retired`

CREATE TABLE IF NOT EXISTS players_growth_accounting (
	player_name TEXT,
	first_season INTEGER,
	last_season INTEGER,
	state_change_tracking TEXT,
	season_played INTEGER[],
	current_season INTEGER,
	PRIMARY KEY (player_name, current_season))

DO $$
DECLARE
    year INTEGER;
BEGIN
	-- Create loop from start to end, define year
    FOR year IN 1997..2021 LOOP

        INSERT INTO players_growth_accounting
        SELECT *
        FROM (
            WITH last_season AS (
                SELECT *
                FROM players_growth_accounting
                WHERE current_season = year - 1),
				
            this_season AS (
                SELECT player_name,
                       current_season
                FROM players
                WHERE current_season = year
                  AND is_active = TRUE)
				  
            SELECT
                COALESCE(ts.player_name, ls.player_name) AS player_name,
				-- get first_season 
                COALESCE(ls.first_season, ts.current_season) AS first_season,
				-- get last_season
                COALESCE(ts.current_season, ls.last_season) AS last_season,
				-- state change tracking
                CASE 
						-- if 2000 dont have that player, 2001 have that player => NEW
                    WHEN ls.player_name IS NULL AND ts.player_name IS NOT NULL THEN 'New'
						-- 2000 and 2001 both have that player => CONTINUED PLAYING
                    WHEN ls.player_name IS NOT NULL AND ts.player_name IS NOT NULL AND ls.last_season = ts.current_season - 1 THEN 'Continued Playing'
						-- 1999 play, 2000 off, 2001 return => RETURNED FROM RETIREMENT
                    WHEN ls.player_name IS NOT NULL AND ts.player_name IS NOT NULL AND ls.last_season < ts.current_season - 1 THEN 'Returned from Retirement'
						-- 2001 dont appear, 2000 appear and = current season => RETIRED
                    WHEN ls.player_name IS NOT NULL AND ts.current_season IS NULL THEN 'Retired'
						-- 1999 play last season, 2000 and 2001 off => STAYED RETIRED (else condition)
                    WHEN ls.player_name IS NOT NULL AND ts.player_name IS NULL AND ls.last_season < year - 1 THEN 'Stayed Retired'
					ELSE 'Unknown'
				-- combine array season played	
                END AS state_change_tracking,
                COALESCE(ls.season_played, ARRAY[]::INTEGER[]) ||
						-- when this season have that player => concat together
                CASE WHEN ts.player_name IS NOT NULL THEN ARRAY[ts.current_season] ELSE ARRAY[]::INTEGER[] END AS season_played,
                COALESCE(ts.current_season, ls.last_season + 1) AS current_season
            FROM this_season AS ts
            FULL OUTER JOIN last_season AS ls
            ON ts.player_name = ls.player_name
        ) AS derived
        WHERE NOT EXISTS (
            SELECT 1
            FROM players_growth_accounting pga
            WHERE pga.player_name = derived.player_name
              AND pga.current_season = derived.current_season
        );

    END LOOP;
END $$;

-- TASK 2: - A query that uses `GROUPING SETS` for `game_details` data
with combined AS(
	SELECT EXTRACT(year from g.game_date_est) AS season,
		g.game_id,
		g.home_team_id,
		home_team_wins,
		gd.team_abbreviation, 
		gd.team_city, 
		gd.player_name, 
		gd.pts,
		CASE WHEN home_team_wins = 1 THEN 'Win'
			ELSE 'Lose' END AS Status
	FROM games AS g
	INNER JOIN game_details AS gd
	ON g.game_id = gd.game_id AND gd.team_id = g.home_team_id
	WHERE gd.pts IS NOT NULL),

	calculate AS(
	SELECT
		COALESCE(season::TEXT, 'Overall') AS season,
		COALESCE(team_abbreviation, 'Overall') AS team_abbreviation, 
		COALESCE(team_city, 'Overall') AS team_city,
		COALESCE(player_name, 'Overall') AS player_name,
		SUM(pts) AS total_points,
		COUNT(DISTINCT game_id) AS total_games
	FROM combined
	GROUP BY GROUPING SETS(
		(player_name, team_abbreviation, team_city),
		(player_name, season),
		(team_abbreviation, team_city))
	ORDER BY total_points DESC),

	team_wins AS (
	SELECT DISTINCT	game_id, 
			team_abbreviation, 
			team_city, 
			status
	FROM combined
	WHERE status = 'Win')

-- who scored the most points playing for one team?
SELECT player_name,
		team_city,
		team_abbreviation, 
		total_points
FROM calculate
WHERE season = 'Overall' and player_name != 'Overall'
ORDER BY total_points DESC

-- who scored the most points in one season?
SELECT 	season,
		player_name, 
		total_points
FROM calculate
WHERE season != 'Overall'
		AND player_name != 'Overall'
ORDER BY total_points DESC

-- which team has won the most games?
SELECT  team_city,
		team_abbreviation,
		COUNT(status) AS total_game_wins
FROM team_wins
GROUP BY 1,2
ORDER BY 3 DESC

-- TASK 3: A query that uses window functions on `game_details`
with combined AS(
	SELECT g.game_date_est AS game_day,
		g.game_id,
		g.home_team_id,
		home_team_wins,
		gd.team_abbreviation, 
		gd.team_city, 
		gd.player_name, 
		gd.pts,
		CASE WHEN home_team_wins = 1 THEN 'Win'
			ELSE 'Lose' END AS Status
	FROM games AS g
	INNER JOIN game_details AS gd
	ON g.game_id = gd.game_id AND gd.team_id = g.home_team_id
	WHERE gd.pts IS NOT NULL),

	-- deduplicate the data
	team_stats AS (
	SELECT DISTINCT game_day, team_city, team_abbreviation, status
	FROM combined
	ORDER BY game_day, team_city),

	-- row number all the games of each team
	all_games AS(
	SELECT *, 
			ROW_NUMBER() OVER(PARTITION BY team_city ORDER BY game_day) AS game_number
	FROM team_stats),

	-- window function for 90 recently games
	window_90_games AS
	(SELECT  team_city,
  			 game_day,
  			 game_number,
  			 status,
			  -- Count wins in the 90-game window (including current)
			  COUNT(*) FILTER (WHERE status = 'Win') 
			  OVER (PARTITION BY team_city ORDER BY game_number 
			          ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) AS wins_last_90_games
	FROM all_games
	ORDER BY team_city, game_number),

	-- filter only lebron james stats and points > 10
	lebron AS(
	SELECT  game_day, 
			game_id, 
			pts,
			CASE WHEN pts > 10 then 1 ELSE 0 END AS streak_indicator
	FROM combined
	WHERE player_name LIKE 'LeBron%'
	ORDER BY 1),

	-- row number games of lebron, also row number for each type of streak indicator
	numbered AS (
	  SELECT *,
		ROW_NUMBER() OVER (ORDER BY game_day) AS rn_all,
		ROW_NUMBER() OVER (PARTITION BY streak_indicator ORDER BY game_day) AS rn_streak
	  FROM lebron),

	-- calculate the streaks
	streaks AS (
	  	SELECT *,
	    -- Unique streak group: consecutive rows where streak_indicator = 1 will share the same value
	    (rn_all - rn_streak) AS streak_group
	  FROM numbered)

-- What is the most games a team has won in a 90 game stretch? 
SELECT team_city,
		MAX(wins_last_90_games) AS most_game_win_in_90
FROM window_90_games
GROUP BY team_city
ORDER BY 2 DESC

-- How many games in a row did LeBron James score over 10 points a game?
SELECT 
  COUNT(*) AS lebron_streak_10_pts,
  MIN(game_day) AS start_date,
  MAX(game_day) AS end_date
FROM streaks
WHERE streak_indicator = 1
GROUP BY streak_group
