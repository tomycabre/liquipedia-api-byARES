
-- This script gets the match count for each team

SELECT
    T.team_id,
    T.team_name,
    T.game_id, -- Added game_id for context
    COALESCE(SUM(TeamMatchCounts.match_count), 0) AS total_matches_played
FROM
    Teams AS T
LEFT JOIN (
    -- Subquery to count matches for each team where they were team1
    SELECT
        MS.team1_id AS team_id,
        COUNT(MS.series_lp_matchid) AS match_count
    FROM
        MatchSeries AS MS
    GROUP BY
        MS.team1_id
    
    UNION ALL
    
    -- Subquery to count matches for each team where they were team2
    SELECT
        MS.team2_id AS team_id,
        COUNT(MS.series_lp_matchid) AS match_count
    FROM
        MatchSeries AS MS
    GROUP BY
        MS.team2_id
) AS TeamMatchCounts ON T.team_id = TeamMatchCounts.team_id
GROUP BY
    T.team_id,
    T.team_name,
    T.game_id
ORDER BY
    total_matches_played DESC,
    T.team_name ASC;