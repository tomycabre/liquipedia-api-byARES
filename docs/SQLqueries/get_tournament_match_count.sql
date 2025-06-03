
-- This script gets all of the matches played in a tournament

SELECT
    T.tournament_id,
    T.tournament_name,
    T.game_id,
    T.start_date,
    T.end_date,
    COUNT(MS.series_lp_matchid) AS number_of_matches
FROM
    Tournaments AS T
INNER JOIN
    MatchSeries AS MS ON T.tournament_id = MS.tournament_id
GROUP BY
    T.tournament_id,
    T.tournament_name,
    T.game_id,
    T.start_date,
    T.end_date
ORDER BY
    number_of_matches DESC, -- Show tournaments with most matches first
    T.start_date DESC;