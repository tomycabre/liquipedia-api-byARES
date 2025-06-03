-- This script gets all the tournaments that DO NOT have a match inside matchseries table

SELECT
    T.tournament_id,
    T.tournament_name,
    T.game_id,
    T.start_date,
    T.end_date,
    T.tier,
    T.tournament_weight
FROM
    Tournaments AS T
LEFT JOIN
    MatchSeries AS MS
    ON T.tournament_id = MS.tournament_id
WHERE
    MS.series_lp_matchid IS NULL AND end_date > '2023-12-31' -- This identifies tournaments with no corresponding match series
ORDER BY
    T.tier,
	T.start_date DESC;