--! WARNING: Ensure you understand the implications of resetting sequences,
-- especially if the tables contain data. This is typically done on empty tables
-- or after truncating tables.

-- Reset sequence for the 'playermapstats' table
ALTER SEQUENCE playermapstats_player_map_stat_id_seq RESTART WITH 1;
SELECT setval('playermapstats_player_map_stat_id_seq', 1, false); -- Ensures next val is 1

-- Reset sequence for the 'players' table
ALTER SEQUENCE players_player_id_seq RESTART WITH 1;
SELECT setval('players_player_id_seq', 1, false);

-- Reset sequence for the 'teamrosters' table
ALTER SEQUENCE teamrosters_roster_id_seq RESTART WITH 1;
SELECT setval('teamrosters_roster_id_seq', 1, false);

-- Reset sequence for the 'teams' table
ALTER SEQUENCE teams_team_id_seq RESTART WITH 1;
SELECT setval('teams_team_id_seq', 1, false);

-- Reset sequence for the 'tournamentplacements' table
ALTER SEQUENCE tournamentplacements_placement_id_seq RESTART WITH 1;
SELECT setval('tournamentplacements_placement_id_seq', 1, false);

-- Reset sequence for the 'tournaments' table
ALTER SEQUENCE tournaments_tournament_id_seq RESTART WITH 1;
SELECT setval('tournaments_tournament_id_seq', 1, false);

-- Optional: Confirm the next value for a sequence (example)
-- SELECT nextval('players_player_id_seq'); 
-- (Running nextval will consume the value, so the next insert will use the value after that)

/*
Notes:
- `ALTER SEQUENCE sequence_name RESTART WITH 1;` sets the sequence's internal counter.
- `SELECT setval('sequence_name', 1, false);` is a more robust way for some PostgreSQL versions
  to ensure the *next* value generated will be 1. If the third argument is `true` (default),
  the sequence's current value is set to 1, and the next value generated would be 2.
  Setting it to `false` means the next value generated will be the specified value (1 in this case).
- The tables `games`, `matchseries`, and `playedmaps` have TEXT primary keys in your schema,
  so they do not have associated SERIAL sequences to reset in this manner.
*/
