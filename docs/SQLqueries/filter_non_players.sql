-- !! THIS IS UPDATED AS PER MAY 21st 2025 ANY CHANGES FROM HERE IN ROLE NAMES ARE NOT ACCOUNTED FOR !!
-- For example: if they add 'op' (operator) for valorant snipers this will delete those too.


-- Ensure you understand what this query does before running it.
-- It's recommended to first run the SELECT part of this query
-- to see which players WOULD BE DELETED.
-- You can check if new player roles were added here
SELECT DISTINCT curr_role
FROM Players
WHERE curr_role IS NOT NULL
  AND TRIM(curr_role) != '' -- Keep players with empty string roles
  AND LOWER(TRIM(curr_role)) NOT IN (
    'igl', 
    'in-game leader', 
    'rifler', 
    'awp', 
    'lurker', 
    'coach', 
    'entry fragger', 
    'support'
  );

-- Once you are sure, you can run the DELETE statement (In your PSQL DATABASE):
DELETE FROM Players
WHERE curr_role IS NOT NULL                      -- Only consider rows where curr_role has a value
  AND TRIM(curr_role) != ''                      -- Do NOT delete if the role is an empty string
  AND LOWER(TRIM(curr_role)) NOT IN (            -- Convert to lowercase and trim for case-insensitive comparison
    'igl', 
    'in-game leader', 
    'rifler', 
    'awp', 
    'lurker', 
    'coach', 
    'entry fragger', 
    'support'
  );