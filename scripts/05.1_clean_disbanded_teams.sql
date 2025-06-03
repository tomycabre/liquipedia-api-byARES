
-- ! READ THIS BEFORE PROCEEDING !
-- ? WHY DOES THIS SCRIPT EXIST?
/*
05_fetch_team_rosters fetches all of the information inside Squad Players v3 and then filters out the data to insert
only playters with leave_date::NULL and status::active, because of that it adds new teams even though they are not 
active anymore. So this SQL script gets all teams that do not have players inside squadrosters and eliminates them.
*/ 

-- ? PROBLEM WITH SOME TEAMS:
/*
The problem is that there seems to be active rosters for teams that when fetched in 02_fetch_teams
DO NOT have status::active (the teams with this error are 50 *As of May 2025* (42 Valorant, 8 CS2)). So even though they appear to have
active rosters they are not active in Teams V3.
*/ 



-- STEP 1: IDENTIFY TEAMS TO BE DELETED (RUN THIS FIRST TO VERIFY)

SELECT
    t.team_id,
    t.team_name,
    t.game_id,
    t.is_disbanded -- And other columns from Teams you might want to see
FROM
    Teams t
WHERE
    NOT EXISTS (
        SELECT 1
        FROM TeamRosters tr
        WHERE tr.team_id = t.team_id
          AND tr.status = 'active' -- Ensure 'active' is the correct value for the status in TeamRosters
    );


-- STEP 2: DELETE THE IDENTIFIED TEAMS (WITH CAUTION)
-- Once you are sure about the results from the SELECT query, you can execute the DELETE statement.
-- IT IS STRONGLY RECOMMENDED TO MAKE A BACKUP BEFORE RUNNING THIS!

DELETE FROM Teams
WHERE
    team_id IN (
        SELECT
            t.team_id
        FROM
            Teams t
        WHERE
            NOT EXISTS (
                SELECT 1
                FROM TeamRosters tr
                WHERE tr.team_id = t.team_id
                  AND tr.status = 'active' -- Ensure 'active' is the correct value for the status in TeamRosters
            )
    );

/*
Explanation of the DELETE statement:
- DELETE FROM Teams: Indicates that rows will be deleted from the Teams table.
- WHERE team_id IN (...): Specifies that only teams whose team_id are in the result of the subquery will be deleted.
- The subquery returns the team_ids of teams that do not have any active players in TeamRosters.

Important Considerations:
1. Value of TeamRosters.status: It's crucial that 'active' is the exact value (case-sensitive) you are using.
2. Foreign Keys and ON DELETE:
   - MatchSeries: Has FKs to Teams.team_id with ON DELETE RESTRICT (for team1_id, team2_id) and ON DELETE SET NULL (for series_winner_team_id).
     If a team to be deleted is referenced in MatchSeries.team1_id or MatchSeries.team2_id, the DELETE will fail. You must delete those MatchSeries first or update the team references.
   - TournamentPlacements: Has an FK to Teams.team_id with ON DELETE CASCADE. Placements for deleted teams will be automatically removed.
   - PlayerMapStats: Has an FK to Teams.team_id with ON DELETE CASCADE. Player map stats for deleted teams will be automatically removed.
3. Teams.is_disbanded: This query focuses on whether the team has active players in TeamRosters, not directly on the Teams.is_disbanded column.
*/
