# scripts/lib/db_utils.py
import psycopg2
import psycopg2.extras # For extras like execute_values
import logging
import re # For regex stripping of stage names
from datetime import date as datetime_date 
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, LOG_LEVEL

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logger.debug(f"Successfully connected to database '{DB_NAME}' on {DB_HOST}.")
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL database '{DB_NAME}': {e}")
        raise 
    return conn

def get_or_create_game(cursor, game_id, game_name):
    try:
        cursor.execute(
            """
            INSERT INTO Games (game_id, game_name)
            VALUES (%s, %s)
            ON CONFLICT (game_id) DO NOTHING;
            """,
            (game_id, game_name)
        )
        logger.debug(f"Ensured game exists: ID='{game_id}', Name='{game_name}'")
        return game_id 
    except psycopg2.Error as e:
        logger.error(f"Database error in get_or_create_game for '{game_name}': {e}")
        raise

def get_or_create_team(cursor, team_name, game_id, region=None, location=None, is_disbanded=None, api_pagename=None):
    if not game_id: 
        logger.warning("Game_id is missing. Cannot get or create team.")
        return None
    team_name_to_use = team_name
    if not team_name_to_use and api_pagename:
        team_name_to_use = api_pagename
    if not team_name_to_use: 
        logger.warning(f"Team identifier is missing for game_id {game_id}. Cannot proceed.")
        return None
    try:
        cursor.execute(
            "SELECT team_id FROM Teams WHERE team_name = %s AND game_id = %s;",
            (team_name_to_use, game_id)
        )
        result = cursor.fetchone()
        if result:
            team_id = result[0]
            logger.debug(f"Found existing team: '{team_name_to_use}' (Game: {game_id}), ID: {team_id}")
            update_fields = []
            update_values = []
            if region is not None: update_fields.append("region = %s"); update_values.append(region)
            if location is not None: update_fields.append("location = %s"); update_values.append(location)
            if is_disbanded is not None: update_fields.append("is_disbanded = %s"); update_values.append(is_disbanded)
            if update_fields: 
                update_query = f"UPDATE Teams SET {', '.join(update_fields)} WHERE team_id = %s"
                cursor.execute(update_query, (*update_values, team_id))
                logger.info(f"Updated existing team '{team_name_to_use}' (ID: {team_id}).")
            return team_id
        else:
            effective_is_disbanded = False if is_disbanded is None else is_disbanded
            cursor.execute(
                """
                INSERT INTO Teams (team_name, game_id, region, location, is_disbanded)
                VALUES (%s, %s, %s, %s, %s) RETURNING team_id;
                """,
                (team_name_to_use, game_id, region, location, effective_is_disbanded)
            )
            new_team_id = cursor.fetchone()[0]
            logger.info(f"Created new team: '{team_name_to_use}' (Game: {game_id}), ID: {new_team_id}.")
            return new_team_id
    except psycopg2.Error as e:
        logger.error(f"Database error in get_or_create_team for '{team_name_to_use}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_or_create_team for '{team_name_to_use}': {e}")
        raise

def get_team_id_by_name(cursor, team_name, game_id):
    if not team_name or not game_id:
        logger.debug(f"get_team_id_by_name: Missing team_name or game_id. Name: {team_name}, GameID: {game_id}")
        return None
    try:
        cursor.execute(
            "SELECT team_id FROM Teams WHERE team_name = %s AND game_id = %s;",
            (team_name, game_id)
        )
        result = cursor.fetchone()
        if result:
            logger.debug(f"Found team for match processing: '{team_name}' (Game: {game_id}), ID: {result[0]}")
            return result[0]
        else:
            logger.debug(f"Team not found in DB for match processing: '{team_name}' (Game: {game_id})")
            return None
    except psycopg2.Error as e:
        logger.error(f"Database error in get_team_id_by_name for '{team_name}': {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in get_team_id_by_name for '{team_name}': {e}")
        return None

def get_or_create_player(cursor, player_nickname, game_id, birth_date=None, nationality=None, status=None, current_role=None, type=None, api_pagename=None):
    if not game_id:
        logger.warning("Game_id is missing. Cannot get or create player.")
        return None
    nickname_to_use = player_nickname
    if not nickname_to_use and api_pagename: 
        nickname_to_use = api_pagename
    if not nickname_to_use: 
        logger.warning(f"Player identifier is missing for game_id {game_id}. Cannot proceed.")
        return None
    try:
        cursor.execute(
            "SELECT player_id FROM Players WHERE player_nickname = %s AND game_id = %s;",
            (nickname_to_use, game_id)
        )
        result = cursor.fetchone()
        if result:
            player_id = result[0]
            logger.debug(f"Found existing player: '{nickname_to_use}' (Game: {game_id}), ID: {player_id}")
            update_fields, update_values = [], []
            if birth_date is not None: update_fields.append("birth_date = %s"); update_values.append(birth_date)
            if nationality is not None: update_fields.append("nationality = %s"); update_values.append(nationality)
            if status is not None: update_fields.append("status = %s"); update_values.append(status)
            if current_role is not None: update_fields.append("curr_role = %s"); update_values.append(current_role)
            if type is not None: update_fields.append("type = %s"); update_values.append(type)
            if update_fields:
                update_query = f"UPDATE Players SET {', '.join(update_fields)} WHERE player_id = %s"
                cursor.execute(update_query, (*update_values, player_id))
                logger.info(f"Updated existing player '{nickname_to_use}' (ID: {player_id}).")
            return player_id
        else:
            cursor.execute(
                """
                INSERT INTO Players (player_nickname, game_id, birth_date, nationality, status, curr_role, type) 
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING player_id;
                """,
                (nickname_to_use, game_id, birth_date, nationality, status, current_role, type)
            )
            new_player_id = cursor.fetchone()[0]
            logger.info(f"Created new player: '{nickname_to_use}' (Game: {game_id}), ID: {new_player_id}.")
            return new_player_id
    except psycopg2.Error as e:
        logger.error(f"Database error in get_or_create_player for '{nickname_to_use}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_or_create_player for '{nickname_to_use}': {e}")
        raise

def get_or_create_tournament(cursor, tournament_name, game_id, start_date, 
                             tier=None, end_date=None, type_val=None, region=None, location=None, 
                             prize_pool=None, tournament_weight=None, api_pagename=None):
    if not game_id or not start_date: 
        logger.warning("Game_id, or start_date is missing. Cannot get or create tournament.")
        return None
    name_to_use = tournament_name
    if not name_to_use and api_pagename: name_to_use = api_pagename
    if not name_to_use:
        logger.warning(f"Tournament identifier is missing for game_id {game_id}, start_date {start_date}. Cannot proceed.")
        return None
    try:
        cursor.execute(
            "SELECT tournament_id FROM Tournaments WHERE tournament_name = %s AND game_id = %s AND start_date = %s;",
            (name_to_use, game_id, start_date)
        )
        result = cursor.fetchone()
        if result:
            tournament_id = result[0]
            logger.debug(f"Found existing tournament: '{name_to_use}' (Game: {game_id}, Starts: {start_date}), ID: {tournament_id}")
            update_fields, update_values = [], []
            if tier is not None: update_fields.append("tier = %s"); update_values.append(tier)
            if end_date is not None: update_fields.append("end_date = %s"); update_values.append(end_date)
            if type_val is not None: update_fields.append("type = %s"); update_values.append(type_val)
            if region is not None: update_fields.append("region = %s"); update_values.append(region)
            if location is not None: update_fields.append("location = %s"); update_values.append(location)
            if prize_pool is not None: update_fields.append("prize_pool = %s"); update_values.append(prize_pool)
            if tournament_weight is not None: update_fields.append("tournament_weight = %s"); update_values.append(tournament_weight)
            if update_fields:
                update_query = f"UPDATE Tournaments SET {', '.join(update_fields)} WHERE tournament_id = %s"
                cursor.execute(update_query, (*update_values, tournament_id))
                logger.info(f"Updated existing tournament '{name_to_use}' (ID: {tournament_id}).")
            return tournament_id
        else:
            cursor.execute(
                """
                INSERT INTO Tournaments (tournament_name, game_id, tier, start_date, end_date, type, region, location, prize_pool, tournament_weight)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING tournament_id;
                """,
                (name_to_use, game_id, tier, start_date, end_date, type_val, region, location, prize_pool, tournament_weight)
            )
            new_tournament_id = cursor.fetchone()[0]
            logger.info(f"Created new tournament: '{name_to_use}' (Game: {game_id}, Starts: {start_date}), ID: {new_tournament_id}.")
            return new_tournament_id
    except psycopg2.Error as e:
        logger.error(f"Database error in get_or_create_tournament for '{name_to_use}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_or_create_tournament for '{name_to_use}': {e}")
        raise

# --- ENHANCED find_tournament_id_for_match ---
def find_tournament_id_for_match(cursor, tournament_api_name_raw, game_id, match_date_obj: datetime_date):
    """
    Finds a tournament_id from the Tournaments table based on its name (or a base name if it's a stage), 
    game_id, and ensuring the match_date falls within the tournament's start_date and end_date.
    """
    if not tournament_api_name_raw or not game_id or not match_date_obj:
        logger.warning("Missing tournament name, game_id, or match_date for finding tournament ID.")
        return None

    # Normalize the raw tournament name from API (replace underscores, strip whitespace)
    # The API might return a pagename like "PGL_Major_Copenhagen_2024" or a display name "PGL Major Copenhagen 2024"
    # Or a stage name "PGL Major Copenhagen 2024: Opening Stage"
    tournament_name_cleaned = str(tournament_api_name_raw).replace('_', ' ').strip()
    if not tournament_name_cleaned:
        logger.warning(f"Cleaned tournament name is empty for raw: '{tournament_api_name_raw}'. Cannot find tournament.")
        return None

    # Common stage indicators (regex patterns to strip them from the end)
    # This list can be expanded. Order might matter if some are substrings of others.
    stage_patterns = [
        r"\s*:\s*(Opening Stage|Challengers Stage|Legends Stage|Champions Stage|Playoffs|Group Stage|Finals|Qualifier|Main Event)\s*$",
        r"\s*-\s*(Opening Stage|Challengers Stage|Legends Stage|Champions Stage|Playoffs|Group Stage|Finals|Qualifier|Main Event)\s*$",
        r"\s+Play-In\s*$",
        r"\s+Last Chance Qualifier\s*$",
        r"\s+LCQ\s*$",
        r"\s+Regional Finals\s*$",
        r"\s+Stage [1-3]\s*$",
        r"\s+Phase [1-3]\s*$"
    ]

    search_names = [tournament_name_cleaned]
    base_name_derived = tournament_name_cleaned
    for pattern in stage_patterns:
        # Remove the stage part to get a potential base tournament name
        stripped_name = re.sub(pattern, "", base_name_derived, flags=re.IGNORECASE).strip()
        if stripped_name != base_name_derived and stripped_name: # If something was stripped and it's not empty
            if stripped_name not in search_names:
                search_names.append(stripped_name)
            base_name_derived = stripped_name # Continue stripping from the most stripped version
        # Also try stripping common separators if the pattern itself didn't include them
        if ":" in base_name_derived:
            potential_base = base_name_derived.split(":")[0].strip()
            if potential_base and potential_base not in search_names: search_names.append(potential_base)
        if " - " in base_name_derived:
            potential_base = base_name_derived.split(" - ")[0].strip()
            if potential_base and potential_base not in search_names: search_names.append(potential_base)


    logger.debug(f"Attempting to find tournament for match date {match_date_obj}, game {game_id}. Search names: {search_names}")

    for name_to_search in search_names:
        if not name_to_search: continue
        try:
            cursor.execute(
                """
                SELECT tournament_id, start_date, end_date FROM Tournaments 
                WHERE tournament_name = %s AND game_id = %s 
                  AND start_date <= %s AND end_date >= %s;
                """,
                (name_to_search, game_id, match_date_obj, match_date_obj)
            )
            results = cursor.fetchall()

            if len(results) == 1:
                logger.info(f"Found unique tournament ID {results[0][0]} for '{name_to_search}' (original API name: '{tournament_api_name_raw}') matching date range.")
                return results[0][0]
            elif len(results) > 1:
                logger.warning(f"Multiple tournaments found for '{name_to_search}' (Game: {game_id}) matching date range for match {match_date_obj}. Results: {results}. Picking the one with the latest start_date <= match_date.")
                best_match_id = None
                latest_start_date_found = None
                for res_id, res_start, res_end in results:
                    if latest_start_date_found is None or res_start > latest_start_date_found:
                        latest_start_date_found = res_start
                        best_match_id = res_id
                if best_match_id:
                    logger.info(f"Selected tournament ID {best_match_id} for '{name_to_search}' based on latest start_date criteria.")
                return best_match_id
        except psycopg2.Error as e:
            logger.error(f"Database error in find_tournament_id_for_match step for '{name_to_search}': {e}")
            # Continue to next search name if one fails
        except Exception as e:
            logger.error(f"Unexpected error in find_tournament_id_for_match step for '{name_to_search}': {e}")

    # If no match after trying all search names, try a broader search without date range as a last resort
    logger.warning(f"Could not find tournament for '{tournament_name_cleaned}' (or its base names) within date range. Trying fallback search by name only for '{search_names[0]}'.")
    try:
        cursor.execute(
            "SELECT tournament_id, start_date FROM Tournaments WHERE tournament_name = %s AND game_id = %s ORDER BY ABS(start_date - %s::date) LIMIT 1;",
            (search_names[0], game_id, match_date_obj) # Use the most specific name tried for fallback
        )
        fallback_result = cursor.fetchone()
        if fallback_result:
            logger.warning(f"Fallback: Found tournament ID {fallback_result[0]} for '{search_names[0]}' by name and game, closest to match date {match_date_obj}. This might not be the correct event if stages are separate.")
            return fallback_result[0]
    except psycopg2.Error as e:
        logger.error(f"Database error during fallback tournament search for '{search_names[0]}': {e}")
    
    logger.error(f"All attempts failed for tournament raw name '{tournament_api_name_raw}' (Cleaned: '{tournament_name_cleaned}', Game: {game_id}) for match date {match_date_obj}.")
    return None


def upsert_roster_entry(cursor, team_id, player_id, join_date, leave_date, is_substitute, role_during_tenure, status, player_nickname_for_roster):
    if not all([team_id, player_id, join_date is not None]): 
        logger.warning(f"Missing team_id, player_id, or join_date for roster entry. Skipping. T:{team_id}, P:{player_id}, J:{join_date}")
        return None
    try:
        cursor.execute(
            """
            INSERT INTO TeamRosters (team_id, player_id, player_nickname, join_date, leave_date, is_substitute, role_during_tenure, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id, player_id, join_date) DO UPDATE SET
                leave_date = EXCLUDED.leave_date,
                is_substitute = EXCLUDED.is_substitute,
                role_during_tenure = EXCLUDED.role_during_tenure,
                status = EXCLUDED.status,
                player_nickname = EXCLUDED.player_nickname 
            RETURNING roster_id;
            """,
            (team_id, player_id, player_nickname_for_roster, join_date, leave_date, is_substitute, role_during_tenure, status)
        )
        result = cursor.fetchone()
        if result:
            roster_id = result[0]
            logger.debug(f"Upserted TeamRosters entry for T:{team_id}, P:{player_id} (Nick: {player_nickname_for_roster}), J:{join_date}. Roster ID: {roster_id}")
            return roster_id
        else: 
            logger.warning(f"Upsert for TeamRosters T:{team_id}, P:{player_id}, J:{join_date} did not return an ID.")
            return None
    except psycopg2.Error as e:
        logger.error(f"Database error in upsert_roster_entry for T:{team_id}, P:{player_id}, J:{join_date}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in upsert_roster_entry for T:{team_id}, P:{player_id}, J:{join_date}: {e}")
        raise

def truncate_team_rosters_for_game(cursor, game_id):
    try:
        cursor.execute("SELECT team_id FROM Teams WHERE game_id = %s;", (game_id,))
        teams_in_game = cursor.fetchall()
        if not teams_in_game:
            logger.info(f"No teams found in DB for game_id '{game_id}'. No rosters to truncate.")
            return 0
        team_ids_to_truncate = [row[0] for row in teams_in_game]
        if not team_ids_to_truncate: 
            logger.info(f"No team_ids derived for game_id '{game_id}' to truncate rosters.")
            return 0
        team_ids_tuple = tuple(team_ids_to_truncate) if len(team_ids_to_truncate) > 1 else (team_ids_to_truncate[0],)
        delete_query = "DELETE FROM TeamRosters WHERE team_id IN %s;"
        cursor.execute(delete_query, (team_ids_tuple,)) 
        deleted_count = cursor.rowcount
        logger.info(f"Truncated (deleted) {deleted_count} TeamRosters entries for game_id '{game_id}'.")
        return deleted_count
    except psycopg2.Error as e:
        logger.error(f"Database error during truncate_team_rosters_for_game for game_id '{game_id}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in truncate_team_rosters_for_game for game_id '{game_id}': {e}")
        raise

# In scripts/lib/db_utils.py

def bulk_insert_data(conn, table_name, data_list, column_names, on_conflict_full_sql_clause=None):
    """
    Inserts multiple rows into a table using psycopg2.extras.execute_values for efficiency.
    Accepts a full ON CONFLICT SQL clause.

    Args:
        conn: Active psycopg2 connection object.
        table_name (str): The name of the table.
        data_list (list of dicts): Each dict represents a row. Keys must match column_names.
        column_names (list of str): Ordered list of column names for the INSERT statement.
        on_conflict_full_sql_clause (str, optional): The FULL SQL clause starting with "ON CONFLICT..."
                                                    e.g., "ON CONFLICT (series_lp_matchid) DO UPDATE SET ...".
                                                    If None, a simple INSERT is performed (or error on conflict).
    """
    if not data_list:
        logger.info(f"No data to bulk insert into {table_name}.")
        return 0

    rows_to_insert = []
    for record_dict in data_list:
        try:
            # Ensure all specified columns are present in the dict, using None if missing
            row_tuple = tuple(record_dict.get(col) for col in column_names)
            rows_to_insert.append(row_tuple)
        except Exception as e: 
            logger.error(f"Error preparing record for bulk insert into {table_name}: {record_dict}. Error: {e}")
            continue 
    
    if not rows_to_insert:
        logger.warning(f"No valid rows to insert into {table_name} after preparation.")
        return 0

    cols_str = ", ".join(f'"{col}"' for col in column_names) # Quote column names
    
    query = f"INSERT INTO {table_name} ({cols_str}) VALUES %s" #NOSONAR
    if on_conflict_full_sql_clause: # Expects full clause like "ON CONFLICT (col) DO UPDATE SET ..."
        query += f" {on_conflict_full_sql_clause}"

    query += ";" 

    cursor = None
    try:
        cursor = conn.cursor()
        psycopg2.extras.execute_values(cursor, query, rows_to_insert, page_size=500)
        # Commit is handled by the calling script's transaction management (e.g., 'with conn:' block)
        # If this function is called outside a 'with conn:' block, the caller must commit.
        logger.info(f"Bulk insert into {table_name} prepared for {len(rows_to_insert)} records. Commit should be handled by caller.")
        return len(rows_to_insert) 
    except psycopg2.Error as e:
        logger.error(f"Database error during bulk insert into {table_name}: {e}")
        # Try to log the query that failed, but be careful with large data
        try:
            failed_query_example = cursor.mogrify(query, [rows_to_insert[0]]).decode('utf-8', 'replace') if cursor and rows_to_insert else query
            logger.debug(f"Failed Query structure (first row example): {failed_query_example}")
        except Exception as log_e:
            logger.error(f"Error trying to log failed query: {log_e}")
        raise # Re-raise to allow calling function to handle transaction
    except Exception as e:
        logger.error(f"Unexpected error during bulk insert into {table_name}: {e}")
        raise # Re-raise
    finally:
        if cursor:
            cursor.close()
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logger.info("Testing db_utils.py...")
    # Test block remains the same as lib_db_utils_py_v4_final_tourn_schema
    # ... (you can copy the __main__ block from that version if needed for standalone testing of db_utils)
    logger.info("Standalone tests for db_utils.py would go here.")
