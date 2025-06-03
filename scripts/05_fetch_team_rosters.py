# scripts/05_fetch_team_rosters.py
import logging
import psycopg2
from datetime import datetime
from config import SUPPORTED_GAMES, LOG_LEVEL, API_KEY, DB_NAME
from .lib.api_utils import fetch_all_api_v3_data
from .lib.db_utils import (
    get_db_connection, 
    get_or_create_game, 
    get_or_create_team, 
    get_or_create_player, 
    truncate_team_rosters_for_game, 
    bulk_insert_data
)

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

NON_PLAYING_STAFF_ROLES = [
    "Coach", "Head Coach", "Assistant Coach", "Analyst", "Strategic Coach", "Mental Coach", "Performance Coach",
    "Manager", "Team Manager", "General Manager", "Esports Manager", "Performance Manager",
    "CEO", "Founder", "Co-Founder", "Chairman", "Owner", "President",
    "Streamer", "Content Creator", "Commentator", "Caster",
    "CFO", "COO", "Staff", "Head of Esports" 
]
NON_PLAYING_STAFF_TYPES = ["coach", "analyst", "manager", "caster", "commentator", "staff"] 

NON_PLAYING_STAFF_ROLES_LOWER = [role.lower() for role in NON_PLAYING_STAFF_ROLES]
NON_PLAYING_STAFF_TYPES_LOWER = [type_val.lower() for type_val in NON_PLAYING_STAFF_TYPES]

# DEBUG_PROCESS_LIMIT_SQUAD_ENTRIES = 20 # Process only this many global squad entries per game for debugging
DEBUG_PROCESS_LIMIT_SQUAD_ENTRIES = None 

def parse_date_from_api(date_str):
    if date_str and date_str not in ["0000-01-01", "0000-00-00"]:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(f"Could not parse date string '{date_str}'. Returning None.")
            return None
    return None

def fetch_and_store_team_rosters_globally():
    """
    Implements the following strategy:
    1. For each game, truncate TeamRosters.
    2. Make ONE global (paginated) call per game to /v3/squadplayer for all active roster members.
    3. For each entry, get/create team and player in DB using names/IDs from squadplayer data.
    4. Insert into TeamRosters.
    """
    conn = None
    total_rosters_inserted_all_games = 0
    
    logger.warning("This script assumes that the /v3/squadplayer endpoint can be queried globally. "              
                   "If this is not supported by the API, this script will likely fail "
                   "or return no data.")

    try:
        conn = get_db_connection()
        if not conn:
            return

        with conn, conn.cursor() as cursor:
            for game_db_id, game_config in SUPPORTED_GAMES.items():
                liquipedia_wiki_name = game_config.get("liquipedia_wiki")
                game_display_name = game_config.get("name", game_db_id)

                if not liquipedia_wiki_name:
                    logger.warning(f"Liquipedia wiki name not configured for game ID '{game_db_id}'. Skipping.")
                    continue

                logger.info(f"--- Starting to process team rosters for game: {game_display_name} (Wiki: {liquipedia_wiki_name}) using global squadplayer fetch ---")
                get_or_create_game(cursor, game_db_id, game_display_name)

                try:
                    logger.info(f"Truncating TeamRosters for game '{game_display_name}'...")
                    truncate_team_rosters_for_game(cursor, game_db_id)
                except Exception as e:
                    logger.error(f"Error truncating TeamRosters for '{game_display_name}': {e}")
                    conn.rollback() 
                    continue 

                # Step 1: Fetch all active squad player entries globally for the current wiki
                # Required fields: id (player handle), name (player real name), nationality, role, type, 
                #                  status (on roster), joindate, leavedate, 
                #                  newteam (team display name), pagename (team's page), link (player's page)
                squadplayer_api_query_fields = "id,name,nationality,role,type,status,joindate,leavedate,newteam,pagename,link"
                
                # Conditions for GLOBAL /squadplayer query:
                # - status on the squad must be 'active'
                # - leavedate must be empty or a "zero" date
                squad_conditions = "[[status::active]] AND ([[leavedate::]] OR [[leavedate::0000-00-00]] OR [[leavedate::0000-01-01]])"

                squad_api_params = {
                    'wiki': liquipedia_wiki_name,
                    'query': squadplayer_api_query_fields,
                    'conditions': squad_conditions,
                    'order': 'pagename ASC, id ASC' # Order by team pagename, then player id
                }
                
                logger.info(f"Fetching all active squad player entries for wiki '{liquipedia_wiki_name}' with conditions: {squad_conditions}...")
                all_active_squad_entries = fetch_all_api_v3_data("squadplayer", squad_api_params)

                if all_active_squad_entries is None:
                    logger.error(f"Failed to fetch any squad player data for wiki '{liquipedia_wiki_name}'. This might indicate global query is not supported or an API issue. Skipping game.")
                    continue
                if not all_active_squad_entries:
                    logger.info(f"No active squad player entries found for wiki '{liquipedia_wiki_name}'.")
                    continue
                
                logger.info(f"Fetched {len(all_active_squad_entries)} active squad entries for {game_display_name}. Processing...")
                if DEBUG_PROCESS_LIMIT_SQUAD_ENTRIES is not None:
                     logger.info(f"DEBUG MODE: Processing up to {DEBUG_PROCESS_LIMIT_SQUAD_ENTRIES} squad entries.")

                roster_entries_for_this_game = []
                processed_squad_entries_count = 0

                for squad_entry_data in all_active_squad_entries:
                    if DEBUG_PROCESS_LIMIT_SQUAD_ENTRIES is not None and processed_squad_entries_count >= DEBUG_PROCESS_LIMIT_SQUAD_ENTRIES:
                        logger.info(f"Reached debug processing limit for squad entries in {game_display_name}.")
                        break
                    processed_squad_entries_count += 1

                    # --- Team Info ---
                    # 'pagename' from /squadplayer is the TEAM's page.
                    # 'newteam' from /squadplayer is the TEAM's display name/template name.
                    team_api_raw_pagename = squad_entry_data.get("pagename") 
                    team_api_raw_name_from_template = squad_entry_data.get("newteam") 

                    team_name_for_db = None
                    if team_api_raw_name_from_template:
                        team_name_for_db = team_api_raw_name_from_template.replace('_', ' ').strip()
                    elif team_api_raw_pagename: 
                        team_name_for_db = team_api_raw_pagename.replace('_', ' ').strip()
                    
                    if not team_name_for_db:
                        logger.warning(f"Squad entry missing team identifier (newteam/pagename). Skipping: {squad_entry_data}")
                        continue
                    
                    team_id_db = get_or_create_team(
                        cursor, 
                        team_name=team_name_for_db, 
                        game_id=game_db_id,
                        is_disbanded=False, # Assumption: team hosting an active roster is active
                        api_pagename=team_api_raw_pagename 
                    )
                    if not team_id_db:
                        logger.warning(f"Could not get/create team_id for team '{team_name_for_db}' (Raw Pagename: {team_api_raw_pagename}). Skipping squad entry.")
                        continue

                    # --- Player Info ---
                    player_api_id = squad_entry_data.get("id") # Player's handle/nickname
                    if not player_api_id:
                        logger.warning(f"Squad member for team '{team_name_for_db}' missing player 'id'. Skipping: {squad_entry_data}")
                        continue

                    player_api_role = squad_entry_data.get("role", "").strip() 
                    if not player_api_role: # If the primary 'role' field is empty
                        extradata = squad_entry_data.get("extradata", {})
                        if isinstance(extradata, dict):
                            player_api_role_from_extra = extradata.get("role") # First check 'role' in extradata
                            if not player_api_role_from_extra:
                                player_api_role_from_extra = extradata.get("role2") # Then check 'role2' in extradata
                                
                            if player_api_role_from_extra:
                                player_api_role = player_api_role_from_extra.strip()
                                logger.debug(f"Player ID '{player_api_id}' on team '{team_name_for_db}': Primary role empty, using role from extradata: '{player_api_role}'")
                    player_api_type = squad_entry_data.get("type", "").lower() 

                    # Filter out non-playing staff for Players table and TeamRosters
                    is_staff = False
                    if player_api_role.lower() in NON_PLAYING_STAFF_ROLES_LOWER or \
                       player_api_type in NON_PLAYING_STAFF_TYPES_LOWER:
                        logger.debug(f"Identified as staff (will be cleaned by SQL later if inserted into Players): Player ID '{player_api_id}', Role: '{player_api_role}', Type: '{player_api_type}' on team '{team_name_for_db}'.")
                        is_staff = True # Flag for potential different handling if needed, but user wants SQL cleanup
                    
                    # User wants to insert all into Players, then clean with SQL.
                    # So, we pass the role and type as is.
                    player_nationality = squad_entry_data.get("nationality")
                    player_api_status_on_squad = squad_entry_data.get("status", "").lower() # Status on this roster
                    
                    player_link = squad_entry_data.get("link")
                    actual_player_pagename = None
                    if player_link and isinstance(player_link, str) and '/' in player_link:
                        actual_player_pagename = player_link.split('/')[-1]
                    
                    player_id_db = get_or_create_player(
                        cursor, 
                        player_nickname=player_api_id, 
                        game_id=game_db_id,
                        nationality=player_nationality,
                        status="active", # General player status; if on active squad, assume player is active
                        current_role=player_api_role, # Role from squadplayer for Players.curr_role
                        type=player_api_type if player_api_type else "Player", # Type from squadplayer for Players.type
                        api_pagename=actual_player_pagename
                    )
                    if not player_id_db:
                        logger.warning(f"Could not get/create player_id for '{player_api_id}'. Skipping roster entry.")
                        continue
                    
                    # If it's staff, we've added them to Players table (for later SQL cleanup by user),
                    # but we should NOT add them to TeamRosters as a playing member.
                    if is_staff:
                        logger.info(f"Staff member '{player_api_id}' added/updated in Players table. Skipping TeamRosters entry.")
                        continue


                    # --- Roster Info ---
                    join_date = parse_date_from_api(squad_entry_data.get("joindate"))
                    # leave_date should be NULL due to our API filter
                    leave_date = parse_date_from_api(squad_entry_data.get("leavedate")) 
                    
                    if not join_date:
                        logger.warning(f"Skipping roster entry for player '{player_api_id}' on team '{team_name_for_db}' due to missing/invalid join_date.")
                        continue
                    
                    is_sub = True if player_api_type == "substitute" else False
                    
                    roster_payload = {
                        "team_id": team_id_db,
                        "player_id": player_id_db,
                        "player_nickname": player_api_id, # As per user schema for TeamRosters
                        "join_date": join_date,
                        "leave_date": leave_date, 
                        "is_substitute": is_sub,
                        "role_during_tenure": player_api_role,
                        "status": player_api_status_on_squad # Should be 'active'
                    }
                    roster_entries_for_this_game.append(roster_payload)
                
                # Bulk insert all collected active roster entries for this game
                if roster_entries_for_this_game:
                    roster_cols = ["team_id", "player_id", "player_nickname", "join_date", "leave_date", "is_substitute", "role_during_tenure", "status"]
                    # Define the full ON CONFLICT clause
                    on_conflict_sql_rosters = "ON CONFLICT (team_id, player_id, join_date) DO NOTHING"
                    bulk_insert_data(conn, "TeamRosters", roster_entries_for_this_game, roster_cols,
                                     on_conflict_full_sql_clause=on_conflict_sql_rosters) 
                    total_rosters_inserted_all_games += len(roster_entries_for_this_game)
                    logger.info(f"Attempted to insert {len(roster_entries_for_this_game)} active roster entries for {game_display_name}.")
                
                conn.commit() 
                logger.info(f"Finished processing rosters for {game_display_name}.")

        logger.info(f"--- All games processed for rosters. Total new active roster entries inserted: {total_rosters_inserted_all_games}. ---")

    except psycopg2.Error as db_err:
        logger.error(f"Database connection or transaction error: {db_err}")
        if conn and not conn.closed:
            conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred in fetch_and_store_team_rosters_globally: {e}")
        if conn and not conn.closed:
            conn.rollback()
    finally:
        if conn and not conn.closed:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    if API_KEY == "YOUR_LIQUIPEDIA_API_KEY" or \
       DB_NAME == "your_esports_db_name":
        logging.critical("CRITICAL: Default API_KEY or database name found in config.py. "
                         "Please update them before running this script.")
    else:
        fetch_and_store_team_rosters_globally()