# scripts/03_fetch_players.py
import logging
import psycopg2
from config import SUPPORTED_GAMES, LOG_LEVEL, API_KEY, DB_NAME
from .lib.api_utils import fetch_all_api_v3_data
from .lib.db_utils import get_db_connection, get_or_create_game, get_or_create_player

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# DEBUG_PROCESS_LIMIT = 10 # Comment out or remove for full processing
DEBUG_PROCESS_LIMIT = None # Set to None to process all players

def map_api_player_to_db_player(api_player_data, game_db_id):
    """
    Maps Liquipedia API player data to the structure needed for our Players table.
    Assumes API data is already filtered for type 'Player'.
    Populates 'curr_role' from extradata and 'type' from API 'type'.
    """
    api_player_id_handle = api_player_data.get("id") 
    api_player_pagename = api_player_data.get("pagename")

    if not api_player_id_handle:
        logger.warning(f"Skipping player record due to missing 'id' (nickname): {api_player_data}")
        return None
    
    db_player_nickname = api_player_id_handle
    
    # Extract role from extradata to populate curr_role
    extradata = api_player_data.get("extradata", {})
    player_role_from_api = None
    if isinstance(extradata, dict):
        player_role_from_api = extradata.get("role")
        if not player_role_from_api: 
             player_role_from_api = extradata.get("role2")
    
    if player_role_from_api:
        logger.debug(f"Player '{db_player_nickname}' (Pagename: {api_player_pagename}) has API role in extradata: '{player_role_from_api}'. This will be stored in curr_role.")
    else:
        logger.debug(f"Player '{db_player_nickname}' (Pagename: {api_player_pagename}) has no role specified in extradata. curr_role will be NULL.")

    db_birth_date = api_player_data.get("birthdate")
    if db_birth_date == "0000-01-01":
        db_birth_date = None

    db_nationality = api_player_data.get("nationality")
    
    api_status = api_player_data.get("status") 
    db_status = api_status 
    if api_status != "Active": 
        logger.warning(f"Player '{db_player_nickname}' (Pagename: {api_player_pagename}) has API status '{api_status}' "
                       f"despite [[status::Active]] filter. Storing this status.")

    # Get the 'type' from the API response (should be 'Player' due to API filter)
    db_player_type = api_player_data.get("type")
    if db_player_type:
        logger.debug(f"Player '{db_player_nickname}' has API type: '{db_player_type}'.")
    else: # Should not happen if API filter [[type::Player]] works and type is always returned
        logger.warning(f"Player '{db_player_nickname}' missing 'type' field from API response, though it was queried.")


    return {
        "player_nickname": db_player_nickname,
        "game_id": game_db_id,
        "birth_date": db_birth_date,
        "nationality": db_nationality,
        "status": db_status, 
        "current_role": player_role_from_api, # This will be passed to get_or_create_player for curr_role
        "player_type": db_player_type,       # This will be passed for player_type (DB column 'type')
        "api_pagename": api_player_pagename
    }

def fetch_and_store_players():
    """
    Fetches individuals from Liquipedia API v3 (/player) filtered by 
    status 'Active' and type 'Player', and stores their information 
    (including curr_role and player_type) in the PostgreSQL Players table.
    """
    conn = None
    total_players_inserted_all_games = 0
    total_players_evaluated_all_games = 0
    try:
        conn = get_db_connection()
        if not conn:
            return

        with conn, conn.cursor() as cursor:
            for game_db_id, game_config in SUPPORTED_GAMES.items():
                liquipedia_wiki_name = game_config.get("liquipedia_wiki")
                game_display_name = game_config.get("name", game_db_id)

                if not liquipedia_wiki_name:
                    logger.warning(f"Liquipedia wiki name not configured for game ID '{game_db_id}'. Skipping players for this game.")
                    continue

                logger.info(f"--- Starting to fetch players for game: {game_display_name} (Wiki: {liquipedia_wiki_name}) ---")

                get_or_create_game(cursor, game_db_id, game_display_name)

                # Request 'type' field from API. Also 'extradata' for role.
                player_api_query_fields = "id,pagename,name,nationality,birthdate,status,extradata,teampagename,region,type"
                
                # Filter by status 'Active' AND type 'Player' at API level
                # Using 'Player' (capitalized) as it's common for type classifications.
                # If this fails, try 'player' (lowercase).
                conditions = "[[status::Active]] AND ([[type::Player]] OR [[type::player]])"

                api_params = {
                    'wiki': liquipedia_wiki_name,
                    'query': player_api_query_fields,
                    'conditions': conditions,
                    'order': 'id ASC' 
                }

                all_players_api_data = fetch_all_api_v3_data("player", api_params)

                if all_players_api_data is None:
                    logger.error(f"Failed to fetch player data for {game_display_name} with type 'Player'. Skipping this game.")
                    continue
                
                if not all_players_api_data:
                    logger.info(f"No individuals with status 'Active' and type 'Player' found for {game_display_name} via API.")
                    continue

                logger.info(f"Fetched {len(all_players_api_data)} individuals with API status 'Active' and type 'Player' for {game_display_name}. Processing all...")
                if DEBUG_PROCESS_LIMIT is not None: # Keep debug limit if set for testing
                    logger.info(f"DEBUG MODE: Processing up to {DEBUG_PROCESS_LIMIT} players for this game.")


                game_players_inserted_count = 0
                game_players_evaluated_this_game = 0 

                for i, api_player_data_item in enumerate(all_players_api_data):
                    if DEBUG_PROCESS_LIMIT is not None and i >= DEBUG_PROCESS_LIMIT: 
                        logger.info(f"Reached debug processing limit of {DEBUG_PROCESS_LIMIT} for {game_display_name}.")
                        break
                    
                    game_players_evaluated_this_game += 1
                    db_player_payload = map_api_player_to_db_player(api_player_data_item, game_db_id) 
                    
                    if db_player_payload: 
                        try:
                            player_id = get_or_create_player(
                                cursor=cursor,
                                player_nickname=db_player_payload["player_nickname"],
                                game_id=db_player_payload["game_id"],
                                birth_date=db_player_payload["birth_date"],
                                nationality=db_player_payload["nationality"],
                                status=db_player_payload["status"], 
                                current_role=db_player_payload["current_role"], 
                                type=db_player_payload["player_type"], # Pass player_type for DB 'type' column
                                api_pagename=db_player_payload["api_pagename"]
                            )
                            if player_id: 
                                game_players_inserted_count +=1 
                        except Exception as e:
                            logger.error(f"Error processing/storing player '{db_player_payload.get('player_nickname')}' "
                                         f"(API Pagename: {db_player_payload.get('api_pagename')}): {e}")
                            conn.rollback() 
                            logger.error(f"Transaction rolled back for game {game_display_name} due to error processing player.")
                            break 
                
                if not conn.closed and cursor.connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INTRANS:
                     logger.info(f"Committing transaction for {game_display_name} after processing players.")
                     pass # Let the 'with conn:' block handle the commit.

                logger.info(f"For {game_display_name}: Evaluated {game_players_evaluated_this_game} individuals from API. Inserted/Updated {game_players_inserted_count} players.")
                total_players_inserted_all_games += game_players_inserted_count
                total_players_evaluated_all_games += game_players_evaluated_this_game 
        
        logger.info(f"--- All games processed. Total players evaluated: {total_players_evaluated_all_games}. Total players inserted/updated in DB: {total_players_inserted_all_games}. ---")

    except psycopg2.Error as db_err:
        logger.error(f"Database connection or transaction error: {db_err}")
        if conn and not conn.closed: 
            conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred in fetch_and_store_players: {e}")
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
        fetch_and_store_players()
