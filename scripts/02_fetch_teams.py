# scripts/02_fetch_teams.py
import logging
import psycopg2
from config import SUPPORTED_GAMES, LOG_LEVEL, API_KEY, DB_NAME
# FETCH_DATA_SINCE_DATE is not typically used for fetching all active teams,
# as their creation date might be old but they are still active.
from .lib.api_utils import fetch_all_api_v3_data
from .lib.db_utils import get_db_connection, get_or_create_game, get_or_create_team

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

def map_api_team_to_db_team(api_team_data, game_db_id):
    """Maps Liquipedia API team data to the structure needed for our Teams table."""
    
    api_pagename = api_team_data.get("pagename")
    team_name_from_api = api_team_data.get("name")
    
    if not team_name_from_api and not api_pagename:
        logger.warning(f"Skipping team record due to missing name and pagename: {api_team_data}")
        return None
    
    # Use pagename as a fallback if name is empty, though API usually provides 'name'
    db_team_name = team_name_from_api if team_name_from_api else api_pagename

    db_region = api_team_data.get("region") 

    # Location might be derived from 'locations' JSON or use 'region' as fallback
    # Example 'locations' from logs: {'region1': 'South America'}
    locations_json = api_team_data.get("locations")
    db_location = None
    if isinstance(locations_json, dict):
        # Prioritize more specific location keys if they exist (e.g., country)
        # This is a simple extraction, might need refinement based on 'locations' variability
        if "country" in locations_json: # Assuming 'country' might be a key
            db_location = locations_json["country"]
        elif "country1" in locations_json: # Another common pattern
            db_location = locations_json["country1"]
        elif "city1" in locations_json: # Or city
             db_location = locations_json["city1"]
        elif "region1" in locations_json: # Fallback to region1 from locations
             db_location = locations_json["region1"]
    
    if not db_location and db_region: # If no specific location found in JSON, use the top-level region
        db_location = db_region
    elif not db_location and not db_region: # If both are none, set location to None or a placeholder
        db_location = None # Or "Unknown" if you prefer not to have NULLs

    api_status = api_team_data.get("status")
    # We are querying for status::active, so is_disbanded should be False.
    db_is_disbanded = True if api_status == "disbanded" else False
    if api_status != "active": # This should ideally not happen if API filter works
        logger.warning(f"Team '{db_team_name}' (Pagename: {api_pagename}) has status '{api_status}' "
                       f"despite [[status::active]] filter. Setting is_disbanded accordingly.")
        if api_status == "active": # Should not happen if filter works
            db_is_disbanded = False
    
    return {
        "team_name": db_team_name,
        "game_id": game_db_id,
        "region": db_region,
        "location": db_location,
        "is_disbanded": db_is_disbanded,
        "api_pagename": api_pagename # Pass this to get_or_create_team for logging/potential future use
    }


def fetch_and_store_teams():
    """
    Fetches active team data from Liquipedia API v3 for supported games
    and stores them in the PostgreSQL database.
    """
    conn = None
    total_teams_processed_all_games = 0
    try:
        conn = get_db_connection()
        if not conn:
            return

        with conn, conn.cursor() as cursor: # Auto-commit or rollback on exit of 'with' block
            for game_db_id, game_config in SUPPORTED_GAMES.items():
                liquipedia_wiki_name = game_config.get("liquipedia_wiki")
                game_display_name = game_config.get("name", game_db_id)

                if not liquipedia_wiki_name:
                    logger.warning(f"Liquipedia wiki name not configured for game ID '{game_db_id}'. Skipping teams for this game.")
                    continue

                logger.info(f"--- Starting to fetch teams for game: {game_display_name} (Wiki: {liquipedia_wiki_name}) ---")

                # Ensure the game exists in our Games table (from 01_fetch_games.py)
                get_or_create_game(cursor, game_db_id, game_display_name)
                # No explicit commit needed here if conn is in autocommit=False and we commit per game later

                # Define fields to query from the API for teams
                # Based on example output: 'pagename', 'name', 'locations' (json), 'region', 'status'
                # Our DB 'Teams' table needs: team_name, game_id, region, location, is_disbanded
                team_api_query_fields = "pagename,name,region,locations,status"

                conditions = "[[status::active]]" # Only fetch active teams

                api_params = {
                    'wiki': liquipedia_wiki_name,
                    'query': team_api_query_fields,
                    'conditions': conditions,
                    'order': 'name ASC' 
                }

                all_teams_api_data = fetch_all_api_v3_data("team", api_params)

                if all_teams_api_data is None:
                    logger.error(f"Failed to fetch team data for {game_display_name}. Skipping this game.")
                    continue
                
                if not all_teams_api_data:
                    logger.info(f"No active teams found for {game_display_name} with current filters.")
                    continue

                logger.info(f"Fetched {len(all_teams_api_data)} active teams for {game_display_name}. Processing and storing...")

                game_teams_processed_count = 0
                for api_team_data_item in all_teams_api_data:
                    db_team_payload = map_api_team_to_db_team(api_team_data_item, game_db_id)
                    
                    if db_team_payload:
                        try:
                            get_or_create_team(
                                cursor=cursor,
                                team_name=db_team_payload["team_name"],
                                game_id=db_team_payload["game_id"],
                                region=db_team_payload["region"],
                                location=db_team_payload["location"],
                                is_disbanded=db_team_payload["is_disbanded"],
                                api_pagename=db_team_payload["api_pagename"]
                            )
                            game_teams_processed_count += 1
                        except Exception as e: # Catch errors from get_or_create_team
                            logger.error(f"Error processing/storing team '{db_team_payload.get('team_name')}' "
                                         f"(API Pagename: {db_team_payload.get('api_pagename')}): {e}")
                            # Decide if you want to rollback the whole game's batch or just skip this team
                            # For now, logging and continuing. Transaction is per game.
                
                conn.commit() # Commit after processing all teams for the current game
                logger.info(f"Finished processing {game_teams_processed_count} teams for {game_display_name}.")
                total_teams_processed_all_games += game_teams_processed_count

        logger.info(f"--- All supported games processed. Total teams processed across all games: {total_teams_processed_all_games}. ---")

    except psycopg2.Error as db_err:
        logger.error(f"Database connection or transaction error: {db_err}")
        if conn: # conn might be None if initial get_db_connection failed
            conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred in fetch_and_store_teams: {e}")
        if conn:
            conn.rollback() 
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    if API_KEY == "YOUR_LIQUIPEDIA_API_KEY" or \
       DB_NAME == "your_esports_db_name": # Simplified check
        logging.critical("CRITICAL: Default API_KEY or database name found in config.py. "
                         "Please update them before running this script.")
    else:
        fetch_and_store_teams()
