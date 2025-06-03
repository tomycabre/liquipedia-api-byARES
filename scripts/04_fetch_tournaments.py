# scripts/04_fetch_tournaments.py
import logging
import psycopg2
from datetime import datetime, timezone, date as datetime_date 
from config import SUPPORTED_GAMES, LOG_LEVEL, API_KEY, DB_NAME
from .lib.api_utils import fetch_all_api_v3_data
from .lib.db_utils import get_db_connection, get_or_create_game, get_or_create_tournament

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# DEBUG_PROCESS_LIMIT = 5 
DEBUG_PROCESS_LIMIT = None 

# --- Tournament Weight Calculation Configuration ---
# Tier scores (higher is better)
# Liquipedia API returns tier as a string, e.g., "1", "2", "Qualifier"
TIER_VALUE_SCORES = {
    "1": 100,  # S-Tier
    "2": 75,   # A-Tier
    "3": 50,   # B-Tier
    "4": 25,   # C-Tier
    "Qualifier": 15,
    "Show Match": 10,
    # Add other known string values for tiers if necessary
}
DEFAULT_TIER_SCORE = 10 # For tiers not in the map or if tier is None/empty

# Weighting factors for combining scores
TIER_WEIGHT_FACTOR = 0.70
PRIZE_POOL_WEIGHT_FACTOR = 0.30
# ----------------------------------------------------

def parse_locations(locations_json):
    """
    Parses the 'locations' JSON from the API to extract region and a general location string.
    """
    region = None
    location_str = None
    if isinstance(locations_json, dict):
        region = locations_json.get('region1') 
        city = locations_json.get('city1')
        country = locations_json.get('country1')
        if city and country: location_str = f"{city}, {country}"
        elif country: location_str = country
        elif city: location_str = city
        elif region: location_str = region
    return region, location_str

def calculate_tournament_weight(api_tournament_data, min_prize_for_game, max_prize_for_game):
    """Calculates a weight for the tournament based on tier and prize pool."""
    tier_str = api_tournament_data.get("liquipediatier")
    prize_pool_api = api_tournament_data.get("prizepool")

    # Calculate tier score
    tier_score = TIER_VALUE_SCORES.get(str(tier_str).strip(), DEFAULT_TIER_SCORE) if tier_str else DEFAULT_TIER_SCORE

    # Calculate normalized prize pool score (0-1)
    normalized_prize_score = 0.0
    if prize_pool_api is not None:
        try:
            current_prize = float(prize_pool_api)
            if max_prize_for_game > min_prize_for_game: # Avoid division by zero
                normalized_prize_score = (current_prize - min_prize_for_game) / (max_prize_for_game - min_prize_for_game)
                normalized_prize_score = max(0, min(1, normalized_prize_score)) # Clamp between 0 and 1
            elif max_prize_for_game == min_prize_for_game and current_prize > 0: # All prizes are the same and non-zero
                normalized_prize_score = 1.0 # Or 0.5, depending on desired behavior
            # If current_prize is 0 or less, or max_prize = min_prize = 0, score remains 0
        except (ValueError, TypeError):
            logger.warning(f"Could not parse prize pool '{prize_pool_api}' for weight calculation. Treating as 0.")
            normalized_prize_score = 0.0
    
    final_weight = (tier_score * TIER_WEIGHT_FACTOR) + (normalized_prize_score * 100 * PRIZE_POOL_WEIGHT_FACTOR) # Prize score scaled to 0-100 for weighting
    
    logger.debug(f"Tournament: {api_tournament_data.get('name')}, Tier: {tier_str} (Score: {tier_score}), Prize: {prize_pool_api}, NormPrizeScore: {normalized_prize_score:.2f}, FinalWeight: {final_weight:.2f}")
    return round(final_weight, 2)


def map_api_tournament_to_db(api_tournament_data, game_db_id, tournament_weight_val):
    """Maps Liquipedia API tournament data to the structure for our Tournaments table."""
    
    api_pagename = api_tournament_data.get("pagename")
    tournament_name_from_api = api_tournament_data.get("name")

    if not tournament_name_from_api and not api_pagename:
        logger.warning(f"Skipping tournament record due to missing name and pagename: {api_tournament_data}")
        return None
        
    db_tournament_name = tournament_name_from_api if tournament_name_from_api else api_pagename
    db_tier = api_tournament_data.get("liquipediatier") 
    
    db_start_date_str = api_tournament_data.get("startdate")
    db_start_date = None
    if db_start_date_str and db_start_date_str != "0000-01-01":
        try:
            db_start_date = datetime.strptime(db_start_date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(f"Could not parse startdate '{db_start_date_str}' for tournament '{db_tournament_name}'. Setting to None.")
            db_start_date = None 
    
    db_end_date_str = api_tournament_data.get("enddate")
    db_end_date = None
    if db_end_date_str and db_end_date_str != "0000-01-01":
        try:
            db_end_date = datetime.strptime(db_end_date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(f"Could not parse enddate '{db_end_date_str}' for tournament '{db_tournament_name}'. Setting to None.")
            db_end_date = None

    db_type = api_tournament_data.get("type") 
    locations_json = api_tournament_data.get("locations")
    db_region, db_location = parse_locations(locations_json)
    db_prize_pool = api_tournament_data.get("prizepool")
    api_status = api_tournament_data.get("status") 

    return {
        "tournament_name": db_tournament_name,
        "game_id": game_db_id,
        "tier": db_tier,
        "start_date": db_start_date, 
        "end_date": db_end_date,     
        "type_val": db_type, 
        "region": db_region,
        "location": db_location,
        "prize_pool": db_prize_pool,
        "tournament_weight": tournament_weight_val, # Include calculated weight
        "api_pagename": api_pagename,
        "api_status": api_status 
    }

def fetch_and_store_tournaments():
    """
    Fetches concluded tournament data, calculates their weight, and stores them.
    """
    conn = None
    total_tournaments_inserted_all_games = 0
    total_tournaments_evaluated_all_games = 0
    
    current_date_obj = datetime.now(timezone.utc).date() 
    cs2_filter_start_date_obj = datetime_date(2024, 3, 16) 

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

                logger.info(f"--- Starting to fetch tournaments for game: {game_display_name} (Wiki: {liquipedia_wiki_name}) ---")
                get_or_create_game(cursor, game_db_id, game_display_name)

                tournament_api_query_fields = "pagename,name,game,startdate,enddate,prizepool,liquipediatier,type,locations,status,organizers"
                
                api_conditions_list = []
                # Date filter: must have ended before the current day
                # Use current_date_obj.strftime for the API condition string
                api_conditions_list.append("[[liquipediatiertype::!Points]]") 
                if game_db_id == "csgo" or game_db_id == "cs2": # Apply CS2 specific start date filter
                    api_conditions_list.append(f"[[startdate::>{cs2_filter_start_date_obj.strftime('%Y-%m-%d')}]]") 
                
                api_conditions = " AND ".join(api_conditions_list)

                api_params = {
                    'wiki': liquipedia_wiki_name,
                    'query': tournament_api_query_fields,
                    'conditions': api_conditions, 
                    'order': 'enddate DESC' 
                }
                logger.debug(f"Fetching tournaments with API conditions: {api_conditions}")
                all_tournaments_api_data = fetch_all_api_v3_data("tournament", api_params)

                if all_tournaments_api_data is None:
                    logger.error(f"Failed to fetch tournament data for {game_display_name}. Skipping.")
                    continue
                
                if not all_tournaments_api_data:
                    logger.info(f"No ended tournaments found for {game_display_name} with current API filters: {api_conditions}")
                    continue

                logger.info(f"Fetched {len(all_tournaments_api_data)} tournaments for {game_display_name} from API. Applying Python date filters and calculating weights...")
                
                valid_tournaments_for_weight_calc = []
                for api_tourn_item in all_tournaments_api_data:
                    start_date_str = api_tourn_item.get("startdate")
                    end_date_str = api_tourn_item.get("enddate")
                    
                    start_date_obj, end_date_obj = None, None
                    try:
                        if start_date_str and start_date_str != "0000-01-01":
                            start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                        if end_date_str and end_date_str != "0000-01-01":
                            end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                    except ValueError:
                        logger.warning(f"Skipping tournament '{api_tourn_item.get('name')}' due to unparseable date(s).")
                        continue

                    if not start_date_obj or not end_date_obj:
                        logger.debug(f"Skipping tournament '{api_tourn_item.get('name')}' due to missing essential date data for Python filter.")
                        continue

                    passes_python_filter = False
                    if not start_date_obj > current_date_obj:
                        if game_db_id == "csgo" or game_db_id == "cs2": 
                            if start_date_obj >= cs2_filter_start_date_obj:
                                passes_python_filter = True
                        else: 
                            passes_python_filter = True
                    
                    if passes_python_filter:
                        valid_tournaments_for_weight_calc.append(api_tourn_item)
                    else:
                        logger.debug(f"Skipped tournament '{api_tourn_item.get('name')}' due to Python date filter. Start: {start_date_obj}, End: {end_date_obj}")
                
                if not valid_tournaments_for_weight_calc:
                    logger.info(f"No tournaments remained for {game_display_name} after strict Python date filtering.")
                    conn.commit() 
                    continue

                prize_pools = []
                for t_data in valid_tournaments_for_weight_calc:
                    pp = t_data.get("prizepool")
                    if pp is not None:
                        try:
                            prize_pools.append(float(pp))
                        except (ValueError, TypeError):
                            pass 
                
                min_prize = min(prize_pools) if prize_pools else 0.0
                max_prize = max(prize_pools) if prize_pools else 0.0
                logger.info(f"For {game_display_name}, Min Prize: {min_prize}, Max Prize: {max_prize} among {len(prize_pools)} tournaments with prize pools.")


                game_tournaments_inserted_count = 0
                game_tournaments_evaluated_this_game = 0

                for i, api_tournament_data_item in enumerate(valid_tournaments_for_weight_calc):
                    if DEBUG_PROCESS_LIMIT is not None and i >= DEBUG_PROCESS_LIMIT:
                        logger.info(f"Reached debug processing limit of {DEBUG_PROCESS_LIMIT} for {game_display_name}.")
                        break
                    
                    game_tournaments_evaluated_this_game += 1
                    
                    calculated_weight = calculate_tournament_weight(api_tournament_data_item, min_prize, max_prize)
                    db_tournament_payload = map_api_tournament_to_db(api_tournament_data_item, game_db_id, calculated_weight)
                    
                    if db_tournament_payload: 
                        try:
                            get_or_create_tournament(
                                cursor=cursor,
                                tournament_name=db_tournament_payload["tournament_name"],
                                game_id=db_tournament_payload["game_id"],
                                start_date=db_tournament_payload["start_date"],
                                tier=db_tournament_payload["tier"],
                                end_date=db_tournament_payload["end_date"],
                                type_val=db_tournament_payload["type_val"],
                                region=db_tournament_payload["region"],
                                location=db_tournament_payload["location"],
                                prize_pool=db_tournament_payload["prize_pool"],
                                tournament_weight=db_tournament_payload["tournament_weight"], 
                                api_pagename=db_tournament_payload["api_pagename"]
                            )
                            game_tournaments_inserted_count += 1
                        except Exception as e:
                            logger.error(f"Error processing/storing tournament '{db_tournament_payload.get('tournament_name')}' "
                                         f"(API Pagename: {db_tournament_payload.get('api_pagename')}): {e}")
                            conn.rollback()
                            logger.error(f"Transaction rolled back for game {game_display_name} due to error processing tournament.")
                            break 
                    else: 
                        logger.debug(f"Skipped tournament due to mapping failure (should have been caught by date checks): {api_tournament_data_item.get('name')}")


                logger.info(f"For {game_display_name}: API fetched {len(all_tournaments_api_data)}. Python filtered to {len(valid_tournaments_for_weight_calc)}. Evaluated for DB: {game_tournaments_evaluated_this_game}. Inserted/Updated {game_tournaments_inserted_count}.")
                total_tournaments_inserted_all_games += game_tournaments_inserted_count
                total_tournaments_evaluated_all_games += game_tournaments_evaluated_this_game
                
                conn.commit() 

        logger.info(f"--- All games processed. Total tournaments evaluated: {total_tournaments_evaluated_all_games}. Total tournaments inserted/updated: {total_tournaments_inserted_all_games}. ---")

    except psycopg2.Error as db_err:
        logger.error(f"Database connection or transaction error: {db_err}")
        if conn and not conn.closed:
            conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred in fetch_and_store_tournaments: {e}")
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
        fetch_and_store_tournaments()
