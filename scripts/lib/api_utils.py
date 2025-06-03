# scripts/lib/api_utils.py
import requests
import time
import logging
from datetime import datetime, timezone # For current date, timezone-aware
from config import API_V3_BASE_URL, API_KEY, USER_AGENT, API_REQUEST_DELAY, API_V3_LIMIT, LOG_LEVEL

# Configure basic logging (if not already configured by the calling script)
# This allows the library to log independently if needed, but respects root logger if set.
logger = logging.getLogger(__name__)
if not logger.handlers: # Avoid adding multiple handlers if already configured
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')


def make_api_v3_request(endpoint, params=None):
    """
    Makes a GET request to a specific Liquipedia API v3 endpoint.

    Args:
        endpoint (str): The API endpoint (e.g., "team", "player", "tournament").
        params (dict, optional): Dictionary of query parameters for the API request.
                                 Common params: 'wiki', 'conditions', 'query',
                                                'limit', 'offset', 'order', 'groupby'.

    Returns:
        list: A list of dictionaries, where each dictionary represents an item
              from the API's "result" array, or None if an error occurs or
              the response is not as expected.
    """
    if API_KEY == "YOUR_LIQUIPEDIA_API_KEY":
        logger.critical("API Key not set in config.py. Aborting API call.")
        return None

    if not params:
        params = {}

    # Ensure 'limit' is included if not specified, using the config default
    if 'limit' not in params:
        params['limit'] = API_V3_LIMIT

    # The 'wiki' parameter is mandatory for all v3 calls
    if 'wiki' not in params or not params['wiki']:
        logger.error("The 'wiki' parameter is mandatory and was not provided for the API call.")
        return None

    request_url = f"{API_V3_BASE_URL}{endpoint.lstrip('/')}"
    
    headers = {
        'Authorization': f'Apikey {API_KEY}',
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'gzip' # As per Liquipedia API docs
    }

    logger.debug(f"Making API v3 request to: {request_url} with params: {params}")

    try:
        response = requests.get(request_url, headers=headers, params=params)
        
        # Respect rate limit AFTER every request, successful or not
        logger.debug(f"Waiting for {API_REQUEST_DELAY} seconds due to rate limit.")
        time.sleep(API_REQUEST_DELAY)

        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        
        data = response.json()

        if "error" in data and data["error"]:
            logger.error(f"Liquipedia API v3 Error for endpoint '{endpoint}': {data['error']}")
            if "result" not in data: 
                 return None
        
        if "warning" in data and data["warning"]:
            logger.warning(f"Liquipedia API v3 Warning for endpoint '{endpoint}': {data['warning']}")

        if "result" not in data or not isinstance(data["result"], list):
            logger.error(f"Unexpected API response format for endpoint '{endpoint}': "
                         f"'result' key missing or not a list. Response: {data}")
            return None
            
        logger.debug(f"Successfully fetched {len(data['result'])} items from endpoint '{endpoint}'.")
        return data["result"]

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred for endpoint '{endpoint}': {http_err} - Response: {response.text}")
        try:
            error_data = response.json()
            if "error" in error_data:
                logger.error(f"Liquipedia API v3 (from HTTP error content): {error_data['error']}")
        except ValueError: 
            pass
        return None
    except requests.exceptions.RequestException as req_err:
        logger.error(f"API Request Error for endpoint '{endpoint}': {req_err}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during API call to '{endpoint}': {e}")
        return None

def fetch_all_api_v3_data(endpoint, base_params):
    """
    Fetches all data for a given Liquipedia API v3 endpoint, handling pagination.

    Args:
        endpoint (str): The API endpoint (e.g., "team", "player").
        base_params (dict): The base parameters for the API query, excluding 'offset'.
                            Must include 'wiki'. 'limit' will be managed by this function.

    Returns:
        list: A list of all fetched records, or an empty list if an error occurs or no data.
    """
    all_records = []
    offset = 0
    
    limit_per_page = base_params.get('limit', API_V3_LIMIT)
    if limit_per_page > API_V3_LIMIT: 
        limit_per_page = API_V3_LIMIT
    
    if 'wiki' not in base_params or not base_params['wiki']:
        logger.error("The 'wiki' parameter is mandatory for fetching all data and was not provided.")
        return []

    logger.info(f"Starting to fetch all data for API v3 endpoint '{endpoint}', wiki '{base_params['wiki']}'")

    while True:
        current_params = {**base_params, 'offset': offset, 'limit': limit_per_page}
        
        logger.debug(f"Fetching with offset: {offset}, limit: {limit_per_page}")
        
        batch = make_api_v3_request(endpoint, current_params)

        if batch is None: 
            logger.error("Failed to fetch a batch of data. Stopping pagination for this query.")
            break 
        
        if not batch: 
            logger.info("No more data found in the current batch. Pagination complete.")
            break

        all_records.extend(batch)
        
        if len(batch) < limit_per_page:
            logger.info(f"Fetched {len(batch)} records, which is less than the page limit ({limit_per_page}). Assuming end of data.")
            break
        
        offset += len(batch) 
        logger.info(f"Fetched {len(batch)} records. Total fetched so far: {len(all_records)}. Moving to next offset: {offset}")

    logger.info(f"Finished fetching all data for endpoint '{endpoint}'. Total records retrieved: {len(all_records)}")
    return all_records

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG) 
    logger.info("Testing lib_api_utils.py...")

    # Get current date for filtering (timezone-aware)
    current_datetime_utc = datetime.now(timezone.utc)
    # This was the format used when Test 4 produced good logs for you.
    current_date_str_api_format = current_datetime_utc.strftime("%Y-%m-%d %H:%M:%S") 
    current_day_str_api_format = current_datetime_utc.strftime("%Y-%m-%d")       # For date-only comparisons


    if API_KEY != "YOUR_LIQUIPEDIA_API_KEY":
        test_wiki = "counterstrike" 
        target_team_pagename_for_test = "Team_Vitality" 

        logger.info(f"\n--- Test 1: Fetching ACTIVE {target_team_pagename_for_test} information from '{test_wiki}' wiki ---")
        team_params = {
            'wiki': test_wiki,
            'limit': 1,
            'conditions': f"[[pagename::{target_team_pagename_for_test}]] AND [[status::active]]" 
        }
        team_data = make_api_v3_request("team", team_params)
        if team_data:
            logger.info(f"Data for {target_team_pagename_for_test}: {team_data[0]}")
            team_status = team_data[0].get('status', 'N/A')
            logger.info(f"{target_team_pagename_for_test} status: {team_status}")
            if team_status != 'active': 
                logger.warning(f"{target_team_pagename_for_test} is not 'active' (status: {team_status})!")
        else:
            logger.error(f"Failed to fetch data for {target_team_pagename_for_test} or it's not 'active'.")

        logger.info(f"\n--- Test 2: Fetching ACTIVE PLAYERS (excluding staff roles) from {target_team_pagename_for_test} from '{test_wiki}' wiki ---")
        excluded_roles = ["Coach", "Analyst", "Founder, CEO", "Esports Manager", "Head of Esports", "Manager", "CEO"]
        role_conditions = " AND ".join([f"[[role::!{role}]]" for role in excluded_roles])
        
        squadplayer_params = {
            'wiki': test_wiki,
            'query': 'id,name,nationality,role,joindate,status,teamtemplate', 
            'conditions': f"[[pagename::{target_team_pagename_for_test}]] AND [[status::active]] AND {role_conditions}"
        }
        logger.debug(f"Squadplayer conditions for Test 2: {squadplayer_params['conditions']}")
        squadplayers_batch = make_api_v3_request("squadplayer", squadplayer_params)
        if squadplayers_batch:
            logger.info(f"Fetched {len(squadplayers_batch)} ACTIVE PLAYERS (filtered roles) for {target_team_pagename_for_test}:")
            for sp in squadplayers_batch:
                logger.info(f"  - Player ID: {sp.get('id')} (Name: {sp.get('name')}), Role: {sp.get('role')}, Joined: {sp.get('joindate')}, Status: {sp.get('status')}")
        else:
            logger.error(f"Failed to fetch active players (filtered roles) for {target_team_pagename_for_test}.")


        logger.info(f"\n--- Test 3: Fetching FINISHED S-Tier (Tier 1) Tournaments from '{test_wiki}' wiki ---")
        tournament_params = {
            'wiki': test_wiki,
            'limit': 3,
            'query': 'pagename,name,game,startdate,enddate,prizepool,liquipediatier,type,locations,status', 
            'conditions': f"[[liquipediatier::1]] AND [[enddate::<{current_day_str_api_format}]]", 
            'order': 'enddate DESC'
        }
        tournaments_batch = make_api_v3_request("tournament", tournament_params)
        if tournaments_batch:
            logger.info(f"Fetched {len(tournaments_batch)} finished S-Tier tournaments:")
            for t in tournaments_batch:
                logger.info(f"  - {t.get('name')} (Tier: {t.get('liquipediatier')}, Ended: {t.get('enddate')}, Status: {t.get('status')})")
        else:
            logger.error("Failed to fetch finished S-Tier tournaments for Test 3.")

        logger.info(f"\n--- Test 4: Fetching FINISHED Matches from '{test_wiki}' wiki ---")
        # Reverting Test 4 conditions to exactly what produced your successful log for matches.
        match_params = {
            'wiki': test_wiki,
            'limit': 3, 
            'query': 'pagename,match2id,date,tournament,game,winner,bestof,match2opponents,finished,status',
            'conditions': f"[[finished::1]] AND [[date::<{current_date_str_api_format}]]", # Using YYYY-MM-DD HH:MM:SS for date comparison
            'order': 'date DESC'
        }
        logger.debug(f"Match conditions for Test 4: {match_params['conditions']}")
        matches_batch = make_api_v3_request("match", match_params)
        if matches_batch:
            logger.info(f"Fetched {len(matches_batch)} finished matches (series):")
            for i, m in enumerate(matches_batch):
                logger.info(f"--- Match {i+1} Data (API Finished: {m.get('finished')}, API Status: {m.get('status')}) ---")
                logger.info(f"  Raw match data: {m}") 
                
                match_id = m.get('match2id', 'N/A')
                match_pagename = m.get('pagename', 'N/A')
                match_date = m.get('date', 'N/A')
                winner_api = m.get('winner', 'N/A') 

                opponents_data_raw = m.get('match2opponents')
                logger.info(f"  Raw match2opponents for {match_id}: {opponents_data_raw}")

                opponent_names = ["N/A", "N/A"]
                if isinstance(opponents_data_raw, list): 
                    for idx, opp_item in enumerate(opponents_data_raw):
                        if idx < 2 and isinstance(opp_item, dict):
                            opponent_names[idx] = opp_item.get('name', opp_item.get('pagename', opp_item.get('id', 'N/A')))
                        elif idx < 2 and isinstance(opp_item, str):
                            opponent_names[idx] = opp_item
                elif isinstance(opponents_data_raw, dict): 
                    for idx in range(2):
                        opponent_info = opponents_data_raw.get(f'opponent{idx+1}')
                        if isinstance(opponent_info, dict):
                            opponent_names[idx] = opponent_info.get('name', opponent_info.get('pagename', opponent_info.get('id', 'N/A')))
                        elif isinstance(opponent_info, str): 
                            opponent_names[idx] = opponent_info
                
                logger.info(f"  Parsed: ID: {match_id} (Pagename: {match_pagename}) - ({opponent_names[0]} vs {opponent_names[1]}), Winner: {winner_api}, Date: {match_date}")
        else:
            logger.error("Failed to fetch finished matches for Test 4.")
            
        # --- Test 5: MODIFIED SECTION ---
        logger.info(f"\n--- Test 5: Fetching individual ACTIVE player data for 'max' (9z) from '{test_wiki}' using /player endpoint ---")
        player_id_from_squadplayer = "max" # This is the ID from /squadplayer for "max"
        
        # Step 5.1: Try fetching by 'id' from /player endpoint, with status 'Active' (uppercase A)
        logger.info(f"Step 5.1: Attempting to fetch player by ID '{player_id_from_squadplayer}' from /player endpoint with status 'Active'...")
        player_params_by_id_active = {
            'wiki': test_wiki,
            'limit': 1,
            'query': "id,name,pagename,nationality,status,birthdate,extradata,teampagename", # Added pagename
            'conditions': f"[[id::{player_id_from_squadplayer}]] AND [[status::Active]]" # Uppercase 'Active'
        }
        player_data_by_id_active_result = make_api_v3_request("player", player_params_by_id_active)
        
        if player_data_by_id_active_result:
            logger.info(f"SUCCESS (by ID '{player_id_from_squadplayer}', status 'Active'): Data: {player_data_by_id_active_result[0]}")
            # Further checks if needed, e.g., ensuring the pagename matches expectations if known
        else:
            logger.error(f"Failed to fetch player by ID '{player_id_from_squadplayer}' with status 'Active' using /player endpoint.")
            logger.info("This could mean 'max' (as an ID) is not found in the /player table, or his status there isn't 'Active'.")
            logger.info("The /player endpoint might use a different primary identifier (like a full pagename) than the 'id' from /squadplayer for this specific player.")
            logger.info("For example, if 'max's player page on Liquipedia is '/counterstrike/Max_(Maximiliano_Gonzalez)', then 'Max_(Maximiliano_Gonzalez)' would be his pagename.")

    else:
        logger.warning("API_KEY is not set in config.py. Skipping direct API tests.")

    logger.info("Finished testing lib_api_utils.py.")