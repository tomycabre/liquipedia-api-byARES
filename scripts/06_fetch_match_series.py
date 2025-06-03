# scripts/06_fetch_match_series.py
import logging
import psycopg2
from datetime import datetime, timezone, date as datetime_date
from config import SUPPORTED_GAMES, LOG_LEVEL, API_KEY, DB_NAME
from .lib.api_utils import fetch_all_api_v3_data # Assuming relative import if run with -m
from .lib.db_utils import ( # Assuming relative import if run with -m
    get_db_connection, 
    get_or_create_game, 
    get_team_id_by_name, 
    find_tournament_id_for_match, 
    bulk_insert_data # This should be the version expecting on_conflict_full_sql_clause
)

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# DEBUG_PROCESS_LIMIT_MATCHES = 20 
DEBUG_PROCESS_LIMIT_MATCHES = None 

def parse_date_from_api(date_str, is_datetime=False):
    """Parses date or datetime string from API, handles '0000-01-01' as None."""
    if date_str and date_str not in ["0000-01-01", "0000-00-00", "0000-00-00 00:00:00"]:
        try:
            if is_datetime:
                if date_str.endswith('Z'): 
                    date_str = date_str[:-1] + "+00:00"
                dt_obj = datetime.fromisoformat(date_str.replace(' ', 'T')) 
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                else:
                    dt_obj = dt_obj.astimezone(timezone.utc)
                return dt_obj
            else: 
                return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(f"Could not parse date/datetime string '{date_str}'. Returning None.")
            return None
    return None

def map_api_match_to_db_series(api_match_data, game_db_id, cursor):
    """
    Maps Liquipedia API /v3/match data to the structure for our MatchSeries table.
    Uses find_tournament_id_for_match and get_team_id_by_name.
    Returns a dictionary for a single MatchSeries entry, or None if critical data is missing, 
    tournament not found, or any opponent team not found in DB.
    """
    raw_match2id = api_match_data.get("match2id")
    if not raw_match2id: 
        logger.warning(f"Skipping match record due to missing 'match2id': {api_match_data}")
        return None
    
    series_lp_matchid = str(raw_match2id).strip() 
    if not series_lp_matchid: 
        logger.warning(f"Skipping match record because 'match2id' is empty after stripping. Raw was: '{raw_match2id}', Data: {api_match_data}")
        return None

    series_date_str = api_match_data.get("date")
    db_series_date = parse_date_from_api(series_date_str, is_datetime=True)
    if not db_series_date:
        logger.warning(f"Skipping match '{series_lp_matchid}' due to missing or invalid series_date: {series_date_str}")
        return None
    
    match_date_obj_for_tournament_lookup = db_series_date.date() 

    tournament_api_identifier_raw = api_match_data.get("tournament") 
    db_tournament_id = find_tournament_id_for_match(
        cursor,
        tournament_api_name_raw=tournament_api_identifier_raw,
        game_id=game_db_id,
        match_date_obj=match_date_obj_for_tournament_lookup
    )
    
    if not db_tournament_id:
        logger.warning(f"Could not find a valid tournament in DB for match '{series_lp_matchid}' with tournament identifier '{tournament_api_identifier_raw}' and match date '{match_date_obj_for_tournament_lookup}'. This match series will be SKIPPED.")
        return None

    match_opponents = api_match_data.get("match2opponents")
    team1_id, team2_id = None, None
    team1_series_score, team2_series_score = 0, 0 

    if isinstance(match_opponents, list) and len(match_opponents) >= 2:
        for i, opp_data in enumerate(match_opponents[:2]): 
            if not isinstance(opp_data, dict):
                logger.warning(f"Opponent data {i+1} is not a dict in match {series_lp_matchid}. Data: {opp_data}")
                continue

            opp_name_raw = opp_data.get("name") or opp_data.get("pagename") or opp_data.get("id")
            opp_name_for_db = None
            if isinstance(opp_name_raw, str):
                opp_name_for_db = opp_name_raw.replace('_', ' ').strip()
            elif opp_name_raw is not None: 
                opp_name_for_db = str(opp_name_raw)
            
            if not opp_name_for_db:
                logger.warning(f"Opponent {i+1} name/identifier missing in match {series_lp_matchid}")
                continue

            current_team_id = get_team_id_by_name(cursor, team_name=opp_name_for_db, game_id=game_db_id)
            if not current_team_id:
                logger.info(f"Opponent team '{opp_name_for_db}' not found in DB for match '{series_lp_matchid}'. Skipping this match series.")
                return None 

            current_team_score_val = opp_data.get("score")
            current_score = 0 
            if isinstance(current_team_score_val, (int, float)):
                 current_score = int(current_team_score_val)
            elif isinstance(current_team_score_val, str) and (current_team_score_val.lstrip('-').isdigit()):
                current_score = int(current_team_score_val)
            elif current_team_score_val is not None and current_team_score_val != '': 
                logger.warning(f"Unparseable series score '{current_team_score_val}' for opponent {i+1} in match {series_lp_matchid}. Defaulting to 0.")
            
            if i == 0:
                team1_id = current_team_id
                team1_series_score = current_score
            else:
                team2_id = current_team_id
                team2_series_score = current_score
    else:
        logger.warning(f"Match '{series_lp_matchid}' does not have at least 2 opponents in match2opponents: {match_opponents}")
        return None 

    if not team1_id or not team2_id: 
        logger.warning(f"Could not resolve both team IDs from DB for match '{series_lp_matchid}'. Team1_ID attempt: {team1_id}, Team2_ID attempt: {team2_id}. Skipping.")
        return None

    series_winner_team_id = None
    api_winner_indicator = str(api_match_data.get("winner","")).strip() 
    if api_winner_indicator == "1" and team1_id:
        series_winner_team_id = team1_id
    elif api_winner_indicator == "2" and team2_id:
        series_winner_team_id = team2_id
    elif api_winner_indicator not in ["", "0", "draw", "tbd", None, "TBD", "DRAW"]: 
        logger.warning(f"Unexpected winner indicator '{api_winner_indicator}' for match {series_lp_matchid}. Winner not set.")

    db_best_of = api_match_data.get("bestof")
    if db_best_of is None: 
        logger.warning(f"Match '{series_lp_matchid}' missing 'bestof' value. Skipping as it's NOT NULL in schema.")
        return None 
    try:
        db_best_of = int(db_best_of)
    except (ValueError, TypeError):
        logger.warning(f"Match '{series_lp_matchid}' has invalid 'bestof' value '{api_match_data.get('bestof')}'. Skipping.")
        return None

    api_walkover = str(api_match_data.get("walkover","")).strip()
    db_is_forfeit = True if api_walkover in ["1", "2"] else False
    db_tier = api_match_data.get("liquipediatier")

    return {
        "series_lp_matchid": series_lp_matchid,
        "tournament_id": db_tournament_id,
        "game_id": game_db_id, 
        "series_date": db_series_date,
        "team1_id": team1_id,
        "team2_id": team2_id,
        "team1_series_score": team1_series_score,
        "team2_series_score": team2_series_score,
        "series_winner_team_id": series_winner_team_id,
        "best_of": db_best_of,
        "is_forfeit": db_is_forfeit,
        "tier": db_tier
    }

def fetch_and_store_match_series():
    """
    Fetches finished match series data from Liquipedia API v3 (/v3/match)
    and stores them in the PostgreSQL MatchSeries table.
    Only processes matches where both teams and the tournament are already in the local DB.
    De-duplicates matches by series_lp_matchid before bulk insertion.
    """
    conn = None
    total_series_upserted_all_games = 0
    
    current_datetime_utc = datetime.now(timezone.utc)
    current_datetime_utc_str = current_datetime_utc.strftime("%Y-%m-%d %H:%M:%S")
    
    cs2_epoch_start_date_str = "2024-03-16 00:00:00" 
    valorant_epoch_start_date_str = "2022-01-01 00:00:00"

    try:
        conn = get_db_connection()
        if not conn:
            return

        with conn, conn.cursor() as cursor:
            for game_db_id, game_config in SUPPORTED_GAMES.items():
                liquipedia_wiki_name = game_config.get("liquipedia_wiki")
                game_display_name = game_config.get("name", game_db_id)

                if not liquipedia_wiki_name:
                    logger.warning(f"Liquipedia wiki name for game ID '{game_db_id}' not configured. Skipping.")
                    continue

                logger.info(f"--- Starting to fetch match series for game: {game_display_name} (Wiki: {liquipedia_wiki_name}) ---")
                get_or_create_game(cursor, game_db_id, game_display_name)

                match_api_query_fields = "match2id,tournament,game,date,match2opponents,winner,bestof,walkover,finished,status,liquipediatier,pagename"
                
                conditions_list = [
                    "[[finished::1]]", 
                    f"[[date::<{current_datetime_utc_str}]]" 
                ]
                if game_config.get("liquipedia_wiki") == "counterstrike": 
                    conditions_list.append(f"[[date::>{cs2_epoch_start_date_str}]]")
                    logger.info(f"Applying CS2 specific start date filter for {game_display_name}: matches after {cs2_epoch_start_date_str}.")
                elif game_config.get("liquipedia_wiki") == "valorant": 
                    conditions_list.append(f"[[date::>{valorant_epoch_start_date_str}]]")
                    logger.info(f"Applying Valorant specific start date filter for {game_display_name}: matches after {valorant_epoch_start_date_str}.")
                
                api_conditions = " AND ".join(conditions_list)

                api_params = {
                    'wiki': liquipedia_wiki_name,
                    'query': match_api_query_fields,
                    'conditions': api_conditions,
                    'order': 'date DESC'
                }
                logger.debug(f"Fetching match series with API conditions: {api_conditions}")
                all_matches_api_data = fetch_all_api_v3_data("match", api_params)

                if all_matches_api_data is None:
                    logger.error(f"Failed to fetch match series data for {game_display_name}. Skipping.")
                    continue
                if not all_matches_api_data:
                    logger.info(f"No finished match series found for {game_display_name} with current filters: {api_conditions}")
                    continue

                logger.info(f"Fetched {len(all_matches_api_data)} finished match series for {game_display_name}. Processing and de-duplicating...")
                if DEBUG_PROCESS_LIMIT_MATCHES is not None:
                    logger.info(f"DEBUG MODE: Processing up to {DEBUG_PROCESS_LIMIT_MATCHES} match series.")

                unique_series_payloads = {} 
                processed_count = 0
                skipped_count = 0

                for i, api_match_item in enumerate(all_matches_api_data):
                    if DEBUG_PROCESS_LIMIT_MATCHES is not None and i >= DEBUG_PROCESS_LIMIT_MATCHES:
                        logger.info(f"Reached debug processing limit of {DEBUG_PROCESS_LIMIT_MATCHES} for {game_display_name}.")
                        break
                    
                    processed_count += 1
                    db_series_payload = map_api_match_to_db_series(api_match_item, game_db_id, cursor)
                    
                    if db_series_payload:
                        match_id_key = db_series_payload.get("series_lp_matchid")
                        if match_id_key: 
                            if match_id_key not in unique_series_payloads:
                                unique_series_payloads[match_id_key] = db_series_payload
                            else:
                                logger.warning(f"Duplicate series_lp_matchid '{match_id_key}' encountered in API data for game '{game_display_name}'. Keeping first instance processed.")
                        else:
                            logger.warning(f"Mapped payload resulted in empty series_lp_matchid. Skipping for de-duplication. Original API item: {api_match_item}")
                            skipped_count +=1
                    else:
                        skipped_count +=1
                
                series_to_insert = list(unique_series_payloads.values())
                
                logger.info(f"For {game_display_name}: Processed {processed_count} matches from API. {len(series_to_insert)} unique and valid series for DB insertion. {skipped_count} initially skipped (missing tournament/teams in DB or other mapping issue).")

                if series_to_insert:
                    series_cols = [
                        "series_lp_matchid", "tournament_id", "game_id", "series_date", 
                        "team1_id", "team2_id", "team1_series_score", "team2_series_score",
                        "series_winner_team_id", "best_of", "is_forfeit", "tier"
                    ]
                    # Construct the full ON CONFLICT clause for the bulk_insert_data function
                    # that expects on_conflict_full_sql_clause
                    on_conflict_sql_full_clause = (
                        "ON CONFLICT (series_lp_matchid) DO UPDATE SET "
                        "tournament_id = EXCLUDED.tournament_id, "
                        "game_id = EXCLUDED.game_id, " 
                        "series_date = EXCLUDED.series_date, "
                        "team1_id = EXCLUDED.team1_id, "
                        "team2_id = EXCLUDED.team2_id, "
                        "team1_series_score = EXCLUDED.team1_series_score, "
                        "team2_series_score = EXCLUDED.team2_series_score, "
                        "series_winner_team_id = EXCLUDED.series_winner_team_id, "
                        "best_of = EXCLUDED.best_of, "
                        "is_forfeit = EXCLUDED.is_forfeit, "
                        "tier = EXCLUDED.tier"
                    )
                    bulk_insert_data(conn, "MatchSeries", series_to_insert, series_cols,
                                     on_conflict_full_sql_clause=on_conflict_sql_full_clause) 
                    total_series_upserted_all_games += len(series_to_insert)
                    logger.info(f"Attempted to upsert {len(series_to_insert)} MatchSeries entries for {game_display_name}.")
                
                conn.commit() 
                logger.info(f"Finished processing {processed_count} match series from API for {game_display_name}. Upserted {len(series_to_insert)} unique series.")

        logger.info(f"--- All games processed for match series. Total unique series upserted: {total_series_upserted_all_games}. ---")

    except psycopg2.Error as db_err:
        logger.error(f"Database connection or transaction error in MatchSeries: {db_err}")
        if conn and not conn.closed:
            conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred in fetch_and_store_match_series: {e}")
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
        fetch_and_store_match_series()
