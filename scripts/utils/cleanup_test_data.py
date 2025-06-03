# scripts/utils/cleanup_test_data.py
import psycopg2
import logging
# Assuming config.py is in the parent directory of 'scripts' (i.e., project root)
# Adjust the import path if your config.py is located elsewhere relative to this script's execution
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, LOG_LEVEL

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# --- Test Data Identifiers ---
# Aligned with the test data created by the db_utils.py __main__ block (lib_db_utils_py_v3_roster_upsert)
TEST_GAME_ID = "csgo" 
TEST_TEAM_NAME = "Test Roster Team" 
TEST_PLAYER_NICKNAMES = ["roster_player1", "roster_player2"] # List of test players
# TEST_TOURNAMENT_NAME = "Test Major Championship" # Not consistently created in latest db_utils test
# TEST_TOURNAMENT_START_DATE = "2025-04-01" 
# -----------------------------

def get_test_entity_id(cursor, query, params, entity_type):
    """Helper to get ID of a test entity."""
    try:
        cursor.execute(query, params)
        result = cursor.fetchone()
        if result:
            logger.info(f"Found test {entity_type} ID: {result[0]} for params: {params}")
            return result[0]
        else:
            logger.info(f"Test {entity_type} not found with params: {params}")
            return None
    except psycopg2.Error as e:
        logger.error(f"Error finding test {entity_type} ID: {e}")
        return None

def cleanup_test_data():
    """Connects to PostgreSQL and deletes specific test data."""
    conn = None
    total_deleted_rosters = 0
    total_deleted_players = 0
    total_deleted_teams = 0
    # total_deleted_tournaments = 0 

    try:
        logger.info(f"Connecting to PostgreSQL database '{DB_NAME}' to cleanup test data...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = False 
        
        with conn.cursor() as cursor:
            logger.info("Starting test data cleanup...")

            # 1. Find ID of the test team
            test_team_id = get_test_entity_id(
                cursor,
                "SELECT team_id FROM Teams WHERE team_name = %s AND game_id = %s;",
                (TEST_TEAM_NAME, TEST_GAME_ID),
                "team"
            )

            # Find IDs of test players
            test_player_ids = []
            for nickname in TEST_PLAYER_NICKNAMES:
                player_id = get_test_entity_id(
                    cursor,
                    "SELECT player_id FROM Players WHERE player_nickname = %s AND game_id = %s;",
                    (nickname, TEST_GAME_ID),
                    f"player ({nickname})"
                )
                if player_id:
                    test_player_ids.append(player_id)
            
            # 2. Delete from TeamRosters 
            # Delete any roster entries involving the test team OR any of the test players
            # This is safer than just specific combinations if tests created more.
            if test_team_id:
                try:
                    logger.info(f"Attempting to delete test TeamRosters entries associated with test team ID: {test_team_id}...")
                    cursor.execute(
                        "DELETE FROM TeamRosters WHERE team_id = %s;",
                        (test_team_id,)
                    )
                    deleted_count = cursor.rowcount
                    total_deleted_rosters += deleted_count
                    logger.info(f"Deleted {deleted_count} TeamRosters entries for test team ID {test_team_id}.")
                except psycopg2.Error as e:
                    logger.error(f"Error deleting TeamRosters entries for test team ID {test_team_id}: {e}")
                    conn.rollback()
                    return 
            
            if test_player_ids:
                try:
                    # Ensure test_player_ids is a tuple for the IN clause
                    player_ids_tuple = tuple(test_player_ids)
                    if len(player_ids_tuple) == 1: # Psycopg2 needs a trailing comma for single-element tuples
                        player_ids_tuple = (test_player_ids[0],)

                    logger.info(f"Attempting to delete test TeamRosters entries associated with test player IDs: {player_ids_tuple}...")
                    cursor.execute(
                        "DELETE FROM TeamRosters WHERE player_id IN %s;", # Note: IN %s requires a tuple of tuples for executemany, or just a tuple for execute
                        (player_ids_tuple,)
                    )
                    deleted_count = cursor.rowcount
                    # This might double-count if an entry was already deleted by team_id, but DELETE is idempotent.
                    # To avoid double counting in log, we might sum unique deletions if needed. For now, this is fine.
                    total_deleted_rosters = max(total_deleted_rosters, deleted_count) # A simple way to avoid huge numbers if both delete same rows
                    logger.info(f"Deleted {deleted_count} TeamRosters entries for test player IDs {player_ids_tuple}.")
                except psycopg2.Error as e:
                    logger.error(f"Error deleting TeamRosters entries for test player IDs {test_player_ids}: {e}")
                    conn.rollback()
                    return

            # 3. Delete from Players
            if test_player_ids:
                try:
                    player_ids_tuple = tuple(test_player_ids)
                    if len(player_ids_tuple) == 1:
                        player_ids_tuple = (test_player_ids[0],)
                    
                    logger.info(f"Attempting to delete test players with IDs: {player_ids_tuple}...")
                    cursor.execute("DELETE FROM Players WHERE player_id IN %s;", (player_ids_tuple,))
                    total_deleted_players = cursor.rowcount
                    logger.info(f"Deleted {total_deleted_players} test player entries.")
                except psycopg2.Error as e:
                    logger.error(f"Error deleting test players: {e}")
                    conn.rollback()
                    return

            # 4. Delete from Teams
            if test_team_id:
                try:
                    logger.info(f"Attempting to delete test team '{TEST_TEAM_NAME}' (ID: {test_team_id})...")
                    cursor.execute("DELETE FROM TournamentPlacements WHERE team_id = %s;", (test_team_id,))
                    logger.info(f"Deleted {cursor.rowcount} TournamentPlacements for test team.")
                    cursor.execute("DELETE FROM Teams WHERE team_id = %s;", (test_team_id,))
                    total_deleted_teams = cursor.rowcount
                    logger.info(f"Deleted {total_deleted_teams} test team entries.")
                except psycopg2.Error as e:
                    logger.error(f"Error deleting test team '{TEST_TEAM_NAME}': {e}")
                    conn.rollback()
                    return
            
            # 5. Tournament cleanup can be re-added if db_utils.py test block creates one again.

            conn.commit()
            logger.info("Test data cleanup process completed.")
            logger.info(f"Summary: Rosters Deleted (approx): {total_deleted_rosters}, Players Deleted: {total_deleted_players}, Teams Deleted: {total_deleted_teams}")

    except psycopg2.Error as e:
        if conn:
            conn.rollback() 
        logger.error(f"Database error during test data cleanup: {e}")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"An unexpected error occurred during cleanup: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed after cleanup.")

if __name__ == "__main__":
    if DB_NAME == "your_esports_db_name" or \
       DB_USER == "your_postgres_user" or \
       DB_PASSWORD == "your_postgres_password":
        logging.critical("CRITICAL: Default database credentials found in config.py. "
                         "Please update them before running this script.")
    else:
        confirm_message = f"Are you sure you want to delete test data ({TEST_TEAM_NAME}, {', '.join(TEST_PLAYER_NICKNAMES)})? [y/N]: "
        confirm = input(confirm_message)
        if confirm.lower() == 'y':
            cleanup_test_data()
        else:
            logger.info("Test data cleanup aborted by user.")