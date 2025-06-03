# scripts/01_fetch_games.py
import psycopg2
import logging
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, SUPPORTED_GAMES, LOG_LEVEL

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

def populate_games_table():
    """Connects to PostgreSQL and populates the Games table from config.SUPPORTED_GAMES."""
    conn = None
    inserted_count = 0
    skipped_count = 0

    if not SUPPORTED_GAMES:
        logger.warning("No games defined in SUPPORTED_GAMES in config.py. Nothing to populate.")
        return

    try:
        logger.info(f"Connecting to PostgreSQL database '{DB_NAME}' to populate Games table...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = False # Use transactions
        
        with conn.cursor() as cursor: # Using 'with' ensures cursor is closed
            logger.info("Populating Games table...")
            for game_db_id, game_config_info in SUPPORTED_GAMES.items():
                game_name_to_insert = game_config_info.get("name") # Get the 'name' from the nested dict
                if not game_name_to_insert:
                    logger.warning(f"Game with id '{game_db_id}' is missing 'name' in its config in SUPPORTED_GAMES. Skipping.")
                    continue
                
                try:
                    # Using INSERT ... ON CONFLICT to avoid duplicates and errors if script is run multiple times
                    # This assumes game_id is the primary key.
                    cursor.execute(
                        """
                        INSERT INTO Games (game_id, game_name)
                        VALUES (%s, %s)
                        ON CONFLICT (game_id) DO NOTHING;
                        """,
                        (game_db_id, game_name_to_insert)
                    )
                    if cursor.rowcount > 0:
                        inserted_count += 1
                        logger.info(f"Inserted game: ID='{game_db_id}', Name='{game_name_to_insert}'")
                    else:
                        skipped_count += 1
                        logger.info(f"Game already exists: ID='{game_db_id}', Name='{game_name_to_insert}'. Skipped.")
                except psycopg2.Error as e:
                    logger.error(f"Error inserting game '{game_name_to_insert}' (ID: {game_db_id}): {e}")
                    conn.rollback() # Rollback this specific transaction part if one game fails
                                    # and re-raise or handle as per overall strategy
                    # For now, we'll log and continue with other games for this script's purpose
            
            conn.commit() # Commit all successful inserts for games
            logger.info(f"Games table population complete. Inserted: {inserted_count}, Skipped (already exist): {skipped_count}.")

    except psycopg2.Error as e:
        if conn: # conn might be None if initial psycopg2.connect fails
            conn.rollback() 
        logger.error(f"Database error during Games table population: {e}")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    # Perform sanity checks from config
    if DB_NAME == "your_esports_db_name" or \
       DB_USER == "your_postgres_user" or \
       DB_PASSWORD == "your_postgres_password":
        logging.critical("CRITICAL: Default database credentials found in config.py. "
                         "Please update them before running this script.")
    else:
        populate_games_table()
