# scripts/00_setup_database.py
import psycopg2
import logging
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, LOG_LEVEL

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# The SQL schema (based on postgres_schema_v4_sql - name-focused with surrogate keys)
SCHEMA_SQL = """
-- Table: Games
CREATE TABLE IF NOT EXISTS Games (
    game_id TEXT PRIMARY KEY,
    game_name TEXT NOT NULL
);

-- Table: Teams
CREATE TABLE IF NOT EXISTS Teams (
    team_id SERIAL PRIMARY KEY,
    team_name TEXT NOT NULL,
    game_id TEXT NOT NULL,
    region TEXT,
    location TEXT,
    is_disbanded BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (game_id) REFERENCES Games(game_id),
    UNIQUE (team_name, game_id)
);

-- Table: Players
CREATE TABLE IF NOT EXISTS Players (
    player_id SERIAL PRIMARY KEY,
    player_nickname TEXT NOT NULL,
    birth_date DATE,
    nationality TEXT,
    status TEXT,
    curr_role TEXT, 
    type TEXT, -- For API type e.g. 'Player'
    game_id TEXT NOT NULL,
    FOREIGN KEY (game_id) REFERENCES Games(game_id),
    UNIQUE (player_nickname, game_id)
);

-- Table: Tournaments
CREATE TABLE IF NOT EXISTS Tournaments (
    tournament_id SERIAL PRIMARY KEY,
    tournament_name TEXT NOT NULL,
    game_id TEXT NOT NULL,
    tier TEXT, 
    start_date DATE,
    end_date DATE,
    type TEXT,
    region TEXT,
    location TEXT,
    prize_pool NUMERIC,
    tournament_weight NUMERIC, 
    FOREIGN KEY (game_id) REFERENCES Games(game_id),
    UNIQUE (tournament_name, game_id, start_date)
);

-- Table: TeamRosters
CREATE TABLE IF NOT EXISTS TeamRosters (
    roster_id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    player_nickname TEXT, -- ADDED as per user request
    join_date DATE NOT NULL,
    leave_date DATE,
    is_substitute BOOLEAN DEFAULT FALSE,
    role_during_tenure TEXT,
    status TEXT, -- Status of the player on this specific roster (e.g., active, former)
    FOREIGN KEY (team_id) REFERENCES Teams(team_id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES Players(player_id) ON DELETE CASCADE,
    UNIQUE (team_id, player_id, join_date)
);

-- Table: MatchSeries
CREATE TABLE IF NOT EXISTS MatchSeries (
    series_lp_matchid TEXT PRIMARY KEY, 
    tournament_id INTEGER NOT NULL,
    game_id TEXT NOT NULL,
    series_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    team1_id INTEGER NOT NULL,
    team2_id INTEGER NOT NULL,
    team1_series_score INTEGER NOT NULL,
    team2_series_score INTEGER NOT NULL,
    series_winner_team_id INTEGER, 
    best_of INTEGER,
    is_forfeit BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (tournament_id) REFERENCES Tournaments(tournament_id) ON DELETE CASCADE,
    FOREIGN KEY (game_id) REFERENCES Games(game_id),
    FOREIGN KEY (team1_id) REFERENCES Teams(team_id) ON DELETE RESTRICT,
    FOREIGN KEY (team2_id) REFERENCES Teams(team_id) ON DELETE RESTRICT,
    FOREIGN KEY (series_winner_team_id) REFERENCES Teams(team_id) ON DELETE SET NULL
);

-- Table: PlayedMaps
CREATE TABLE IF NOT EXISTS PlayedMaps (
    map_lp_gameid TEXT PRIMARY KEY, 
    series_lp_matchid TEXT NOT NULL,
    map_name TEXT NOT NULL,
    map_order_in_series INTEGER NOT NULL,
    team1_score INTEGER NOT NULL,
    team2_score INTEGER NOT NULL,
    map_winner_team_id INTEGER,
    team1_side_first_half TEXT,
    team2_side_first_half TEXT,
    FOREIGN KEY (series_lp_matchid) REFERENCES MatchSeries(series_lp_matchid) ON DELETE CASCADE,
    FOREIGN KEY (map_winner_team_id) REFERENCES Teams(team_id) ON DELETE SET NULL,
    UNIQUE(series_lp_matchid, map_order_in_series) 
);

-- Table: PlayerMapStats
CREATE TABLE IF NOT EXISTS PlayerMapStats (
    player_map_stat_id SERIAL PRIMARY KEY,
    map_lp_gameid TEXT NOT NULL, 
    player_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL, 
    kills INTEGER,
    deaths INTEGER,
    assists INTEGER,
    adr_acs NUMERIC,
    headshot_percentage NUMERIC,
    agent_played TEXT,
    rating_1 NUMERIC,
    rating_2 NUMERIC,
    FOREIGN KEY (map_lp_gameid) REFERENCES PlayedMaps(map_lp_gameid) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES Players(player_id) ON DELETE CASCADE,
    FOREIGN KEY (team_id) REFERENCES Teams(team_id) ON DELETE CASCADE
);

-- Table: TournamentPlacements
CREATE TABLE IF NOT EXISTS TournamentPlacements (
    placement_id SERIAL PRIMARY KEY,
    tournament_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    placement_string TEXT NOT NULL,
    numeric_placement_lower INTEGER NOT NULL,
    numeric_placement_upper INTEGER NOT NULL,
    earnings NUMERIC,
    earnings_currency TEXT, 
    FOREIGN KEY (tournament_id) REFERENCES Tournaments(tournament_id) ON DELETE CASCADE,
    FOREIGN KEY (team_id) REFERENCES Teams(team_id) ON DELETE CASCADE,
    UNIQUE (tournament_id, team_id) 
);
"""

def create_tables():
    """Connects to the PostgreSQL database and creates tables based on the schema."""
    conn = None
    try:
        logger.info(f"Connecting to PostgreSQL database '{DB_NAME}' on {DB_HOST}:{DB_PORT}...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = False 
        cursor = conn.cursor()
        
        logger.info("Executing schema to create tables if they do not exist...")
        cursor.execute(SCHEMA_SQL)
        conn.commit()
        logger.info("Database tables checked/created successfully.")
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Error connecting to or setting up PostgreSQL database: {e}")
        logger.error("Please ensure PostgreSQL is running and connection details in config.py are correct.")
        logger.error("Also ensure the database user has CREATETABLE permissions if tables don't exist.")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    if DB_NAME == "your_esports_db_name" or \
       DB_USER == "your_postgres_user" or \
       DB_PASSWORD == "your_postgres_password":
        logging.critical("CRITICAL: Default database credentials found in config.py. "
                         "Please update them before running this script.")
    else:
        create_tables()
