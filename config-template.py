# config.py
# !!! RENAME THIS FILE TO config.py IN ORDER TO WORK IN THE WORKFLOW !!!

# ! DO NOT UPLOAD config-template.py or config.py TO ANY REPOSITORY, THIS WILL LEAK YOUR PRIVATE API, EMAIL, IP (if not using localhost), AND DATABASE LOGIN INFORMATION. 
# ! Project ARES is not responsible if your data was leaked by this project, you will be the one responsible for making config-template.py or config.py private before uploading it to any website.

# --- Liquipedia API v3 Configuration ---
# !!! REPLACE WITH YOUR ACTUAL API KEY !!!
API_KEY = "YOUR_LIQUIPEDIA_API_KEY"

# !!! REPLACE WITH YOUR ACTUAL EMAIL OR A CONTACT METHOD !!!
CONTACT_EMAIL = "your_email@example.com" 

# Liquipedia API v3 base URL
API_V3_BASE_URL = "https://api.liquipedia.net/api/v3/"

# User-Agent string
USER_AGENT = f"EsportsDataProject/0.2 ({CONTACT_EMAIL}) PythonPsycopg2_APIV3"

# Delay between API requests in seconds.
# Liquipedia API Terms of Use should be checked for the most current rate limits.
# Limit of 60 requests per hour (3600 seconds / 60 requests = 60 seconds/request).
API_REQUEST_DELAY = 60.0 # 60 seconds delay between requests

# --- PostgreSQL Database Configuration ---
# !!! REPLACE WITH YOUR ACTUAL POSTGRESQL CONNECTION DETAILS !!!
DB_NAME = "your_esports_db_name"
DB_USER = "your_postgres_user"
DB_PASSWORD = "your_postgres_password"
DB_HOST = "localhost"  # Or your DB host IP/address
DB_PORT = "5432"       # Default PostgreSQL port

# ! DO NOT UPLOAD config-template.py or config.py TO ANY REPOSITORY, THIS WILL LEAK YOUR PRIVATE API, EMAIL, IP (if not using localhost), AND DATABASE LOGIN INFORMATION. 
# ! Project ARES is not responsible if your data was leaked by this project, you will be the one responsible for making config-template.py or config.py private before uploading it to any website.

# --- Game Configuration ---
# Define the games you want to track and their Liquipedia wiki names for the API.
# The key is the game_id you'll use in your DB.
SUPPORTED_GAMES = {
    "cs2": {"name": "Counter-Strike 2", "liquipedia_wiki": "counterstrike"},
    "valorant": {"name": "Valorant", "liquipedia_wiki": "valorant"}
    # Add more games as needed, e.g.:
    # "dota2": {"name": "Dota 2", "liquipedia_wiki": "dota2"},
    # "lol": {"name": "League of Legends", "liquipedia_wiki": "leagueoflegends"},
}

# --- Data Fetching Configuration ---
# Number of records to fetch per API call (Liquipedia API v3 max limit is 1000)
API_V3_LIMIT = 1000 # This is the limit for items *per page*, not total requests per hour.

# You can specify a date range for fetching time-sensitive data like matches or tournaments
# Set to None to fetch all available data (be mindful of API usage for very large datasets)
# Format: "YYYY-MM-DD"
# This will be used in the 'conditions' parameter for API calls.
FETCH_DATA_SINCE_DATE = "2023-01-01" # Example: Fetch data from the start of 2023
# FETCH_DATA_SINCE_DATE = None # To fetch all data

# --- Logging Configuration (Simplified) ---
LOG_LEVEL = "INFO" # "DEBUG", "INFO", "WARNING", "ERROR"

# --- Sanity Checks ---
if API_KEY == "YOUR_LIQUIPEDIA_API_KEY":
    print("CRITICAL: Please replace 'YOUR_LIQUIPEDIA_API_KEY' in config.py with your actual API key.")
    print("You can request one from the Liquipedia Discord: discord.gg/liquipedia in #api-feedback.")

if CONTACT_EMAIL == "your_email@example.com":
    print("WARNING: Please update 'CONTACT_EMAIL' in config.py to your actual email or contact method.")

if DB_NAME == "your_esports_db_name" or DB_USER == "your_postgres_user" or DB_PASSWORD == "your_postgres_password":
    print("CRITICAL: Please update your PostgreSQL database connection details (DB_NAME, DB_USER, DB_PASSWORD) in config.py.")


# ! DO NOT UPLOAD config-template.py or config.py TO ANY REPOSITORY, THIS WILL LEAK YOUR PRIVATE API, EMAIL, IP (if not using localhost), AND DATABASE LOGIN INFORMATION. 
# ! Project ARES is not responsible if your data was leaked by this project, you will be the one responsible for making config-template.py or config.py private before uploading it to any website.