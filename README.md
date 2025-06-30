# ARES
Analytics &amp; Rankings for Esports

## Table of Contents
- [Introduction](#introduction)
- [Data Description](#data-description)
- [Data Files](#data-files)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Results](#results)
- [Contributing](#contributing)
- [License](#license)

## Introduction
ARES is a project that builds a dataset for deep esports analysis by fetching and structuring match data and other key statistics from Liquipedia.

## Data Description
This project dynamically fetches data from the Liquipedia API and populates a PostgreSQL database. The collected data encompasses multiple facets of the esports ecosystem for supported games like Counter-Strike 2 and Valorant.

The dataset includes:
- Game Information: Basic details of the esports titles being tracked.
- Team Details: Information on active and disbanded teams, including their name, region, and location.
- Player Information: Data on professional players, including nicknames, nationality, and roles.
- Tournament Data: Comprehensive details about tournaments, such as name, tier, prize pool, dates, and region.
- Match Results: Records of match series, including scores, teams involved, winner, and format (e.g., best-of-three).
- Roster Information: Active player rosters for each team, detailing join dates and roles.

## Data Files
The project is composed of several scripts that generate specific sets of data. The core utilities and data fetching modules are executed as follows:
- `py -m scripts.lib.api_utils` : A library module for handling Liquipedia API v3 interactions. 
- `py -m scripts.lib.db_utils` : A library module for managing PostgreSQL database interactions. 
- `py -m scripts.00_setup_database` : Initializes the database and creates the necessary tables based on the schema. 
- `py -m scripts.01_fetch_games` : Fetches and populates the list of supported games. 
- `py -m scripts.02_fetch_teams` : Fetches and stores data for active teams. 
- `py -m scripts.03_fetch_players` : Fetches and stores information about active players. 
- `py -m scripts.04_fetch_tournaments` : Fetches and stores details for tournaments. 
- `py -m scripts.05_fetch_team_rosters` : Fetches and stores active team rosters.
- `py -m scripts.06_fetch_match_series` : Fetches and stores data for completed match series.

## Features
- Automated Data Fetching: Scripts to pull data from the Liquipedia API for various esports titles.
- Modular Scripting: Each script is responsible for a specific data entity (e.g., teams, players, tournaments), allowing for independent execution and easy maintenance.
- Data Integrity: The scripts are designed to handle data consistently, for instance by fetching only active players or teams.
- Configuration Management: A centralized config.py file for easy management of API keys and supported games.
- Comprehensive Data Scope: The project covers games, teams, players, tournaments, rosters, and match history.

## Installation
### Requirements
- Python +3.11
- Pgadmin4 9.4-x64
### Packages
- `psycopg2`
- `requests`
- And others as listed in `requirements.txt`.

### Step 1: Clone the Repository
```bash
git clone https://github.com/tomycabre/ARES.git
cd ARES
```
### Step 2: Virtual Environment Setup
#### Windows
```bash
py -m venv venv
```
#### macOS/Linux
```bash
python3 -m venv venv
```

### Step 3: Install Required Packages
```bash
pip install -r requirements.txt
```

### Step 4: Configure the Application
```bash
1. Rename config-template.py to config.py.
```
```bash
2. Edit config.py to add your Liquipedia API key and, if using a database, your credentials.
```

## Usage
Ensure your PostgreSQL server is running and the database specified in `config.py` has been created.
### Run the Main Pipeline:
The `main_pipeline.py` orchestrates the execution of all data-fetching scripts in the correct order.
```bash
python -m scripts.main_pipeline
```
### Process Overview:
- Initialization: The process begins with setting up the database tables using the `00_setup_database.py` script.
- Execution: The main pipeline runs each numbered script sequentially, from fetching games (`01_fetch_games.py`) to player stats (`09_fetch_player_map_stats.py`).
- Data Cleaning: The `05.1_clean_disbanded_teams.sql` script can be run manually to clean out teams that no longer have active rosters, ensuring data relevance.

## Contributing
We welcome contributions! Please open issues or submit pull requests.
### How to Contribute
1. Fork the Repository:
   - Click the 'Fork' button on the repository page.
2. Clone Your Fork:
```bash
git clone https://github.com/your-username/ARES.git
cd ARES
```
3. Create a New Branch:
```bash
git checkout -b feature/your_feature_name
```
4. Make Changes:
   - Implement your feature or fix.
5. Commit Changes:
```bash
git commit -am 'Add new feature'
```
6. Push to Your Fork:
```bash
git push origin feature/your_feature_name
```
7. Submit a Pull Request:
   - Go to the original repository and create a new pull request.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.





