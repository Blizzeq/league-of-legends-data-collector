# League of Legends Data Collection Script

An asynchronous Python script to collect League of Legends match data from the Riot API, process it, and save to CSV files in incremental chunks. It starts from a given **initial PUUID** and expands through newly encountered players’ PUUIDs to gather a broad range of match information.

## Table of Contents

-   [Features](#features)
-   [Prerequisites](#prerequisites)
-   [Getting Started](#getting-started)
    -   [Installation](#installation)
    -   [Configuration](#configuration)
    -   [Usage](#usage)
-   [How It Works](#how-it-works)
    -   [Step-by-Step Script Explanation](#step-by-step-script-explanation)
-   [License](#license)

----------

## Features

-   **Asynchronous requests** using `aiohttp`, allowing faster data collection.
-   **Automated rate-limiting** with `aiolimiter` to avoid hitting Riot API’s 429 (rate limit) errors excessively.
-   **Retrieves detailed match info**: participant stats, champion mastery, rank info (Solo/Duo and Flex), timeline-based final champion stats, etc.
-   **Chunked CSV output**: automatically splits data into files every _CHUNK_SIZE_ rows, keeping only the newest cumulative file.

----------

## Prerequisites

-   Python **3.7+** (recommended 3.9+)
-   A **Riot Games API key** from [Riot’s Developer Portal](https://developer.riotgames.com/)
-   Basic familiarity with Python & command-line usage.

----------

## Getting Started

### Installation

1.  **Clone** this repository:
    
    `git clone https://github.com/YourUsername/YourRepoName.git` 
    
2.  **Install required libraries**:
    
    `pip install aiohttp aiolimiter` 
    

### Configuration

Open the script file and locate the **Configuration** section at the top (labeled `# 1. CONFIGURATION - EDIT THESE VALUES`):

`RIOT_API_KEY = "YOUR_RIOT_API_KEY_HERE"
MATCH_REGION_BASE_URL = "https://europe.api.riotgames.com"
BASE_DOMAIN = "eun1.api.riotgames.com"

CHUNK_SIZE = 50
MAX_ROWS = 200000
MATCH_HISTORY_COUNT = 20
INITIAL_PUUID = "EXAMPLE_PUUID_HERE"` 

-   **RIOT_API_KEY**: Insert your Riot API key (do **not** commit or share it publicly!).
-   **MATCH_REGION_BASE_URL**: URL for your target region (e.g., `"https://americas.api.riotgames.com"`).
-   **BASE_DOMAIN**: Domain for champion mastery or summoner-specific endpoints (e.g., `"na1.api.riotgames.com"`).
-   **CHUNK_SIZE**: Number of rows processed before creating a new CSV file.
-   **MAX_ROWS**: Maximum total rows to collect before stopping.
-   **MATCH_HISTORY_COUNT**: How many recent matches to fetch per PUUID.
-   **INITIAL_PUUID**: The PUUID you want to start with. (Any valid player PUUID; you can get a PUUID using the [Riot Account API](https://developer.riotgames.com/apis#account-v1/GET_getByRiotId) if needed.)

### Usage

Run the script from your command line:

`python your_script_name.py` 

The script will:

1.  Fetch match IDs for the `INITIAL_PUUID`.
2.  Process each match, collecting participant data.
3.  Save data to CSV files in chunks (`new_league_data_{x}.csv`).
4.  Stop once `MAX_ROWS` has been reached or no more matches are found.

----------

## How It Works

### Step-by-Step Script Explanation

1.  **Imports and Global Variables**:
    
    -   The script imports `asyncio`, `datetime`, `csv`, `os`, plus `ClientSession` from `aiohttp` and `AsyncLimiter` from `aiolimiter`.
    -   Defines constants in the `CONFIGURATION` section (e.g., `RIOT_API_KEY`, `MATCH_REGION_BASE_URL`, etc.).
2.  **Caching**:
    
    `match_details_cache = {}
    match_timeline_cache = {}
    summoner_rank_cache = {}
    champion_mastery_cache = {}` 
    
    -   These dictionaries store previously fetched data (like match details, timelines, summoner rank info, and champion mastery) to avoid re-fetching.
3.  **`do_request` Function**:
    
    `async def do_request(session, url, method="GET", ...):
        ...` 
    
    -   Handles **asynchronous HTTP requests** with built-in **rate limiting** (`AsyncLimiter`).
    -   Retries upon encountering certain HTTP errors (429, 5xx).
    -   Returns the HTTP response object (or `None` if failed).
4.  **Data Fetching Functions**:
    
    -   **`get_match_history(session, puuid, count)`**: Retrieves a list of match IDs for a given PUUID.
    -   **`get_match_details(session, match_id)`**: Fetches the details (participants, game info, etc.) for a specific match.
    -   **`get_match_timeline(session, match_id)`**: Pulls the timeline data for a match (allows final champion stats).
    -   **`get_summoner_rank(session, summoner_id, platform_id)`**: Gets ranked info (tier, LP, wins/losses) for a summoner.
    -   **`get_champion_mastery(session, puuid, champion_id)`**: Returns champion mastery info for a summoner + champion.
5.  **`get_final_champion_stats`**:
    
    `def get_final_champion_stats(timeline_data, participant_id):
        ...` 
    
    -   Extracts final champion stats (armor, MR, AD, etc.) from the last frame of the timeline data for the participant in question.
6.  **`process_match_data`**:
    
    `async def process_match_data(session, match_data, timeline_data, puuid_pool):
        ...` 
    
    -   Combines data from the match details and timeline.
    -   Pulls summoner rank info and champion mastery.
    -   Adds each participant’s PUUID to the `puuid_pool` so future matches can be fetched.
    -   Returns a **list of row dictionaries**, each row containing stats for one participant.
7.  **CSV Chunks Saving**: `save_chunk_to_csv(all_data, total_rows)`
    
    -   Writes **all accumulated rows** (`all_data`) to `new_league_data_{total_rows}.csv`.
    -   Removes the **previous** chunk file (e.g., `new_league_data_{total_rows - CHUNK_SIZE}.csv`) to only keep the most recent cumulative data.
8.  **`main()`** Function:
    
    -   Initializes:
        
        `puuid_pool = {INITIAL_PUUID}
        processed_matches = set()
        all_data = []
        total_rows = 0
        rows_since_last_save = 0` 
        
    -   Asynchronously opens a `ClientSession`, then loops while `total_rows < MAX_ROWS` and there are PUUIDs left in `puuid_pool`.
    -   For each PUUID:
        1.  Fetches match IDs via `get_match_history()`.
        2.  For each match ID (not already processed):
            -   Fetches match details + timeline.
            -   Processes the data (including champion mastery, rank, final stats).
            -   Accumulates rows in `all_data`, increment `total_rows`.
            -   Once `rows_since_last_save` >= `CHUNK_SIZE`, calls `save_chunk_to_csv(...)`.
    -   After collecting `MAX_ROWS` rows or running out of new data, the script finishes. If there’s an incomplete chunk, it still saves at the end.
9.  **Running the Script**:
    
    -   The script’s entry point checks `if __name__ == "__main__": asyncio.run(main())`.
    -   **Important**: Keep your API key safe and respect rate limits.

----------

## License

This project is licensed under the MIT License - feel free to modify and adapt it for your own use.

----------

## Notes

-   For more information on Riot API endpoints, visit the [Riot Developer Portal](https://developer.riotgames.com/).
-   If you have special rate-limit allowances, adjust `RATE_LIMIT = AsyncLimiter(15, 1.0)` to a different number accordingly.
-   Remember, exposing your API key publicly can cause security risks or exceed your usage limits.

----------

### Enjoy collecting your League of Legends data!
