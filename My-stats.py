import requests
import pyodbc
from datetime import datetime

api_key = 'a3f031a9-67fe-48b0-8438-260ddf04880e'
api_url = 'https://fortnite-api.com/v2/stats/br/v2'
player_name = None

# Azure SQL Database configuration
connection_string = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:bit-academy.database.windows.net,1433;"
    "Database=BitAcademyDB;"
    "Uid=168551@student.horizoncollege.nl;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
    "Authentication=ActiveDirectoryInteractive"
)

# Establish a connection to the database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

def create_player_stats_table(table_name):
    # Commit changes before dropping the table
    conn.commit()

    # Drop the table if it exists
    drop_table_query = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    cursor.execute(drop_table_query)

    # Create the new table
    create_table_query = f'''
CREATE TABLE {table_name} (
    Id INT PRIMARY KEY IDENTITY(1,1),
    PlayerName NVARCHAR(255),
    Wins INT,
    Kills INT,
    KillsPerMatch FLOAT,
    Deaths INT,
    KD FLOAT,
    Matches INT,
    WinRate FLOAT,
    MinutesPlayed INT,
    Timestamp DATETIME
)
'''

    cursor.execute(create_table_query)
    conn.commit()

def insert_player_stats(player_name, stats, game_mode):
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Check if all values are 0 (excluding 'wins'), skip insertion if true
    if all(value == 0 for key, value in stats.items() if key != 'wins'):
        print(f"All non-wins values are 0 for {game_mode} stats. Skipping insertion.")
        return

    table_name = f'[PlayerStats_{game_mode}]'
    create_player_stats_table(table_name)

    query = f"""
        INSERT INTO {table_name} (PlayerName, Wins, Kills, KillsPerMatch, Deaths, KD, Matches, WinRate, MinutesPlayed, Timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.execute(query, (
        player_name,
        stats.get('wins', 0),
        stats.get('kills', 0),
        stats.get('killsPerMatch', 0),
        stats.get('deaths', 0),
        stats.get('kd', 0),
        stats.get('matches', 0),
        stats.get('winRate', 0),
        stats.get('minutesPlayed', 0),
        current_datetime
    ))

    conn.commit()

def get_and_save_fortnite_player_stats():
    player_name = input('Voer Fortnite-gebruikersnaam in: ')
    player_stats = get_player_stats(player_name)

    if player_stats:
        overall_stats, solo_stats, duo_stats, squad_stats = player_stats

        game_modes = ['Overall', 'Solo', 'Duo', 'Squad']
        all_stats = [overall_stats, solo_stats, duo_stats, squad_stats]

        for game_mode, stats in zip(game_modes, all_stats):
            if stats:
                insert_player_stats(player_name, stats, game_mode)
                print(f"{game_mode} stats voor {player_name} zijn toegevoegd aan de database.")
            else:
                print(f"{game_mode} stats niet beschikbaar voor {player_name}.")

    else:
        print("Spelerinformatie niet beschikbaar.")

def get_player_stats(player_name):
    headers = {'Authorization': api_key}
    params = {'name': player_name}
    response = requests.get(api_url, headers=headers, params=params)

    if response.status_code == 200:
        return (
            response.json().get('data', {}).get('stats', {}).get('all', {}).get('overall', {}),
            response.json().get('data', {}).get('stats', {}).get('all', {}).get('solo', {}),
            response.json().get('data', {}).get('stats', {}).get('all', {}).get('duo', {}),
            response.json().get('data', {}).get('stats', {}).get('all', {}).get('squad', {})
        )
    else:
        print(f"Fout bij het aanroepen van de API. Statuscode: {response.status_code}")
        print(response.text)
        return None

if __name__ == "__main__":
    get_and_save_fortnite_player_stats()
