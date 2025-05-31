import pandas as pd
import json

def create_player_dictionary_from_shifts(shifts_csv_file, output_format='json'):
    """
    Create a player ID to name dictionary from shift data
    
    Args:
        shifts_csv_file (str): Path to the shifts CSV file
        output_format (str): 'json', 'csv', or 'both'
    
    Returns:
        dict: Player ID to name mapping
    """
    
    # Read the shifts data
    print(f"Reading shift data from {shifts_csv_file}...")
    shifts_df = pd.read_csv(shifts_csv_file)
    
    # Extract unique players
    # Group by playerId to get unique players and their info
    unique_players = shifts_df.groupby('playerId').agg({
        'firstName': 'first',
        'lastName': 'first', 
        'teamAbbrev': 'first',
        'teamName': 'first'
    }).reset_index()
    
    # Create the player dictionary
    player_dict = {}
    
    for _, player in unique_players.iterrows():
        player_id = int(player['playerId'])
        full_name = f"{player['firstName']} {player['lastName']}"
        
        player_dict[player_id] = {
            'fullName': full_name,
            'firstName': player['firstName'],
            'lastName': player['lastName'],
            'team': player['teamAbbrev'],
            'teamName': player['teamName']
        }
    
    print(f"Found {len(player_dict)} unique players")
    
    # Save in requested format(s)
    if output_format in ['json', 'both']:
        json_file = 'player_dictionary.json'
        with open(json_file, 'w') as f:
            json.dump(player_dict, f, indent=2)
        print(f"Saved player dictionary to {json_file}")
    
    if output_format in ['csv', 'both']:
        csv_file = 'player_dictionary.csv'
        # Convert to DataFrame for CSV export
        players_list = []
        for player_id, info in player_dict.items():
            players_list.append({
                'playerId': player_id,
                'fullName': info['fullName'],
                'firstName': info['firstName'],
                'lastName': info['lastName'],
                'team': info['team'],
                'teamName': info['teamName']
            })
        
        players_df = pd.DataFrame(players_list)
        players_df.to_csv(csv_file, index=False)
        print(f"Saved player dictionary to {csv_file}")
    
    return player_dict

def load_player_dictionary(file_path='player_dictionary.json'):
    """
    Load the player dictionary from a JSON file
    
    Args:
        file_path (str): Path to the JSON file
    
    Returns:
        dict: Player ID to name mapping
    """
    with open(file_path, 'r') as f:
        # Convert string keys back to integers
        player_dict = {int(k): v for k, v in json.load(f).items()}
    return player_dict

def get_player_name(player_id, player_dict):
    """
    Get player name from ID using the dictionary
    
    Args:
        player_id (int): NHL player ID
        player_dict (dict): Player dictionary
    
    Returns:
        str: Player full name or 'Unknown Player' if not found
    """
    return player_dict.get(int(player_id), {}).get('fullName', 'Unknown Player')

def demo_usage():
    """Demonstrate how to use the player dictionary"""
    
    # Create the dictionary
    player_dict = create_player_dictionary_from_shifts('game_2023020001_shifts.csv', 'both')
    
    # Show some examples
    print("\n--- Sample Players ---")
    sample_ids = list(player_dict.keys())[:5]
    for player_id in sample_ids:
        info = player_dict[player_id]
        print(f"ID {player_id}: {info['fullName']} ({info['team']})")
    
    # Demonstrate lookup function
    print("\n--- Lookup Examples ---")
    test_ids = [8474151, 8474564, 8475158]  # Some IDs from the data
    for test_id in test_ids:
        name = get_player_name(test_id, player_dict)
        print(f"Player ID {test_id}: {name}")

if __name__ == "__main__":
    demo_usage()
