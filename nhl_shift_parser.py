import requests
import pandas as pd
import time

def fetch_shift_data(game_id):
    """Fetch shift data for a specific game"""
    url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"
    
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.json().get('data', [])
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error (attempt {attempt+1}/3): {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                print("Network connection failed. Please check your internet connection.")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching shift data: {e}")
            break
    
    print(f"No shift data found for game {game_id}")
    return None

def main(game_id):
    """Main function to fetch and save shift data"""
    # Get shift data
    shift_data = fetch_shift_data(game_id)
    if shift_data is None:
        print(f"Failed to get shift data for game {game_id}")
        return
    
    # Convert to DataFrame
    shifts_df = pd.DataFrame(shift_data)
    
    # Save the results
    output_file = f"game_{game_id}_shifts.csv"
    shifts_df.to_csv(output_file, index=False)
    print(f"Saved shift data to {output_file}")
    
    # Print summary
    print(f"Processed {len(shifts_df)} shift records for game {game_id}")
    print(f"Players: {shifts_df['playerId'].nunique()}")
    print(f"Teams: {shifts_df['teamAbbrev'].unique()}")

if __name__ == "__main__":
    # Example usage
    game_id = 2023020001
    main(game_id)