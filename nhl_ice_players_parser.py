import pandas as pd
import numpy as np
import requests
import time

def fetch_nhl_data(endpoint, params=None, max_retries=3):
    """Fetch data from NHL API with retry logic"""
    base_url = "https://api-web.nhle.com"
    url = f"{base_url}/{endpoint}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print("Network connection failed. Please check your internet connection.")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break
    
    return None

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

def parse_play_by_play(game_id):
    """Parse play-by-play data for a specific game"""
    data = fetch_nhl_data(f"v1/gamecenter/{game_id}/play-by-play")
    if not data:
        return None
    
    try:
        plays = data.get('plays', [])
        df = pd.DataFrame(plays)
        
        # Clean the data
        if 'periodDescriptor' in df.columns:
            df['periodNumber'] = df['periodDescriptor'].str.get('number')
            df['periodType'] = df['periodDescriptor'].str.get('periodType')
            df.drop('periodDescriptor', axis=1, inplace=True)
        
        if 'details' in df.columns:
            df['xCoord'] = df['details'].str.get('xCoord')
            df['yCoord'] = df['details'].str.get('yCoord')
            df['zoneCode'] = df['details'].str.get('zoneCode')
            df['shotType'] = df['details'].str.get('shotType')
            df['scoringPlayerId'] = df['details'].str.get('scoringPlayerId')
            df['playerId'] = df['details'].str.get('playerId')
            df['shootingPlayerId'] = df['details'].str.get('shootingPlayerId')
            df['goalieInNetId'] = df['details'].str.get('goalieInNetId')
            df['hittingPlayerId'] = df['details'].str.get('hittingPlayerId')
            df['hitteePlayerId'] = df['details'].str.get('hitteePlayerId')
            df['winningPlayerId'] = df['details'].str.get('winningPlayerId')
            df['losingPlayerId'] = df['details'].str.get('losingPlayerId')
            df.drop('details', axis=1, inplace=True)
        
        return df
    except Exception as e:
        print(f"Error parsing play-by-play data: {e}")
        return None

def convert_time_to_seconds(time_str):
    """Convert MM:SS time format to seconds"""
    if pd.isna(time_str):
        return None
    
    parts = time_str.split(':')
    if len(parts) == 2:
        minutes, seconds = parts
        return int(minutes) * 60 + int(seconds)
    return None

def get_players_on_ice(shifts_df, period, time_in_period, team=None):
    """Get list of players on ice at a specific time in a period"""
    # Convert time to seconds for comparison
    time_seconds = convert_time_to_seconds(time_in_period)
    
    # Filter shifts for the specified period
    period_shifts = shifts_df[shifts_df['period'] == period]
    
    # Filter by team if specified
    if team:
        period_shifts = period_shifts[period_shifts['teamAbbrev'] == team]
    
    # Find players on ice at the specified time
    players_on_ice = []
    
    for _, shift in period_shifts.iterrows():
        # Skip goal events (these are not actual shifts)
        if shift['typeCode'] == 505:
            continue
            
        start_seconds = convert_time_to_seconds(shift['startTime'])
        end_seconds = convert_time_to_seconds(shift['endTime'])
        
        if start_seconds < time_seconds <= end_seconds:
            players_on_ice.append(int(shift['playerId']))
    
    return players_on_ice

def combine_pbp_with_shifts(game_id):
    """Combine play-by-play data with shift data to identify players on ice"""
    # Get play-by-play data
    pbp_df = parse_play_by_play(game_id)
    if pbp_df is None:
        print(f"Failed to get play-by-play data for game {game_id}")
        return None
    
    # Get shift data
    shift_data = fetch_shift_data(game_id)
    if shift_data is None:
        print(f"Failed to get shift data for game {game_id}")
        return None
    
    shifts_df = pd.DataFrame(shift_data)
    
    # Add columns for players on ice
    pbp_df['homePlayersOnIce'] = None
    pbp_df['awayPlayersOnIce'] = None
    pbp_df['homePlayersCount'] = 0
    pbp_df['awayPlayersCount'] = 0
    
    # Get home and away team abbreviations
    home_team = shifts_df['teamAbbrev'].unique()[0]  # Assuming first team is home
    away_team = shifts_df['teamAbbrev'].unique()[1]  # Assuming second team is away
    
    # Process each play-by-play event
    for idx, play in pbp_df.iterrows():
        if pd.isna(play['timeInPeriod']) or pd.isna(play['periodNumber']):
            continue
            
        # Get players on ice for home team
        home_players = get_players_on_ice(shifts_df, play['periodNumber'], play['timeInPeriod'], home_team)
        pbp_df.at[idx, 'homePlayersOnIce'] = str(home_players)
        pbp_df.at[idx, 'homePlayersCount'] = len(home_players)
        
        # Get players on ice for away team
        away_players = get_players_on_ice(shifts_df, play['periodNumber'], play['timeInPeriod'], away_team)
        pbp_df.at[idx, 'awayPlayersOnIce'] = str(away_players)
        pbp_df.at[idx, 'awayPlayersCount'] = len(away_players)
    
    return pbp_df

def main(game_id):
    """Main function to process data and save results"""
    # Combine play-by-play with shifts
    combined_df = combine_pbp_with_shifts(game_id)
    if combined_df is None:
        print(f"Failed to combine data for game {game_id}")
        return
    
    # Save the results
    output_file = f"game_{game_id}_with_players.csv"
    combined_df.to_csv(output_file, index=False)
    print(f"Saved combined data to {output_file}")
    
    # Print summary
    print(f"Processed {len(combined_df)} play-by-play events")
    print(f"Added player on ice data for home and away teams")

if __name__ == "__main__":
    # Example usage
    game_id =  2023020001
    main(game_id)