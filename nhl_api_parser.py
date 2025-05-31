import requests
import pandas as pd
import time
import json
import ast

def fetch_nhl_data(endpoint, params=None, max_retries=3):
    """Fetch data from NHL API with retry logic and better error handling"""
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
                # Wait before retrying (exponential backoff)
                time.sleep(2 ** attempt)
            else:
                print("Network connection failed. Please check your internet connection.")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break
    
    return None

def clean_play_by_play_data(df):
    """Clean and flatten nested JSON structures in the DataFrame"""
    # Make a copy to avoid modifying the original
    cleaned_df = df.copy()
    
    # Handle periodDescriptor column - extract nested values
    if 'periodDescriptor' in cleaned_df.columns:
        # Extract values from the dictionaries - simplified version
        cleaned_df['periodNumber'] = cleaned_df['periodDescriptor'].str.get('number')
        cleaned_df['periodType'] = cleaned_df['periodDescriptor'].str.get('periodType')
        
        # Drop the original nested column
        cleaned_df.drop('periodDescriptor', axis=1, inplace=True)
    
    # Extract strength information from situationCode
    if 'situationCode' in cleaned_df.columns:
        # Create new columns for strength situation
        cleaned_df['homeStrength'] = cleaned_df['situationCode'].apply(lambda x: int(str(x)[0]) if pd.notna(x) else None)
        cleaned_df['awayStrength'] = cleaned_df['situationCode'].apply(lambda x: int(str(x)[1]) if pd.notna(x) else None)
        cleaned_df['strengthSituation'] = cleaned_df.apply(
            lambda row: f"{row['homeStrength']}v{row['awayStrength']}" if pd.notna(row['homeStrength']) else None, 
            axis=1
        )
    
    # Handle details column - extract nested values
    if 'details' in cleaned_df.columns:
        # Extract fields using pandas' str accessor - much cleaner
        cleaned_df['xCoord'] = cleaned_df['details'].str.get('xCoord')
        cleaned_df['yCoord'] = cleaned_df['details'].str.get('yCoord')
        cleaned_df['zoneCode'] = cleaned_df['details'].str.get('zoneCode')
        
        # Extract event-specific details
        cleaned_df['shotType'] = cleaned_df['details'].str.get('shotType')
        cleaned_df['scoringPlayerId'] = cleaned_df['details'].str.get('scoringPlayerId')
        
        # Extract main players involved in the event
        # First get the primary playerId (should be present for most events)
        cleaned_df['playerId'] = cleaned_df['details'].str.get('playerId')
        
        # For specific event types, ensure playerId is populated from the appropriate field
        # For shots, use shootingPlayerId if playerId is missing
        mask_shots = (cleaned_df['typeCode'].isin([501, 504])) & (cleaned_df['playerId'].isna())
        cleaned_df.loc[mask_shots, 'playerId'] = cleaned_df.loc[mask_shots, 'details'].str.get('shootingPlayerId')
        
        # For hits, use hittingPlayerId if playerId is missing
        mask_hits = (cleaned_df['typeCode'] == 503) & (cleaned_df['playerId'].isna())
        cleaned_df.loc[mask_hits, 'playerId'] = cleaned_df.loc[mask_hits, 'details'].str.get('hittingPlayerId')
        
        # For faceoffs, use winningPlayerId if playerId is missing
        mask_faceoffs = (cleaned_df['typeCode'] == 502) & (cleaned_df['playerId'].isna())
        cleaned_df.loc[mask_faceoffs, 'playerId'] = cleaned_df.loc[mask_faceoffs, 'details'].str.get('winningPlayerId')
        
        # For penalties, use committedByPlayerId if playerId is missing
        mask_penalties = (cleaned_df['typeCode'] == 508) & (cleaned_df['playerId'].isna())
        cleaned_df.loc[mask_penalties, 'playerId'] = cleaned_df.loc[mask_penalties, 'details'].str.get('committedByPlayerId')
        
        # Now extract all the specific player IDs
        cleaned_df['shootingPlayerId'] = cleaned_df['details'].str.get('shootingPlayerId')
        cleaned_df['goalieInNetId'] = cleaned_df['details'].str.get('goalieInNetId')
        cleaned_df['hittingPlayerId'] = cleaned_df['details'].str.get('hittingPlayerId')
        cleaned_df['hitteePlayerId'] = cleaned_df['details'].str.get('hitteePlayerId')
        cleaned_df['winningPlayerId'] = cleaned_df['details'].str.get('winningPlayerId')
        cleaned_df['losingPlayerId'] = cleaned_df['details'].str.get('losingPlayerId')
        cleaned_df['committedByPlayerId'] = cleaned_df['details'].str.get('committedByPlayerId')
        cleaned_df['drawnByPlayerId'] = cleaned_df['details'].str.get('drawnByPlayerId')
        cleaned_df['penaltySeverity'] = cleaned_df['details'].str.get('severity')
        cleaned_df['penaltyMinutes'] = cleaned_df['details'].str.get('minutes')
        
        # Drop the original nested column
        cleaned_df.drop('details', axis=1, inplace=True)
    
    return cleaned_df

def parse_play_by_play(game_id):
    """Parse and clean play-by-play data for a specific game"""
    data = fetch_nhl_data(f"v1/gamecenter/{game_id}/play-by-play")
    if not data:
        return None
    
    try:
        plays = data.get('plays', [])
        df = pd.DataFrame(plays)
        
        # Clean the data
        cleaned_df = clean_play_by_play_data(df)
        return cleaned_df
    except Exception as e:
        print(f"Error parsing play-by-play data: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Get play-by-play for game ID 2023020001
    pbp_df = parse_play_by_play(2023020001)
    if pbp_df is not None:
        print(f"Successfully parsed {len(pbp_df)} play-by-play events")
        # Save to CSV
        pbp_df.to_csv("game_2023020001_cleaned.csv", index=False)











