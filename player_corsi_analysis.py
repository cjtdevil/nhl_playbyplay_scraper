import pandas as pd
import json
import ast
from create_player_dictionary import load_player_dictionary, get_player_name

def analyze_player_corsi_5v5(pbp_with_players_file, player_dict_file='player_dictionary.json'):
    """
    Analyze 5v5 Corsi statistics for all players

    Args:
        pbp_with_players_file (str): Path to play-by-play CSV with players on ice
        player_dict_file (str): Path to player dictionary JSON

    Returns:
        pd.DataFrame: Player Corsi statistics
    """

    # Load player dictionary
    print("Loading player dictionary...")
    player_dict = load_player_dictionary(player_dict_file)

    # Load play-by-play data
    print(f"Loading play-by-play data from {pbp_with_players_file}...")
    pbp_df = pd.read_csv(pbp_with_players_file)

    # Define shot attempt events for Corsi calculation
    # 505 = goal, 506 = shot-on-goal, 507 = missed-shot, 508 = blocked-shot
    shot_attempt_events = [505, 506, 507, 508]

    # Filter for TRUE 5v5 situations (even strength with goalies) and shot attempts only
    print("Filtering for 5v5 shot attempts...")

    # True 5v5 means situationCode 1551 (1 goalie + 5 skaters vs 1 goalie + 5 skaters)
    # This excludes empty net situations, power plays, etc.
    corsi_events = pbp_df[
        (pbp_df['typeCode'].isin(shot_attempt_events)) &
        (pbp_df['situationCode'] == 1551) &  # True 5v5 even strength
        (pbp_df['homePlayersCount'] == 6) &  # Double check: 5 skaters + 1 goalie
        (pbp_df['awayPlayersCount'] == 6)    # Double check: 5 skaters + 1 goalie
    ].copy()

    print(f"Found {len(corsi_events)} shot attempts in 5v5 situations")

    # Initialize player stats dictionary
    player_stats = {}

    # Process each shot attempt event
    for idx, event in corsi_events.iterrows():
        # Determine which team took the shot
        shooting_team = None

        # Check if we have a shooting player ID to determine team
        if pd.notna(event.get('shootingPlayerId')):
            shooter_id = int(event['shootingPlayerId'])
            # Find which team the shooter belongs to by checking players on ice
            home_players = ast.literal_eval(event['homePlayersOnIce']) if isinstance(event['homePlayersOnIce'], str) else event['homePlayersOnIce']
            away_players = ast.literal_eval(event['awayPlayersOnIce']) if isinstance(event['awayPlayersOnIce'], str) else event['awayPlayersOnIce']

            if shooter_id in home_players:
                shooting_team = 'home'
            elif shooter_id in away_players:
                shooting_team = 'away'

        # If we couldn't determine from shooter, skip this event
        if shooting_team is None:
            continue

        # Process home team players
        for player_id in home_players:
            if player_id not in player_stats:
                player_stats[player_id] = {
                    'CF': 0,  # Corsi For
                    'CA': 0,  # Corsi Against
                    'TOI_events': 0  # Time on ice (approximated by number of events)
                }

            # If home team took the shot, it's CF for home players, CA for away players
            if shooting_team == 'home':
                player_stats[player_id]['CF'] += 1
            else:
                player_stats[player_id]['CA'] += 1

            player_stats[player_id]['TOI_events'] += 1

        # Process away team players
        for player_id in away_players:
            if player_id not in player_stats:
                player_stats[player_id] = {
                    'CF': 0,
                    'CA': 0,
                    'TOI_events': 0
                }

            # If away team took the shot, it's CF for away players, CA for home players
            if shooting_team == 'away':
                player_stats[player_id]['CF'] += 1
            else:
                player_stats[player_id]['CA'] += 1

            player_stats[player_id]['TOI_events'] += 1

    # Convert to DataFrame and calculate additional metrics
    print("Calculating Corsi metrics...")
    results = []

    for player_id, stats in player_stats.items():
        cf = stats['CF']
        ca = stats['CA']
        total_attempts = cf + ca

        # Calculate Corsi percentage
        corsi_pct = (cf / total_attempts * 100) if total_attempts > 0 else 0

        # Calculate relative Corsi (difference from team average)
        # We'll calculate this after we have all player data

        results.append({
            'playerId': player_id,
            'playerName': get_player_name(player_id, player_dict),
            'CF': cf,
            'CA': ca,
            'CF_plus_CA': total_attempts,
            'Corsi_pct': round(corsi_pct, 1),
            'TOI_events': stats['TOI_events']
        })

    # Create DataFrame
    corsi_df = pd.DataFrame(results)

    # Sort by Corsi percentage (descending)
    corsi_df = corsi_df.sort_values('Corsi_pct', ascending=False)

    # Add team information
    print("Adding team information...")
    team_info = {}
    for player_id, info in player_dict.items():
        team_info[player_id] = info['team']

    corsi_df['team'] = corsi_df['playerId'].map(team_info)

    # Calculate team averages for relative Corsi
    team_averages = corsi_df.groupby('team')['Corsi_pct'].mean()
    corsi_df['team_avg_corsi'] = corsi_df['team'].map(team_averages)
    corsi_df['Corsi_rel'] = round(corsi_df['Corsi_pct'] - corsi_df['team_avg_corsi'], 1)

    # Reorder columns
    corsi_df = corsi_df[['playerId', 'playerName', 'team', 'CF', 'CA', 'CF_plus_CA',
                        'Corsi_pct', 'Corsi_rel', 'TOI_events']]

    return corsi_df

def print_corsi_summary(corsi_df):
    """Print a nice summary of Corsi results"""

    print("\n" + "="*80)
    print("🏒 5v5 CORSI ANALYSIS SUMMARY")
    print("="*80)

    print(f"\nTotal players analyzed: {len(corsi_df)}")
    print(f"Teams: {', '.join(corsi_df['team'].unique())}")

    print("\n📊 TOP 10 PLAYERS BY CORSI%:")
    print("-" * 80)
    top_10 = corsi_df.head(10)
    for idx, player in top_10.iterrows():
        print(f"{player['playerName']:20} ({player['team']}) | "
              f"CF: {player['CF']:2d} | CA: {player['CA']:2d} | "
              f"Corsi%: {player['Corsi_pct']:5.1f}% | "
              f"Rel: {player['Corsi_rel']:+5.1f}")

    print("\n📉 BOTTOM 5 PLAYERS BY CORSI%:")
    print("-" * 80)
    bottom_5 = corsi_df.tail(5)
    for idx, player in bottom_5.iterrows():
        print(f"{player['playerName']:20} ({player['team']}) | "
              f"CF: {player['CF']:2d} | CA: {player['CA']:2d} | "
              f"Corsi%: {player['Corsi_pct']:5.1f}% | "
              f"Rel: {player['Corsi_rel']:+5.1f}")

    print("\n🏒 TEAM AVERAGES:")
    print("-" * 40)
    team_stats = corsi_df.groupby('team').agg({
        'CF': 'sum',
        'CA': 'sum',
        'Corsi_pct': 'mean'
    }).round(1)

    for team, stats in team_stats.iterrows():
        total_attempts = stats['CF'] + stats['CA']
        team_corsi_pct = (stats['CF'] / total_attempts * 100) if total_attempts > 0 else 0
        print(f"{team}: {team_corsi_pct:.1f}% Corsi (CF: {stats['CF']:.0f}, CA: {stats['CA']:.0f})")

def save_corsi_results(corsi_df, output_file='player_corsi_5v5.csv'):
    """Save Corsi results to CSV"""
    corsi_df.to_csv(output_file, index=False)
    print(f"\n💾 Corsi results saved to {output_file}")

def main():
    """Main function to run Corsi analysis"""

    # Run the analysis
    corsi_df = analyze_player_corsi_5v5('game_2023020001_with_players.csv')

    # Print summary
    print_corsi_summary(corsi_df)

    # Save results
    save_corsi_results(corsi_df)

    return corsi_df

if __name__ == "__main__":
    corsi_results = main()
