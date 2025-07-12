import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import logging
from typing import Optional, Dict, List, Any
from airflow.models import Variable
from datetime import datetime


def get_spotify_user_credentials() -> Dict[str, str]:
    """
    Get Spotify user credentials from environment variables or Airflow Variables.
    
    Returns:
        Dict containing Spotify user credentials
        
    Raises:
        ValueError: If credentials are not found
    """
    try:
        # Try to get from Airflow Variables first (more secure in Airflow)
        client_id = Variable.get("SPOTIFY_CLIENT_ID", default_var=None)
        client_secret = Variable.get("SPOTIFY_CLIENT_SECRET", default_var=None)
        redirect_uri = Variable.get("SPOTIFY_REDIRECT_URI", default_var="http://localhost:8080/callback")
        
        # Fallback to environment variables
        if not client_id:
            client_id = os.getenv("SPOTIFY_CLIENT_ID")
        if not client_secret:
            client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        if not redirect_uri:
            redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8080/callback")
        
        if not client_id or not client_secret:
            raise ValueError(
                "Spotify credentials not found. Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET. "
                "Get your credentials from: https://developer.spotify.com/dashboard/applications"
            )
        
        return {
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri
        }
        
    except Exception as e:
        logging.error(f"Error retrieving Spotify credentials: {e}")
        raise


def create_spotify_client(credentials: Dict[str, str], scope: str = 'user-read-recently-played') -> Optional[spotipy.Spotify]:
    """
    Create Spotify client with OAuth authentication.
    
    Args:
        credentials: Dictionary containing Spotify credentials
        scope: OAuth scope for permissions
        
    Returns:
        Configured Spotify client or None if failed
    """
    try:
        auth_manager = SpotifyOAuth(
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
            redirect_uri=credentials["redirect_uri"],
            scope=scope,
            cache_path=".spotify_cache"  # Cache token to avoid re-authentication
        )
        
        sp = spotipy.Spotify(auth_manager=auth_manager)
        
        # Test the connection
        user_info = sp.current_user()
        logging.info(f"Successfully authenticated as: {user_info.get('display_name', 'Unknown')}")
        
        return sp
        
    except Exception as e:
        logging.error(f"Error creating Spotify client: {e}")
        return None


def get_recently_played_tracks(sp: spotipy.Spotify, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Get user's recently played tracks.
    
    Args:
        sp: Spotify client instance
        limit: Number of tracks to retrieve (max 50)
        
    Returns:
        List of track data or None if failed
    """
    try:
        # Ensure limit is within API constraints
        limit = min(max(limit, 1), 50)
        
        # Get recently played tracks
        results = sp.current_user_recently_played(limit=limit)
        
        if not results or not results.get('items'):
            logging.warning("No recently played tracks found")
            return []
        
        # Process track data
        track_data = []
        for idx, item in enumerate(results['items']):
            try:
                track = item['track']
                played_at = item.get('played_at', '')
                
                track_info = {
                    "rank": idx + 1,
                    "track_name": track.get('name', 'Unknown'),
                    "artist_name": track['artists'][0]['name'] if track.get('artists') else 'Unknown',
                    "album_name": track.get('album', {}).get('name', 'Unknown'),
                    "spotify_url": track.get('external_urls', {}).get('spotify', ''),
                    "preview_url": track.get('preview_url', ''),
                    "duration_ms": track.get('duration_ms', 0),
                    "popularity": track.get('popularity', 0),
                    "explicit": track.get('explicit', False),
                    "played_at": played_at,
                    "track_id": track.get('id', ''),
                    "artist_id": track['artists'][0]['id'] if track.get('artists') else '',
                    "album_id": track.get('album', {}).get('id', '')
                }
                
                track_data.append(track_info)
                
            except Exception as e:
                logging.warning(f"Error processing track {idx + 1}: {e}")
                continue
        
        logging.info(f"Successfully processed {len(track_data)} recently played tracks")
        return track_data
        
    except Exception as e:
        logging.error(f"Error getting recently played tracks: {e}")
        return None


def display_tracks(track_data: List[Dict[str, Any]]) -> None:
    """
    Display track information in a formatted way.
    
    Args:
        track_data: List of track dictionaries
    """
    if not track_data:
        print("No tracks to display")
        return
    
    print(f"\n🎵 Your {len(track_data)} Most Recently Played Tracks:\n")
    print("-" * 80)
    
    for track in track_data:
        print(f"#{track['rank']}")
        print(f"🎵 Track: {track['track_name']}")
        print(f"👤 Artist: {track['artist_name']}")
        print(f"💿 Album: {track['album_name']}")
        
        if track['spotify_url']:
            print(f"🔗 URL: {track['spotify_url']}")
        
        if track['played_at']:
            print(f"⏰ Played at: {track['played_at']}")
        
        print(f"⭐ Popularity: {track['popularity']}/100")
        print(f"⏱️  Duration: {track['duration_ms'] // 60000}:{(track['duration_ms'] % 60000) // 1000:02d}")
        
        if track['explicit']:
            print("🚫 Explicit content")
            
        print("-" * 80)


def save_tracks_to_csv(track_data: List[Dict[str, Any]], output_path: str = "recently_played_tracks.csv") -> bool:
    """
    Save track data to CSV file.
    
    Args:
        track_data: List of track dictionaries
        output_path: Output file path
        
    Returns:
        True if successful, False otherwise
    """
    try:
        import pandas as pd
        
        if not track_data:
            logging.warning("No track data to save")
            return False
        
        df = pd.DataFrame(track_data)
        df.to_csv(output_path, index=False)
        
        logging.info(f"Successfully saved {len(track_data)} tracks to {output_path}")
        print(f"\n💾 Data saved to: {output_path}")
        return True
        
    except ImportError:
        logging.warning("pandas not available, cannot save to CSV")
        return False
    except Exception as e:
        logging.error(f"Error saving tracks to CSV: {e}")
        return False


def run_spotify_recently_played_analysis(**context):
    """
    Main function for Spotify recently played tracks analysis.
    
    Args:
        **context: Airflow context (when called from Airflow)
    """
    try:
        logging.info("Starting Spotify recently played tracks analysis")
        
        # Get configuration
        limit = int(Variable.get("SPOTIFY_RECENT_LIMIT", default_var="10"))
        output_path = Variable.get("SPOTIFY_OUTPUT_PATH", default_var="recently_played_tracks.csv")
        display_results = Variable.get("SPOTIFY_DISPLAY_RESULTS", default_var="true").lower() == "true"
        
        # Get credentials and create client
        credentials = get_spotify_user_credentials()
        sp = create_spotify_client(credentials)
        
        if not sp:
            raise Exception("Failed to create Spotify client")
        
        # Get recently played tracks
        track_data = get_recently_played_tracks(sp, limit)
        
        if not track_data:
            logging.warning("No recently played tracks found")
            return {"tracks_processed": 0, "status": "no_data"}
        
        # Display results if requested
        if display_results:
            display_tracks(track_data)
        
        # Save to CSV
        save_success = save_tracks_to_csv(track_data, output_path)
        
        logging.info("Spotify recently played analysis completed successfully")
        
        # Calculate some statistics
        unique_artists = len(set(track['artist_name'] for track in track_data))
        avg_popularity = sum(track['popularity'] for track in track_data) / len(track_data)
        explicit_count = sum(1 for track in track_data if track['explicit'])
        
        # Return summary for XCom
        return {
            "tracks_processed": len(track_data),
            "unique_artists": unique_artists,
            "average_popularity": round(avg_popularity, 2),
            "explicit_tracks": explicit_count,
            "output_path": output_path if save_success else None,
            "execution_date": context.get('ds') if context else str(datetime.now().date()),
            "status": "success"
        }
        
    except Exception as e:
        logging.error(f"Spotify recently played analysis failed: {e}")
        raise


if __name__ == "__main__":
    # For testing purposes
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Run the analysis
        result = run_spotify_recently_played_analysis()
        print(f"\n✅ Analysis completed successfully!")
        print(f"📊 Summary: {result}")
        
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        print("\nMake sure you have:")
        print("1. Set your Spotify credentials (SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)")
        print("2. Configured your Spotify app with the correct redirect URI")
        print("3. Granted the necessary permissions (user-read-recently-played)")
        print("4. Have some recently played tracks in your Spotify account")

