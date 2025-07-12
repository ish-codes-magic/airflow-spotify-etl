import os
import requests
from requests import post, get
import json
import base64
import datetime
import pandas as pd
import logging
from typing import Optional, Dict, List, Any
from airflow.models import Variable


def get_spotify_credentials() -> Dict[str, str]:
    """
    Retrieve Spotify credentials from environment variables or Airflow Variables.
    
    Returns:
        Dict containing client_id and client_secret
        
    Raises:
        ValueError: If credentials are not found
    """
    try:
        # Try to get from Airflow Variables first (more secure in Airflow)
        client_id = Variable.get("SPOTIFY_CLIENT_ID", default_var=None)
        client_secret = Variable.get("SPOTIFY_CLIENT_SECRET", default_var=None)
        
        # Fallback to environment variables
        if not client_id:
            client_id = os.getenv("SPOTIFY_CLIENT_ID")
        if not client_secret:
            client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
            
        if not client_id or not client_secret:
            raise ValueError("Spotify credentials not found. Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET")
            
        return {
            "client_id": client_id,
            "client_secret": client_secret
        }
    except Exception as e:
        logging.error(f"Error retrieving Spotify credentials: {e}")
        raise


def get_spotify_token(client_id: str, client_secret: str) -> Optional[str]:
    """
    Get Spotify access token using client credentials flow.
    
    Args:
        client_id: Spotify client ID
        client_secret: Spotify client secret
        
    Returns:
        Access token string or None if failed
    """
    try:
        auth_string = f"{client_id}:{client_secret}"
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")
        
        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        
        response = post(url, headers=headers, data=data, timeout=30)
        response.raise_for_status()  # Raise exception for bad status codes
        
        json_result = response.json()
        token = json_result.get("access_token")
        
        if not token:
            logging.error("No access token in response")
            return None
            
        logging.info("Successfully obtained Spotify access token")
        return token
        
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP error getting Spotify token: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error getting Spotify token: {e}")
        return None


def get_auth_header(token: str) -> Dict[str, str]:
    """Get authorization header for Spotify API requests."""
    return {"Authorization": f"Bearer {token}"}


def search_for_artist(token: str, artist_name: str) -> Optional[Dict[str, Any]]:
    """
    Search for an artist on Spotify.
    
    Args:
        token: Spotify access token
        artist_name: Name of the artist to search for
        
    Returns:
        Artist data or None if not found
    """
    try:
        # URL encode the artist name to handle special characters
        encoded_name = requests.utils.quote(artist_name)
        url = f"https://api.spotify.com/v1/search?q={encoded_name}&type=artist&limit=1"
        headers = get_auth_header(token)
        
        response = get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        json_result = response.json()
        
        if not json_result.get("artists", {}).get("items"):
            logging.warning(f"No artist found for: {artist_name}")
            return None
            
        artist_data = json_result["artists"]["items"][0]
        logging.info(f"Found artist: {artist_data.get('name')} (ID: {artist_data.get('id')})")
        return artist_data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP error searching for artist {artist_name}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error searching for artist {artist_name}: {e}")
        return None


def get_songs_by_artist(token: str, artist_id: str, market: str = "US") -> List[Dict[str, Any]]:
    """
    Get top tracks for an artist.
    
    Args:
        token: Spotify access token
        artist_id: Spotify artist ID
        market: Market code (default: US)
        
    Returns:
        List of track data
    """
    try:
        url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market={market}"
        headers = get_auth_header(token)
        
        response = get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        json_result = response.json()
        tracks = json_result.get("tracks", [])
        
        if not tracks:
            logging.warning(f"No tracks found for artist ID: {artist_id}")
            return []
            
        logging.info(f"Found {len(tracks)} tracks for artist")
        return tracks
        
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP error getting songs for artist {artist_id}: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error getting songs for artist {artist_id}: {e}")
        return []


def save_to_csv(df: pd.DataFrame, output_path: str) -> bool:
    """
    Save DataFrame to CSV with error handling.
    
    Args:
        df: DataFrame to save
        output_path: Output file path (local or S3)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        if output_path.startswith("s3://"):
            # For S3, you might want to use boto3 for better error handling
            df.to_csv(output_path, index=False)
        else:
            # Local file
            df.to_csv(output_path, index=False)
            
        logging.info(f"Successfully saved {len(df)} records to {output_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving to {output_path}: {e}")
        return False


def run_spotify_etl(**context):
    """
    Main ETL function with proper error handling and configuration.
    
    Args:
        **context: Airflow context (when called from Airflow)
    """
    try:
        # Get configuration from Airflow Variables with defaults
        artist_name = Variable.get("SPOTIFY_ARTIST_NAME", default_var="Ed Sheeran")
        output_bucket = Variable.get("SPOTIFY_OUTPUT_BUCKET", default_var="default-bucket")
        output_path = Variable.get("SPOTIFY_OUTPUT_PATH", default_var="songs.csv")
        market = Variable.get("SPOTIFY_MARKET", default_var="US")
        
        # Construct full output path
        full_output_path = f"s3://{output_bucket}/{output_path}"
        
        logging.info(f"Starting Spotify ETL for artist: {artist_name}")
        
        # Get credentials
        credentials = get_spotify_credentials()
        
        # Get access token
        token = get_spotify_token(credentials["client_id"], credentials["client_secret"])
        if not token:
            raise Exception("Failed to obtain Spotify access token")
        
        # Search for artist
        artist_data = search_for_artist(token, artist_name)
        if not artist_data:
            raise Exception(f"Artist '{artist_name}' not found")
        
        artist_id = artist_data["id"]
        
        # Get top tracks
        tracks = get_songs_by_artist(token, artist_id, market)
        if not tracks:
            raise Exception(f"No tracks found for artist '{artist_name}'")
        
        # Process tracks data
        track_data = []
        for track in tracks:
            try:
                track_info = {
                    "artist_name": track["artists"][0]["name"] if track.get("artists") else "Unknown",
                    "song_name": track.get("name", "Unknown"),
                    "popularity": track.get("popularity", 0),
                    "duration_ms": track.get("duration_ms", 0),
                    "explicit": track.get("explicit", False),
                    "external_url": track.get("external_urls", {}).get("spotify", ""),
                    "preview_url": track.get("preview_url", ""),
                    "album_name": track.get("album", {}).get("name", "Unknown"),
                    "release_date": track.get("album", {}).get("release_date", ""),
                    "track_id": track.get("id", "")
                }
                track_data.append(track_info)
            except Exception as e:
                logging.warning(f"Error processing track: {e}")
                continue
        
        if not track_data:
            raise Exception("No valid track data processed")
        
        # Create DataFrame
        df = pd.DataFrame(track_data)
        
        # Log summary
        logging.info(f"Processed {len(df)} tracks for {artist_name}")
        logging.info(f"Average popularity: {df['popularity'].mean():.2f}")
        
        # Save to CSV
        success = save_to_csv(df, full_output_path)
        if not success:
            raise Exception("Failed to save data to CSV")
        
        logging.info("Spotify ETL completed successfully")
        
        # Return summary for XCom
        return {
            "artist_name": artist_name,
            "tracks_processed": len(df),
            "output_path": full_output_path,
            "avg_popularity": float(df['popularity'].mean()),
            "execution_date": context.get('ds') if context else str(datetime.datetime.now().date())
        }
        
    except Exception as e:
        logging.error(f"Spotify ETL failed: {e}")
        raise


if __name__ == "__main__":
    # For testing purposes
    logging.basicConfig(level=logging.INFO)
    run_spotify_etl()
