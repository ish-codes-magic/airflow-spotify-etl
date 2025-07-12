import os
import pandas as pd 
import requests
import json
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, List, Any
from airflow.models import Variable
import sqlite3


# Configuration
DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"


def get_spotify_user_token() -> str:
    """
    Get Spotify user access token from environment variables or Airflow Variables.
    
    Returns:
        User access token
        
    Raises:
        ValueError: If token is not found
    """
    # Try Airflow Variables first
    token = Variable.get("SPOTIFY_USER_TOKEN", default_var=None)
    
    # Fallback to environment variables
    if not token:
        token = os.getenv("SPOTIFY_USER_TOKEN")
    
    if not token:
        raise ValueError(
            "Spotify user token not found. Please set SPOTIFY_USER_TOKEN. "
            "Get your token from: https://developer.spotify.com/console/get-recently-played/"
        )
    
    return token


def check_if_valid_data(df: pd.DataFrame) -> bool:
    """
    Validate the data in the DataFrame.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if data is valid, False otherwise
        
    Raises:
        Exception: If data validation fails
    """
    # Check if dataframe is empty
    if df.empty:
        logging.warning("No songs downloaded. Finishing execution")
        return False 

    # Primary Key Check
    if 'played_at' in df.columns and not df['played_at'].is_unique:
        raise Exception("Primary Key check is violated - duplicate played_at timestamps found")

    # Check for nulls
    if df.isnull().values.any():
        logging.warning("Null values found in data")
        # You might want to handle nulls instead of raising an exception
        # raise Exception("Null values found")

    # Check that timestamps are from recent dates (last 7 days)
    if 'timestamp' in df.columns:
        seven_days_ago = datetime.now() - timedelta(days=7)
        
        try:
            # Convert timestamps to datetime for comparison
            timestamps = pd.to_datetime(df["timestamp"]).dt.date
            seven_days_ago_date = seven_days_ago.date()
            
            old_tracks = timestamps < seven_days_ago_date
            if old_tracks.any():
                logging.warning(f"Found {old_tracks.sum()} tracks older than 7 days")
                
        except Exception as e:
            logging.error(f"Error validating timestamps: {e}")

    return True


def get_recently_played_tracks(token: str, limit: int = 50) -> Optional[Dict[str, Any]]:
    """
    Get recently played tracks from Spotify API.
    
    Args:
        token: Spotify access token
        limit: Number of tracks to retrieve (max 50)
        
    Returns:
        API response data or None if failed
    """
    try:
        # Convert time to Unix timestamp in milliseconds      
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        # Download all songs listened to in the last 24 hours      
        url = f"https://api.spotify.com/v1/me/player/recently-played"
        params = {
            "after": yesterday_unix_timestamp,
            "limit": min(limit, 50)  # Spotify API limit is 50
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logging.info(f"Successfully retrieved {len(data.get('items', []))} recently played tracks")
        
        return data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP error getting recently played tracks: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error getting recently played tracks: {e}")
        return None


def process_tracks_data(data: Dict[str, Any]) -> pd.DataFrame:
    """
    Process the API response data into a structured DataFrame.
    
    Args:
        data: API response data
        
    Returns:
        Processed DataFrame
    """
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    track_ids = []
    album_names = []
    durations = []
    popularities = []
    
    # Extract relevant data from the JSON response      
    for item in data.get("items", []):
        try:
            track = item.get("track", {})
            
            song_names.append(track.get("name", "Unknown"))
            
            # Get first artist name
            artists = track.get("artists", [])
            artist_name = artists[0].get("name", "Unknown") if artists else "Unknown"
            artist_names.append(artist_name)
            
            played_at = item.get("played_at", "")
            played_at_list.append(played_at)
            
            # Extract date from played_at timestamp
            timestamp = played_at[:10] if played_at else ""
            timestamps.append(timestamp)
            
            # Additional fields
            track_ids.append(track.get("id", ""))
            album_names.append(track.get("album", {}).get("name", "Unknown"))
            durations.append(track.get("duration_ms", 0))
            popularities.append(track.get("popularity", 0))
            
        except Exception as e:
            logging.warning(f"Error processing track item: {e}")
            continue
    
    # Create DataFrame with all the extracted data       
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps,
        "track_id": track_ids,
        "album_name": album_names,
        "duration_ms": durations,
        "popularity": popularities
    }

    df = pd.DataFrame(song_dict)
    logging.info(f"Processed {len(df)} tracks into DataFrame")
    
    return df


def save_to_database(df: pd.DataFrame, database_location: str) -> bool:
    """
    Save DataFrame to SQLite database.
    
    Args:
        df: DataFrame to save
        database_location: Database connection string
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Extract database path from connection string
        db_path = database_location.replace("sqlite:///", "")
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        
        # Save to database (append if table exists)
        df.to_sql("recently_played", conn, if_exists="append", index=False)
        
        conn.close()
        logging.info(f"Successfully saved {len(df)} records to database")
        return True
        
    except Exception as e:
        logging.error(f"Error saving to database: {e}")
        return False


def run_spotify_recently_played_etl(**context):
    """
    Main ETL function for Spotify recently played tracks.
    
    Args:
        **context: Airflow context (when called from Airflow)
    """
    try:
        logging.info("Starting Spotify recently played ETL")
        
        # Get configuration
        limit = int(Variable.get("SPOTIFY_RECENT_LIMIT", default_var="50"))
        
        # Get user token
        token = get_spotify_user_token()
        
        # Get recently played tracks
        data = get_recently_played_tracks(token, limit)
        if not data:
            raise Exception("Failed to retrieve recently played tracks")
        
        # Process the data
        df = process_tracks_data(data)
        
        if df.empty:
            logging.warning("No tracks to process")
            return {"tracks_processed": 0, "status": "no_data"}
        
        # Validate the data
        if not check_if_valid_data(df):
            logging.warning("Data validation failed, but continuing")
        
        # Save to database
        success = save_to_database(df, DATABASE_LOCATION)
        if not success:
            raise Exception("Failed to save data to database")
        
        logging.info("Spotify recently played ETL completed successfully")
        
        # Return summary for XCom
        return {
            "tracks_processed": len(df),
            "unique_artists": df['artist_name'].nunique(),
            "date_range": f"{df['timestamp'].min()} to {df['timestamp'].max()}",
            "database_location": DATABASE_LOCATION,
            "execution_date": context.get('ds') if context else str(datetime.now().date()),
            "status": "success"
        }
        
    except Exception as e:
        logging.error(f"Spotify recently played ETL failed: {e}")
        raise


if __name__ == "__main__":
    # For testing purposes
    logging.basicConfig(level=logging.INFO)
    run_spotify_recently_played_etl()