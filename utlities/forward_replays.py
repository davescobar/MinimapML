import os
import bz2
import requests

# Directory containing .bz2 files
REPLAYS_DIR = r"F:\dota_replays"
API_ENDPOINT = "http://localhost:8000/ingest"  # Update with your FastAPI server URL if different

def decompress_bz2(file_path):
    """Decompress a .bz2 file and return the decompressed data."""
    try:
        with bz2.BZ2File(file_path, 'rb') as bz2_file:
            return bz2_file.read()
    except OSError as e:
        print(f"Error decompressing {file_path}: {e}")
        return None

def send_replay(file_path):
    """Send a decompressed replay file to the FastAPI endpoint."""
    try:
        decompressed_data = decompress_bz2(file_path)
        files = {'file': (os.path.basename(file_path), decompressed_data)}
        response = requests.post(API_ENDPOINT, files=files)
        response.raise_for_status()
        print(f"Successfully sent {file_path}: {response.json()}")
    except Exception as e:
        print(f"Failed to send {file_path}: {e}")

def main():
    """Main function to process all .bz2 files in the directory."""
    for file_name in os.listdir(REPLAYS_DIR):
        if file_name.endswith(".bz2"):
            file_path = os.path.join(REPLAYS_DIR, file_name)
            print(f"Processing {file_path}...")
            send_replay(file_path)

if __name__ == "__main__":
    main()