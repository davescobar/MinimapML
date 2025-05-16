import requests
import json
import os

def upload_file_and_save_json_lines_response(dem_file_path, url, output_json_path="response_list.json"):
    """
    Uploads a DEM file using the requests library,
    parses a JSON Lines (newline-delimited JSON) response,
    collects the objects into a list, and saves this list as a single JSON array.

    Args:
        dem_file_path (str): The absolute path to the .dem file.
        url (str): The URL to upload the file to.
        output_json_path (str): The path where the list of JSON objects will be saved.
    """
    if not os.path.exists(dem_file_path):
        print(f"Error: DEM file not found at {dem_file_path}")
        return

    headers = {
        "Content-Type": "application/octet-stream"
    }

    print(f"Preparing to upload {dem_file_path} to {url} with Content-Type: application/octet-stream")

    try:
        with open(dem_file_path, 'rb') as f_binary:
            response = requests.post(url, data=f_binary, headers=headers, timeout=90)

        print(f"\n--- HTTP Request Sent ---")
        print(f"URL: {response.request.url}")
        print(f"Method: {response.request.method}")
        print(f"Headers Sent: {response.request.headers}")

        print(f"\n--- HTTP Response Received ---")
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {response.headers}")
        # Using response.text to get decoded string
        # print(f"Raw Response Content (first 500 chars): {response.text[:500]}")

        if 200 <= response.status_code < 300:
            print(f"\nUpload successful (HTTP {response.status_code}). Processing JSON Lines response.")

            parsed_objects = []
            lines = response.text.splitlines() # response.text decodes content (e.g. from bytes)
            
            for i, line in enumerate(lines):
                line = line.strip() # Remove leading/trailing whitespace
                if not line: # Skip empty lines
                    continue
                try:
                    obj = json.loads(line)
                    parsed_objects.append(obj)
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not decode line {i+1} as JSON: '{line}'. Error: {e}")
            
            if parsed_objects:
                print(f"\nSuccessfully parsed {len(parsed_objects)} JSON objects from the response.")
                try:
                    with open(output_json_path, 'w') as f_json_out:
                        json.dump(parsed_objects, f_json_out, indent=4)
                    print(f"List of JSON objects saved to {output_json_path}")
                except IOError as e:
                    print(f"Error saving JSON to file: {e}")
            else:
                print("\nNo valid JSON objects found in the response lines.")

        else:
            print(f"\nError: Upload failed with HTTP status code {response.status_code}")
            print(f"Response content (may contain error details):\n{response.text}")
            # Attempt to save error response (which might be a single JSON error object)
            try:
                error_json = json.loads(response.text) # Try to parse the whole thing as a single error JSON
                with open(output_json_path, 'w') as f_json_err_out:
                    json.dump(error_json, f_json_err_out, indent=4)
                print(f"Error response (if JSON) saved to {output_json_path}")
            except json.JSONDecodeError:
                print("Error response was not valid JSON (or was JSON Lines, which isn't a single error object).")


    except requests.exceptions.Timeout:
        print(f"\nError: The request to {url} timed out.")
    except requests.exceptions.ConnectionError:
        print(f"\nError: Could not connect to {url}. Is the server running?")
    except requests.exceptions.RequestException as e:
        print(f"\nAn error occurred during the HTTP request: {e}")
    except IOError as e:
        print(f"\nAn IO error occurred (e.g., reading file '{dem_file_path}' or writing output): {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    # --- Configuration ---
    dem_file_to_upload = r"C:\Users\david\Downloads\dota\8286872699_1254779634.dem" # USE YOUR ACTUAL PATH
    target_url_for_upload = "http://localhost:5700/upload"
    # Output file will contain a JSON array (list of objects)
    output_json_file = "parsed_response_data.json"
    # --- End Configuration ---

    if not os.path.isabs(dem_file_to_upload):
        dem_file_to_upload = os.path.abspath(dem_file_to_upload)
        print(f"Converted DEM file path to absolute: {dem_file_to_upload}")

    upload_file_and_save_json_lines_response(dem_file_to_upload, target_url_for_upload, output_json_file)