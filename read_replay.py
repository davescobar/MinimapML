import json

json_keys = set()
key_value_sets = {}

with open("parsed_response_data.json", 'r') as f_json_out:
    parsed_objects = json.load(f_json_out)
    print(f"Parsed {len(parsed_objects)} JSON objects from the response.")
    
    # for obj in parsed_objects:
    #     for key, value in obj.items():
    #         json_keys.add(key)
            
    #         # Initialize a set for the key if it doesn't exist
    #         if key not in key_value_sets:
    #             key_value_sets[key] = set()
            
    #         # Add the value to the corresponding set
    #         key_value_sets[key].add(value)
            
    #         if key == "type" and not value:
    #             print(f"Warning: Object {obj} has no 'type' field.")
    
    print(f"Unique keys in the JSON objects: {len(json_keys)}")
    for key, value_set in key_value_sets.items():
        print(f"Key: {key}, Sample values: {list(value_set)[:5]}")  # Print up to 5 sample values