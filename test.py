import json

# Load the JSON file
with open('parsed_response_data.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

combat = set()
# Loop through each top-level item (assumed to be a list of dicts)
for i, event in enumerate(data):
    if isinstance(event, dict):
        for key, value in event.items():
            if key == 'type' and value.startswith('DOTA_COMBATLOG'):
                combat.add(value)
    else:
        print(f"[{i}] Skipped: not a dict -> {event}")

print(combat)