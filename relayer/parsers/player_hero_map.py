from parsers.base import BaseEventParser

class PlayerHeroMapParser(BaseEventParser):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.name_to_id = None

    async def initialize(self):
        self.log("Loading hero name-to-ID map from database...", level=20)  # INFO
        try:
            self.name_to_id = await self.db.load_hero_name_map()
            self.log("Hero name map loaded successfully.", level=20)
        except Exception:
            self.logger.exception("Failed to load hero name-to-ID map.")

    def parse_all(self, events: list[dict], match_id: int) -> list[dict]:
        self.log("Parsing player-hero mapping for match_id: %d", match_id, level=20)

        # Step 1: Map player_slot.key → slot value (e.g., 5 → 128)
        key_to_slot_value = {
            int(e["key"]): e["value"]
            for e in events
            if e.get("type") == "player_slot"
        }
        self.log("key_to_slot_value: %s", key_to_slot_value, level=10)  # DEBUG

        # Step 2: Map steamid.key → unit (steam_id)
        key_to_steamid = {
            int(e["key"]): e["unit"]
            for e in events
            if e.get("type") == "steamid"
        }
        self.log("key_to_steamid: %s", key_to_steamid, level=10)  # DEBUG

        # Step 3: slot number (0–9) → steam_id
        slot_to_steamid = {
            slot: key_to_steamid.get(value)
            for slot, value in key_to_slot_value.items()
        }
        self.log("slot_to_steamid: %s", slot_to_steamid, level=10)  # DEBUG

        results = []
        for e in events:
            if e.get("type") not in {"player_slot", "interval"}:
                continue
            if not self.is_valid_time(e):
                continue

            slot = e.get("slot")
            hero_id = e.get("hero_id")

            if not hero_id and "unit" in e and self.name_to_id:
                hero_id = self.name_to_id.get(e["unit"])

            if slot is None or hero_id is None:
                continue

            steam_id = slot_to_steamid.get(slot)
            results.append({
                "match_id": match_id,
                "slot": slot,
                "hero_id": hero_id,
                "steam_id": steam_id
            })

        self.log("Finished parsing player-hero map with %d entries.", len(results), level=20)  # INFO
        return results

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        return self._parse_event(event, match_id)