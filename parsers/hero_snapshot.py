from parsers.base import BaseEventParser
import asyncio

class HeroSnapshotParser(BaseEventParser):
    async def parse(self, event: dict, match_id: int) -> dict | None:
        self.log("Parsing event for match_id: %d", match_id, level=20)  # INFO
        self.log("Event data: %s", event, level=10)  # DEBUG
        try:
            parsed_data = await asyncio.to_thread(self._parse_event, event, match_id)
            self.log("Successfully parsed event for match_id: %d", match_id, level=20)  # INFO
            self.log("Parsed data: %s", parsed_data, level=10)  # DEBUG
            return parsed_data
        except Exception:
            self.logger.exception("Error while parsing event for match_id: %d", match_id)
            return None

    def _parse_event(self, event: dict, match_id: int) -> dict:
        self.log("Starting _parse_event for match_id: %d", match_id, level=10)  # DEBUG
        try:
            parsed_event = {
                "time": event["time"],
                "match_id": match_id,
                "slot": event.get("slot"),
                "hero_id": event.get("hero_id"),
                "x": float(event.get("x", 0.0)),
                "y": float(event.get("y", 0.0)),
                "z": float(event.get("z", 0.0)),
                "hp": int(event.get("hp", 0)),
                "mana": int(event.get("mana", 0)),
                "level": int(event.get("level", 0)),
                "gold": int(event.get("gold", 0)),
                "networth": int(event.get("networth", 0)),
                "xp": int(event.get("xp", 0)),
                "kills": int(event.get("kills", 0)),
                "deaths": int(event.get("deaths", 0)),
                "assists": int(event.get("assists", 0)),
                "denies": int(event.get("denies", 0)),
                "lh": int(event.get("lh", 0)),
                "obs_placed": int(event.get("obs_placed", 0)),
                "sen_placed": int(event.get("sen_placed", 0)),
                "towers_killed": int(event.get("towers_killed", 0)),
                "roshans_killed": int(event.get("roshans_killed", 0)),
                "creeps_stacked": int(event.get("creeps_stacked", 0)),
                "camps_stacked": int(event.get("camps_stacked", 0)),
                "rune_pickups": int(event.get("rune_pickups", 0)),
                "teamfight_participation": float(event.get("teamfight_participation", 0.0)),
                "firstblood_claimed": int(event.get("firstblood_claimed", 0)),
                "life_state": int(event.get("life_state", 0)),
                "randomed": bool(event.get("randomed", False)),
                "pred_vict": bool(event.get("pred_vict", False)),
            }
            self.log("Parsed event data for match_id: %d: %s", match_id, parsed_event, level=10)  # DEBUG
            return parsed_event
        except Exception:
            self.logger.exception("Error while parsing event for match_id: %d", match_id)
            return None

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        if not self.is_valid_time(event):
            return False
        return self._parse_event(event, match_id)