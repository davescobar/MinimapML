from parsers.base import BaseEventParser
import asyncio

class DeathEventParser(BaseEventParser):
    async def parse(self, event: dict, match_id: int) -> dict | None:
        self.log("Parsing death event for match_id: %d", match_id, level=20)  # INFO
        self.log("Event data: %s", event, level=10)  # DEBUG

        try:
            parsed_data = await asyncio.to_thread(self._parse_event, event, match_id)
            self.log("Successfully parsed death event for match_id: %d", match_id, level=20)  # INFO
            self.log("Parsed data: %s", parsed_data, level=10)  # DEBUG
            return parsed_data
        except Exception:
            self.logger.exception("Error while parsing death event for match_id: %d", match_id)
            return None

    def _parse_event(self, event: dict, match_id: int) -> dict:
        self.log("Starting _parse_event for match_id: %d", match_id, level=10)  # DEBUG
        parsed_event = {
            "time": event["time"],
            "match_id": match_id,
            "attackername": event.get("attackername"),
            "targetname": event.get("targetname"),
            "attackerhero": event.get("attackerhero"),
            "targethero": event.get("targethero"),
            "attackerillusion": event.get("attackerillusion"),
            "targetillusion": event.get("targetillusion"),
            "x": event.get("x"),
            "y": event.get("y"),
            "z": event.get("z"),
            "slot": event.get("slot"),
            "player1": event.get("player1"),
            "player2": event.get("player2"),
            "unit": event.get("unit"),
        }
        self.log("Parsed event data for match_id: %d: %s", match_id, parsed_event, level=10)  # DEBUG
        return parsed_event

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        if not self.is_valid_time(event):
            return False
        return self._parse_event(event, match_id)