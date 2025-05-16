from parsers.base import BaseEventParser
import asyncio

class ObjectiveEventParser(BaseEventParser):
    async def parse(self, event: dict, match_id: int) -> dict | None:
        self.log("Parsing objective event for match_id: %d", match_id, level=20)  # INFO
        self.log("Event data: %s", event, level=10)  # DEBUG

        try:
            parsed_event = await asyncio.to_thread(self._parse_event, event, match_id)
            self.log("Successfully parsed objective event for match_id: %d", match_id, level=20)  # INFO
            self.log("Parsed data: %s", parsed_event, level=10)  # DEBUG
            return parsed_event
        except Exception:
            self.logger.exception("Error while parsing objective event for match_id: %d", match_id)
            return None

    def _parse_event(self, event: dict, match_id: int) -> dict:
        self.log("Starting _parse_event for match_id: %d", match_id, level=10)  # DEBUG
        parsed_event = {
            "time": event["time"],
            "match_id": match_id,
            "type": event.get("type"),
            "targetname": event.get("targetname"),
            "attackername": event.get("attackername"),
            "x": event.get("x"),
            "y": event.get("y"),
            "z": event.get("z"),
            "unit": event.get("unit"),
            "variant": event.get("variant"),
            "slot": event.get("slot"),
            "player1": event.get("player1"),
            "player2": event.get("player2"),
        }
        self.log("Parsed event data for match_id: %d: %s", match_id, parsed_event, level=10)  # DEBUG
        return parsed_event

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        return self._parse_event(event, match_id)