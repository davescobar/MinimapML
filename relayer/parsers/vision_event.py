from parsers.base import BaseEventParser
import asyncio

class VisionEventParser(BaseEventParser):
    async def parse(self, event: dict, match_id: int) -> dict | None:
        self.log("Parsing vision event for match_id: %d", match_id, level=20)  # INFO
        self.log("Event data: %s", event, level=10)  # DEBUG


        try:
            parsed_event = await asyncio.to_thread(self._parse_event, event, match_id)
            self.log("Successfully parsed vision event for match_id: %d", match_id, level=20)  # INFO
            self.log("Parsed data: %s", parsed_event, level=10)  # DEBUG
            return parsed_event
        except Exception:
            self.logger.exception("Error while parsing vision event for match_id: %d", match_id)
            return None

    def _parse_event(self, event: dict, match_id: int) -> dict:
        self.log("Starting _parse_event for match_id: %d", match_id, level=10)  # DEBUG
        parsed_event = {
            "time": event["time"],
            "match_id": match_id,
            "slot": event.get("slot"),
            "type": event.get("type"),
            "x": event.get("x"),
            "y": event.get("y"),
            "z": event.get("z"),
            "unit": event.get("unit"),
            "life_state": event.get("life_state"),
            "entityleft": event.get("entityleft"),
            "ehandle": event.get("ehandle"),
        }
        self.log("Parsed event data for match_id: %d: %s", match_id, parsed_event, level=10)  # DEBUG
        return parsed_event

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        return self._parse_event(event, match_id)