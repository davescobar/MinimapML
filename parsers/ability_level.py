from parsers.base import BaseEventParser
import asyncio

class AbilityLevelParser(BaseEventParser):
    async def parse(self, event: dict, match_id: int) -> dict | None:
        self.log("Parsing ability level event for match_id: %d", match_id, level=20)  # INFO
        self.log("Event data: %s", event, level=10)  # DEBUG

        try:
            parsed_event = await asyncio.to_thread(self._parse_event, event, match_id)
            self.log("Successfully parsed ability level event for match_id: %d", match_id, level=20)  # INFO
            self.log("Parsed data: %s", parsed_event, level=10)  # DEBUG
            return parsed_event
        except Exception:
            self.logger.exception("Error while parsing ability level event for match_id: %d", match_id)
            return None

    def _parse_event(self, event: dict, match_id: int) -> dict:
        self.log("Starting _parse_event for match_id: %d", match_id, level=10)  # DEBUG
        parsed_event = {
            "time": event["time"],
            "match_id": match_id,
            "targetname": event.get("targetname"),
            "valuename": event.get("valuename"),
            "abilitylevel": event.get("abilitylevel"),
        }
        self.log("Parsed event data for match_id: %d: %s", match_id, parsed_event, level=10)  # DEBUG
        return parsed_event

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        return self._parse_event(event, match_id)
