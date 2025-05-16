from parsers.base import BaseEventParser
import asyncio

class NeutralItemEventParser(BaseEventParser):
    async def parse(self, event: dict, match_id: int) -> dict | None:
        self.log("Parsing neutral item event for match_id: %d", match_id, level=20)  # INFO
        self.log("Event data: %s", event, level=10)  # DEBUG

        try:
            parsed_event = await asyncio.to_thread(self._parse_event, event, match_id)
            self.log("Successfully parsed neutral item event for match_id: %d", match_id, level=20)  # INFO
            self.log("Parsed data: %s", parsed_event, level=10)  # DEBUG
            return parsed_event
        except Exception:
            self.logger.exception("Error while parsing neutral item event for match_id: %d", match_id)
            return None

    def _parse_event(self, event: dict, match_id: int) -> dict:
        self.log("Starting _parse_event for match_id: %d", match_id, level=10)  # DEBUG
        parsed_event = {
            "time": event["time"],
            "match_id": match_id,
            "slot": event.get("slot"),
            "key": event.get("key"),
            "is_neutral_active_drop": event.get("isNeutralActiveDrop", False),
            "is_neutral_passive_drop": event.get("isNeutralPassiveDrop", False),
        }
        self.log("Parsed event data for match_id: %d: %s", match_id, parsed_event, level=10)  # DEBUG
        return parsed_event

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        if not self.is_valid_time(event):
            return False
        return self._parse_event(event, match_id)