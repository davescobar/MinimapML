from parsers.base import BaseEventParser
import asyncio

class DraftEventParser(BaseEventParser):
    async def parse(self, event: dict, match_id: int) -> dict | None:
        self.log("Parsing draft event for match_id: %d", match_id, level=20)  # INFO
        self.log("Event data: %s", event, level=10)  # DEBUG
        try:
            return await asyncio.to_thread(self._parse_event, event, match_id)
        except Exception:
            self.logger.exception("Error while parsing draft event for match_id: %d", match_id)
            return None

    def _parse_event(self, event: dict, match_id: int) -> dict:
        self.log("Starting _parse_event for match_id: %d", match_id, level=10)  # DEBUG
        parsed_event = {
            "time": event["time"],
            "match_id": match_id,
            "type": event.get("type"),
            "slot": event.get("slot"),
            "stage": event.get("stage"),
            "hero_id": event.get("hero_id"),
            "draft_order": event.get("draft_order"),
            "pick": event.get("pick"),
            "draft_active_team": event.get("draft_active_team"),
            "draft_extime0": event.get("draft_extime0"),
            "draft_extime1": event.get("draft_extime1"),
        }
        self.log("Parsed event data for match_id: %d: %s", match_id, parsed_event, level=10)  # DEBUG
        return parsed_event

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        return self._parse_event(event, match_id)