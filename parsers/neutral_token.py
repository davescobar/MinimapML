from parsers.base import BaseEventParser
import asyncio

class NeutralTokenParser(BaseEventParser):
    async def parse(self, event: dict, match_id: int) -> dict | None:
        if event.get("type") != "neutral_token":
            return None
        if not self.is_valid_time(event):
            return None

        try:
            return await asyncio.to_thread(self._parse_event, event, match_id)
        except Exception:
            self.logger.exception("Error parsing neutral token for match_id: %d", match_id)
            return None

    def _parse_event(self, event: dict, match_id: int) -> dict:
        return {
            "time": event["time"],
            "match_id": match_id,
            "type": event["type"],  # should always be "neutral_token"
            "key": event.get("key"),
            "slot": event.get("slot")
        }

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        if event.get("type") != "neutral_token":
            return None
        if not self.is_valid_time(event):
            return None
        return self._parse_event(event, match_id)
