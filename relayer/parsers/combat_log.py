from parsers.base import BaseEventParser
import asyncio

class CombatLogParser(BaseEventParser):
    async def parse(self, event: dict, match_id: int) -> dict | None:
        self.log("Parsing combat log event for match_id: %d", match_id, level=20)  # INFO
        self.log("Event data: %s", event, level=10)  # DEBUG

        # Allow 'dota_unknown' only if it's a GAME_STATE event
        if any(event.get(name) == "dota_unknown" for name in ("attackername", "targetname", "inflictor")):
            if event.get("type") != "DOTA_COMBATLOG_GAME_STATE":
                self.log("Invalid 'dota_unknown' field in event for match_id: %d", match_id, level=30)  # WARNING
                return False

        try:
            parsed_data = await asyncio.to_thread(self._parse_event, event, match_id)
            self.log("Successfully parsed combat log event for match_id: %d", match_id, level=20)  # INFO
            self.log("Parsed data: %s", parsed_data, level=10)  # DEBUG
            return parsed_data
        except Exception:
            self.logger.exception("Error while parsing combat log event for match_id: %d", match_id)
            return None

    def _parse_event(self, event: dict, match_id: int) -> dict:
        self.log("Starting _parse_event for match_id: %d", match_id, level=10)  # DEBUG
        parsed_event = {
            "time": event["time"],
            "match_id": match_id,
            "type": event["type"],
            "attackername": event.get("attackername"),
            "targetname": event.get("targetname"),
            "inflictor": event.get("inflictor"),
            "abilitylevel": event.get("abilitylevel"),
            "gold": event.get("gold"),
            "xp": event.get("xp"),
            "damage": event.get("value"),
            "variant": event.get("variant"),
            "attackerhero": event.get("attackerhero"),
            "targethero": event.get("targethero"),
            "attackerillusion": event.get("attackerillusion"),
            "targetillusion": event.get("targetillusion"),
            "x": event.get("x"),
            "y": event.get("y"),
            "z": event.get("z"),
            "stun_duration": event.get("stun_duration"),
            "slow_duration": event.get("slow_duration"),
            "slot": event.get("slot"),
            "player1": event.get("player1"),
            "player2": event.get("player2"),
            "gold_reason": event.get("gold_reason"),
            "xp_reason": event.get("xp_reason"),
            "unit": event.get("unit"),
        }
        self.log("Parsed event data for match_id: %d: %s", match_id, parsed_event, level=10)  # DEBUG
        return parsed_event

    def parse_sync(self, event: dict, match_id: int) -> dict | None:
        if not self.is_valid_time(event):
            return False
        return self._parse_event(event, match_id)