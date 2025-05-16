import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from services.postgres import PostgresDB  # Assume this exists
from parsers.parser_lookup import get_parser_for_type  # New function we'll define


def parse_event_static(event: dict, match_id: int) -> tuple[str, dict] | None:
    event_type = event.get("type")
    # Skip specified event types
    if event_type == "neutral_token":
        return False
    if event_type in [
        'chatwheel', 'CHAT_MESSAGE_DISCONNECT', 'CHAT_MESSAGE_CONNECT', 
        'cosmetics', 'chat', 'CHAT_MESSAGE_HERO_KILL', 'CHAT_MESSAGE_PAUSED', 
        'CHAT_MESSAGE_UNPAUSE_COUNTDOWN', 'CHAT_MESSAGE_UNPAUSED', 
        'CHAT_MESSAGE_RECONNECT', 'CHAT_MESSAGE_RUNE_PICKUP', 
        'CHAT_MESSAGE_RUNE_BOTTLE', 'CHAT_MESSAGE_GLYPH_USED', 
        'CHAT_MESSAGE_SCAN_USED', 'CHAT_MESSAGE_OBSERVER_WARD_KILLED', 
        'CHAT_MESSAGE_SENTRY_WARD_KILLED', 'CHAT_MESSAGE_AEGIS_STOLEN', 
        'CHAT_MESSAGE_BUYBACK', 'CHAT_MESSAGE_EFFIGY_KILL', 
        'CHAT_MESSAGE_INVALID', 'DOTA_COMBATLOG_BUYBACK', 'pings', 
        'sen_left', 'obs_left', 'dotaplus', 'epilogue', 
        'CHAT_MESSAGE_COURIER_LOST', 'CHAT_MESSAGE_FIRSTBLOOD', 
        'CHAT_MESSAGE_COURIER_RESPAWNED', 'CHAT_MESSAGE_STREAK_KILL', 'steamid',
        'CHAT_MESSAGE_RUNE_DENY', 'CHAT_MESSAGE_ITEM_PURCHASE', 'CHAT_MESSAGE_AEGIS',
        'CHAT_MESSAGE_SUPER_CREEPS', 'CHAT_MESSAGE_HERO_DENY', 'CHAT_MESSAGE_HERO_BANNED'
    ]:
        return False
    try:
        parser = get_parser_for_type(event_type)
        if not parser:
            print(f"[WARN] No parser found for event type '{event_type}'")
            return None
        result = parser.parse_sync(event, match_id)
        if result is False:
            return False
        if not result:
            print(f"[DEBUG] Parser '{parser.__class__.__name__}' returned no result for type '{event_type}'")
            return None
        return parser.__class__.__name__, result
    except Exception as e:
        print(f"[ERROR] Exception while parsing type '{event_type}': {e}")
        return None



def parse_batch_static(events_chunk: list[dict], match_id: int) -> list[tuple[str, dict]]:
    results = []
    for e in events_chunk:
        parsed = parse_event_static(e, match_id)
        if parsed:
            results.append(parsed)
    return results


class EventProcessor:
    def __init__(self, db_writer: PostgresDB = None):
        self.db_writer = db_writer
        self.hero_map_parser = None

    async def process_events(self, events: list[dict], match_id: int):
        logger = logging.getLogger(__name__)
        logger.info("Processing events for match_id: %d", match_id)

        parsed_batches = {}
        loop = asyncio.get_running_loop()

        CHUNK_SIZE = 10000
        chunks = [events[i:i + CHUNK_SIZE] for i in range(0, len(events), CHUNK_SIZE)]

        # Run in current event loop, serially (or optionally threaded)
        results_nested = [parse_batch_static(chunk, match_id) for chunk in chunks]

        results = [item for sublist in results_nested for item in sublist]  # flatten

        for result in results:
            if result is False or result is None:
                continue
            table_name, parsed = result
            cleaned = {k: v for k, v in parsed.items() if v is not None}
            parsed_batches.setdefault(table_name, []).append(cleaned)

        if self.db_writer:
            try:
                if not self.hero_map_parser:
                    from parsers.player_hero_map import PlayerHeroMapParser
                    self.hero_map_parser = PlayerHeroMapParser(self.db_writer)
                    await self.hero_map_parser.initialize()

                logger.info("Parsing player-hero mappings for match_id: %d", match_id)
                player_rows = self.hero_map_parser.parse_all(events, match_id)
                if player_rows:
                    await self.db_writer.write("PlayerHeroMapParser", player_rows)

                tasks = [
                    self.db_writer.write(table, rows)
                    for table, rows in parsed_batches.items()
                ]
                await asyncio.gather(*tasks)

            except Exception:
                logger.exception("Error writing parsed data to the database for match_id: %d", match_id)

        logger.info("Finished processing events for match_id: %d", match_id)
        return parsed_batches
