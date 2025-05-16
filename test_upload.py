
# import json
# import logging
# from parsers.event_dispatcher import EventDispatcher
# from services.postgres import PostgresDB
# from services.processor import EventProcessor  # Assuming you saved the new class here

# IGNORED_TYPES = {
#     "pings", "chatwheel", 'CHAT_MESSAGE_RUNE_PICKUP', "CHAT_MESSAGE_RUNE_BOTTLE", "CHAT_MESSAGE_SCAN_USED",
#     "CHAT_MESSAGE_HERO_KILL", "CHAT_MESSAGE_FIRSTBLOOD", "CHAT_MESSAGE_STREAK_KILL", "CHAT_MESSAGE_GLYPH_USED",
#     "CHAT_MESSAGE_SENTRY_WARD_KILLED", "CHAT_MESSAGE_OBSERVER_WARD_KILLED", "CHAT_MESSAGE_AEGIS", "CHAT_MESSAGE_COURIER_LOST",
#     "CHAT_MESSAGE_COURIER_RESPAWNED", "chat", "CHAT_MESSAGE_DISCONNECT", "cosmetics", "dotaplus", "epilogue"
# }

# logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
# logger = logging.getLogger(__name__)

# def test_parsers_and_insert(json_path: str, match_id: int = 123):
#     with open(json_path, 'r') as f:
#         events = json.load(f)

    
#     db = PostgresDB()
#     processor = EventProcessor(db_writer=db)
#     parsed_data = processor.process_events(events, match_id)
#     parsed_counts = {}
#     ignored_count = 0
#     unhandled_count = 0


#     # Accumulate rows for each table
#     parsed_data: dict[str, list] = {
#         "HeroSnapshotParser": [],
#         "CombatLogParser": [],
#         "ItemEventParser": [],
#         "DeathEventParser": [],
#         "VisionEventParser": [],
#         "DraftEventParser": [],
#         "ObjectiveEventParser": [],
#         "StatusEffectParser": [],
#         "GameStateChangeParser": [],
#         "ActionEventParser": [],
#         "PlayerHeroMapParser": [],
#         "AbilityLevelParser": [],
#         "NeutralItemEventParser": [],
#     }
#     METHOD_MAP = {
#         "HeroSnapshotParser": db.insert_hero_snapshots,
#         "AbilityLevelParser": db.insert_abilitylevel,
#         "CombatLogParser": db.insert_combatlog,
#         "ItemEventParser": db.insert_itemevent,
#         "DeathEventParser": db.insert_deathevent,
#         "VisionEventParser": db.insert_visionevent,
#         "DraftEventParser": db.insert_draftevent,
#         "ObjectiveEventParser": db.insert_objectiveevent,
#         "ActionEventParser": db.insert_actionevent,
#         "PlayerHeroMapParser": db.insert_playerheromap,
#         "NeutralItemEventParser": db.insert_neutralitemevent,
#     }

#     logger.info(f"Loaded {len(events)} events, {ignored_count} ignored")
#     logger.info("--- Parsed Events Summary ---")
#     for table, count in parsed_counts.items():
#         logger.info(f"{table}: {count} parsed events")

#     logger.info(f"Unhandled events: {unhandled_count}")

#     # Dispatch to DB insert methods
#     for parser_name, rows in parsed_data.items():
#         if not rows:
#             continue

#         logger.info(f"Inserting {len(rows)} rows into {parser_name}")
#         insert_method = METHOD_MAP.get(parser_name)
#         if insert_method:
#             insert_method(rows)
#         else:
#             logger.warning(f"No insert method found for {parser_name}")

#     db.close()


# if __name__ == "__main__":
#     test_parsers_and_insert("parsed_response_data.json")
import json
import logging
from services.postgres import PostgresDB
from services.processor import EventProcessor  # This is your new orchestrator

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def test_parsers_and_insert(json_path: str, match_id: int = 123):
    with open(json_path, 'r') as f:
        events = json.load(f)

    db = PostgresDB()
    processor = EventProcessor(db_writer=db)
    processor.process_events(events, match_id)
    db.close()

if __name__ == "__main__":
    test_parsers_and_insert("parsed_response_data.json")
