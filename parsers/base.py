import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

ENABLE_PARSER_LOGGING = os.getenv("ENABLE_PARSER_LOGGING", "false").lower() == "true"

# Optional: Set global minimum logging level
DEFAULT_LOG_LEVEL = os.getenv("PARSER_LOG_LEVEL", "INFO").upper()

# Define custom logging levels (you can still use these if you want)
PARSER_LOG_LEVELS = {
    "AbilityLevelParser": 25,
    "ActionEventParser": 26,
    "CombatLogParser": 27,
    "DeathEventParser": 28,
    "DraftEventParser": 29,
    "HeroSnapshotParser": 24,
    "ItemEventParser": 23,
    "NeutralItemEventParser": 22,
    "ObjectiveEventParser": 21,
    "PlayerHeroMapParser": 20,
    "VisionEventParser": 19,
}

# Register custom levels if desired
if ENABLE_PARSER_LOGGING:
    for parser_name, level in PARSER_LOG_LEVELS.items():
        logging.addLevelName(level, parser_name)

class BaseEventParser:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        if ENABLE_PARSER_LOGGING:
            # Use custom level if defined, otherwise fallback to env/global level
            default_level = getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO)
            level = PARSER_LOG_LEVELS.get(self.__class__.__name__, default_level)
            self.logger.setLevel(level)
        else:
            self.logger.setLevel(logging.CRITICAL)

    def log(self, message, *args, level=logging.INFO, **kwargs):
        if self.logger.isEnabledFor(level):
            self.logger.log(level, message, *args, **kwargs)

    def is_valid_time(self, event: dict) -> bool:
        return event.get("time", 0) >= 0

    def parse(self, event: dict, match_id: int) -> dict | None:
        raise NotImplementedError("Parser must implement the parse() method")
