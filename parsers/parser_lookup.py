# services/parser_lookup.py
from parsers.hero_snapshot import HeroSnapshotParser
from parsers.combat_log import CombatLogParser
from parsers.item_event import ItemEventParser
from parsers.death_event import DeathEventParser
from parsers.vision_event import VisionEventParser
from parsers.draft_event import DraftEventParser
from parsers.objective_event import ObjectiveEventParser
from parsers.action_event import ActionEventParser
from parsers.ability_level import AbilityLevelParser
from parsers.neutral_item_event import NeutralItemEventParser
from parsers.neutral_token import NeutralTokenParser

def get_parser_for_type(event_type: str):
    type_to_parser = {
        "DOTA_ABILITY_LEVEL": AbilityLevelParser,
        "actions": ActionEventParser,
        "DOTA_COMBATLOG_DEATH": DeathEventParser,
        "neutral_item_history": NeutralItemEventParser,
        "draft_timings": DraftEventParser,
        "draft_start": DraftEventParser,
        "pick": DraftEventParser,
        "interval_update": HeroSnapshotParser,
        "player_slot": HeroSnapshotParser,
        "interval": HeroSnapshotParser,
        "STARTING_ITEM": ItemEventParser,
        "CHAT_MESSAGE_ITEM_PURCHASE": ItemEventParser,
        "CHAT_MESSAGE_ROSHAN_KILL": ObjectiveEventParser,
        "DOTA_COMBATLOG_TEAM_BUILDING_KILL": ObjectiveEventParser,
        "obs": VisionEventParser,
        "sen": VisionEventParser,
        "observers_placed": VisionEventParser,
        # Combat log types
        "DOTA_COMBATLOG_GAME_STATE": CombatLogParser,
        "DOTA_COMBATLOG_DAMAGE": CombatLogParser,
        "DOTA_COMBATLOG_PURCHASE": CombatLogParser,
        "DOTA_COMBATLOG_MULTIKILL": CombatLogParser,
        "DOTA_COMBATLOG_GOLD": CombatLogParser,
        "DOTA_COMBATLOG_XP": CombatLogParser,
        "DOTA_COMBATLOG_ABILITY_TRIGGER": CombatLogParser,
        "DOTA_COMBATLOG_PLAYERSTATS": CombatLogParser,
        "DOTA_COMBATLOG_FIRST_BLOOD": CombatLogParser,
        "DOTA_COMBATLOG_ABILITY": CombatLogParser,
        "DOTA_COMBATLOG_ITEM": CombatLogParser,
        "DOTA_COMBATLOG_MODIFIER_REMOVE": CombatLogParser,
        'DOTA_COMBATLOG_PURCHASE': CombatLogParser,
        "DOTA_COMBATLOG_MODIFIER_ADD": CombatLogParser,
        "DOTA_COMBATLOG_MODIFIER_REMOVE": CombatLogParser,
        "DOTA_COMBATLOG_GOLD": CombatLogParser,
        "DOTA_COMBATLOG_HEAL": CombatLogParser,
        "DOTA_COMBATLOG_MODIFIER_ADD": CombatLogParser,
        "DOTA_COMBATLOG_KILLSTREAK": CombatLogParser,
        "neutral_token": NeutralTokenParser
    }
    parser_cls = type_to_parser.get(event_type)
    return parser_cls() if parser_cls else None
