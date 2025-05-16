import os
import requests
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

DB_DSN = os.getenv("DATABASE_URL")
URL = "https://raw.githubusercontent.com/odota/dotaconstants/master/build/heroes.json"

def fetch_heroes():
    resp = requests.get(URL)
    resp.raise_for_status()
    return list(resp.json().values())

def insert_heroes(heroes):
    with psycopg.connect(DB_DSN, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            for hero in heroes:
                cur.execute("""
                    INSERT INTO dota_heroes (
                        id, name, localized_name, primary_attr, attack_type, roles,
                        base_health, base_health_regen, base_mana, base_mana_regen,
                        base_armor, base_mr, base_attack_min, base_attack_max,
                        base_str, base_agi, base_int,
                        str_gain, agi_gain, int_gain,
                        attack_range, projectile_speed, attack_rate,
                        base_attack_time, attack_point, move_speed, turn_rate,
                        cm_enabled, legs, day_vision, night_vision
                    ) VALUES (
                        %(id)s, %(name)s, %(localized_name)s, %(primary_attr)s, %(attack_type)s, %(roles)s,
                        %(base_health)s, %(base_health_regen)s, %(base_mana)s, %(base_mana_regen)s,
                        %(base_armor)s, %(base_mr)s, %(base_attack_min)s, %(base_attack_max)s,
                        %(base_str)s, %(base_agi)s, %(base_int)s,
                        %(str_gain)s, %(agi_gain)s, %(int_gain)s,
                        %(attack_range)s, %(projectile_speed)s, %(attack_rate)s,
                        %(base_attack_time)s, %(attack_point)s, %(move_speed)s, %(turn_rate)s,
                        %(cm_enabled)s, %(legs)s, %(day_vision)s, %(night_vision)s
                    )
                    ON CONFLICT (id) DO NOTHING
                """, hero)

if __name__ == "__main__":
    heroes = fetch_heroes()
    insert_heroes(heroes)
    print(f"✅ Inserted {len(heroes)} heroes")
