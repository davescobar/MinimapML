from psycopg import connect
from psycopg_pool import AsyncConnectionPool
import os
CHUNK_SIZE = 25_000

class PostgresDB:
    def __init__(self):
        self.pool = AsyncConnectionPool(conninfo=os.getenv("DATABASE_URL"), max_size=90, open=True, max_waiting=240)

    async def insert_hero_snapshots(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No rows to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} hero_snapshot rows")

        values = [
            (
                row["time"], row["match_id"], row["slot"], row["hero_id"], row["x"], row["y"], row["z"],
                row["hp"], row["mana"], row["level"], row["gold"], row["networth"], row["xp"],
                row["kills"], row["deaths"], row["assists"], row["denies"], row["lh"],
                row["obs_placed"], row["sen_placed"], row["towers_killed"], row["roshans_killed"],
                row["creeps_stacked"], row["camps_stacked"], row["rune_pickups"],
                row["teamfight_participation"], row["firstblood_claimed"], row["life_state"],
                row["randomed"], row["pred_vict"]
            )
            for row in rows
        ]

        

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        """
                        INSERT INTO hero_snapshots (
                            time_offset, match_id, slot, hero_id, x, y, z, hp, mana, level, gold, networth, xp,
                            kills, deaths, assists, denies, lh, obs_placed, sen_placed, towers_killed,
                            roshans_killed, creeps_stacked, camps_stacked, rune_pickups,
                            teamfight_participation, firstblood_claimed, life_state,
                            randomed, pred_vict
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s
                        )
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )

    async def insert_abilitylevel(self, rows: list[dict]):
        if not rows:
            return

        
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(rows), CHUNK_SIZE):
                    chunk = rows[i:i + CHUNK_SIZE]

                    values = [
                        (
                            row["time"],  # already relative seconds
                            row["match_id"],
                            row.get("targetname"),
                            row.get("valuename"),
                            row.get("abilitylevel")
                        )
                        for row in chunk
                    ]

                    await cur.executemany(
                        """
                        INSERT INTO ability_levels (
                            time_offset, match_id, targetname, valuename, abilitylevel
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        values
                    )

    async def insert_combatlog(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No combat event rows to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} combat event rows")

        values = [
            (
                int(row["time"]),
                row["match_id"],
                row.get("type"),
                row.get("attackername"),
                row.get("targetname"),
                row.get("inflictor"),
                row.get("abilitylevel"),
                row.get("gold"),
                row.get("xp"),
                row.get("damage"),
                row.get("variant"),
                row.get("attackerhero"),
                row.get("targethero"),
                row.get("attackerillusion"),
                row.get("targetillusion"),
                row.get("x"),
                row.get("y"),
                row.get("z"),
                row.get("stun_duration"),
                row.get("slow_duration"),
                row.get("slot"),
                row.get("player1"),
                row.get("player2"),
                row.get("gold_reason"),
                row.get("xp_reason"),
                row.get("unit")
            )
            for row in rows
        ]

        
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        """
                        INSERT INTO combat_events (
                            time_offset, match_id, type, attackername, targetname, inflictor,
                            abilitylevel, gold, xp, damage, variant, attackerhero, targethero,
                            attackerillusion, targetillusion, x, y, z, stun_duration, slow_duration,
                            slot, player1, player2, gold_reason, xp_reason, unit
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )
    CHUNK_SIZE = 2000

    async def insert_neutraltoken(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No neutral token rows to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} neutral token rows")

        values = [
            (
                int(row["time"]),
                row["match_id"],
                row.get("type"),  # will be 'neutral_token'
                row.get("key"),   # e.g., 'Tier1Token'
                row.get("slot")
            )
            for row in rows
        ]

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        """
                        INSERT INTO neutral_tokens (
                            time_offset, match_id, type, key, slot
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )

    async def insert_itemevent(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No item event rows to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} item event rows")

        values = [
            (
                int(row["time"]),
                row["match_id"],
                row.get("slot"),
                row.get("type"),
                row.get("item"),
                row.get("player1"),
                row.get("player2"),
                row.get("itemslot"),
                row.get("charges")
            )
            for row in rows
        ]

        
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        """
                        INSERT INTO item_events (
                            time_offset, match_id, slot, type, item,
                            player1, player2, itemslot, charges
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )

    async def insert_deathevent(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No death event rows to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} death event rows")
                          
        values = [
            (
                int(row["time"]),
                row["match_id"],
                row.get("attackername"),
                row.get("targetname"),
                row.get("attackerhero"),
                row.get("targethero"),
                row.get("attackerillusion"),
                row.get("targetillusion"),
                row.get("x"),
                row.get("y"),
                row.get("z"),
                row.get("slot"),
                row.get("player1"),
                row.get("player2"),
                row.get("unit")
            )
            for row in rows
        ]

        
        static_placeholders = (
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
        )

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        f"""
                        INSERT INTO death_events (
                            time_offset, match_id, attackername, targetname,
                            attackerhero, targethero, attackerillusion, targetillusion,
                            x, y, z, slot, player1, player2, unit
                        ) VALUES ({static_placeholders})
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )
    
    async def insert_visionevent(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No vision event rows to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} vision event rows")

        values = [
            (
                int(row["time"]),
                row["match_id"],
                row.get("slot"),
                row.get("type"),
                row.get("x"),
                row.get("y"),
                row.get("z"),
                row.get("unit"),
                row.get("life_state"),
                row.get("entityleft"),
                row.get("ehandle")
            )
            for row in rows
        ]

        
        static_placeholders = (
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
        )

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        f"""
                        INSERT INTO vision_events (
                            time_offset, match_id, slot, type,
                            x, y, z, unit, life_state, entityleft, ehandle
                        ) VALUES ({static_placeholders})
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )

    async def insert_draftevent(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No draft event rows to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} draft event rows")

        values = [
            (
                int(row["time"]),
                row["match_id"],
                row.get("type"),
                row.get("slot"),
                row.get("stage"),
                row.get("hero_id"),
                row.get("draft_order"),
                row.get("pick"),
                row.get("draft_active_team"),
                row.get("draft_extime0"),
                row.get("draft_extime1")
            )
            for row in rows
        ]

        
        static_placeholders = (
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
        )

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        f"""
                        INSERT INTO draft_events (
                            time_offset, match_id, type, slot, stage, hero_id,
                            draft_order, pick, draft_active_team, draft_extime0, draft_extime1
                        ) VALUES ({static_placeholders})
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )

    async def insert_objectiveevent(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No objective event rows to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} objective event rows")

        values = [
            (
                int(row["time"]),
                row["match_id"],
                row.get("type"),
                row.get("targetname"),
                row.get("attackername"),
                row.get("x"),
                row.get("y"),
                row.get("z"),
                row.get("unit"),
                row.get("variant"),
                row.get("slot"),
                row.get("player1"),
                row.get("player2")
            )
            for row in rows
        ]

        
        static_placeholders = (
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
        )

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        f"""
                        INSERT INTO objective_events (
                            time_offset, match_id, type, targetname, attackername,
                            x, y, z, unit, variant, slot, player1, player2
                        ) VALUES ({static_placeholders})
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )
    
    async def insert_actionevent(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No action event rows to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} action event rows")

        values = [
            (
                int(row["time"]),
                row["match_id"],
                row.get("slot"),
                row.get("key")
            )
            for row in rows
        ]

        
        static_placeholders = "%s, %s, %s, %s"

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        f"""
                        INSERT INTO action_events (
                            time_offset, match_id, slot, key
                        ) VALUES ({static_placeholders})
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )

    async def insert_neutralitemevent(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No neutral item events to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} neutral item events")

        values = [
            (
                int(row["time"]),
                row["match_id"],
                row.get("slot"),
                row.get("key"),
                row.get("is_neutral_active_drop"),
                row.get("is_neutral_passive_drop")
            )
            for row in rows
        ]

        
        static_placeholders = "%s, %s, %s, %s, %s, %s"

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        f"""
                        INSERT INTO neutral_item_events (
                            time_offset, match_id, slot, key,
                            is_neutral_active_drop, is_neutral_passive_drop
                        ) VALUES ({static_placeholders})
                        ON CONFLICT DO NOTHING
                        """,
                        chunk
                    )

    async def insert_playerheromap(self, rows: list[dict]):
        if not rows:
            print("[DEBUG] No player → hero mappings to insert.")
            return

        print(f"[DEBUG] Preparing to insert {len(rows)} player hero map rows")

        # Deduplicate by (match_id, slot) — keep last entry (usually most complete)
        deduped = {}
        for row in rows:
            key = (row["match_id"], row["slot"])
            deduped[key] = row  # newer replaces older

        values = [
            (
                row["match_id"],
                row["slot"],
                row["hero_id"],
                row.get("steam_id")
            )
            for row in deduped.values()
        ]

        static_placeholders = "%s, %s, %s, %s"

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK_SIZE):
                    chunk = values[i:i + CHUNK_SIZE]
                    await cur.executemany(
                        f"""
                        INSERT INTO player_hero_map (
                            match_id, slot, hero_id, steam_id
                        ) VALUES ({static_placeholders})
                        ON CONFLICT (match_id, slot) DO UPDATE
                        SET
                            hero_id = EXCLUDED.hero_id,
                            steam_id = COALESCE(player_hero_map.steam_id, EXCLUDED.steam_id)
                        """,
                        chunk
                    )

    async def load_hero_name_map(self) -> dict[str, int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT name, id FROM dota_heroes")
                return dict(await cur.fetchall())

    async def write(self, table: str, rows: list[dict]):
        method_map = {
            "HeroSnapshotParser": self.insert_hero_snapshots,
            "AbilityLevelParser": self.insert_abilitylevel,
            "CombatLogParser": self.insert_combatlog,
            "ItemEventParser": self.insert_itemevent,
            "DeathEventParser": self.insert_deathevent,
            "VisionEventParser": self.insert_visionevent,
            "DraftEventParser": self.insert_draftevent,
            "ObjectiveEventParser": self.insert_objectiveevent,
            "ActionEventParser": self.insert_actionevent,
            "PlayerHeroMapParser": self.insert_playerheromap,
            "NeutralItemEventParser": self.insert_neutralitemevent,
        }

        insert_fn = method_map.get(table)
        if insert_fn:
            await insert_fn(rows)
        else:
            print(f"[WARN] No insert function mapped for {table}")
    async def reset_download_status(self, match_id: int):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE match_metadata
                    SET downloaded = FALSE,
                        download_failed = FALSE,
                        failure_reason = NULL,
                        downloaded_at = NULL
                    WHERE match_id = %s;
                """, (match_id,))

    async def get_pending_replays(self, limit: int = 100):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                offset = 0
                while True:
                    await cur.execute("""
                        SELECT match_id FROM match_metadata
                        WHERE downloaded = TRUE
                          AND download_failed = FALSE
                          AND replay_url IS NOT NULL
                          AND parsed IS NULL
                        ORDER BY match_id DESC
                        LIMIT %s OFFSET %s;
                    """, (limit, offset))
                    rows = await cur.fetchall()
                    if not rows:
                        break
                    yield rows
                    offset += limit

    async def mark_match_as_parsed(self, match_id: int):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE match_metadata
                    SET parsed = TRUE
                    WHERE match_id = %s;
                """, (match_id,))

    async def mark_match_as_failed(self, match_id: int):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE match_metadata
                    SET parsed = FALSE
                    WHERE match_id = %s;
                """, (match_id,))

    async def close(self):
        await self.pool.close()
