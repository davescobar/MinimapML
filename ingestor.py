import asyncio
from concurrent.futures import ThreadPoolExecutor
from services.fetcher import ReplayFetcher
from services.uploader import ReplayUploader
from services.postgres import PostgresDB
from services.processor import EventProcessor
from pathlib import Path
import bz2
import os
import logging
from asyncio import WindowsSelectorEventLoopPolicy
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

REPLAYS_DIR = r"F:\\dota_replays"
db_url = os.getenv("DATABASE_URL")
if not db_url:
    logger.critical("DATABASE_URL not set")
    raise ValueError("DATABASE_URL not set")

# Set the WindowsSelectorEventLoopPolicy for compatibility with psycopg
asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

class ReplayIngestor:
    def __init__(self, parser_url: str = None, db: PostgresDB = None):
        self.uploader = ReplayUploader(parser_url) if parser_url else None
        self.db = db

    async def ingest_from_url(self, replay_url: str) -> list[dict]:
        fetcher = ReplayFetcher(replay_url)
        try:
            await asyncio.to_thread(fetcher.download)
            dem_path = await asyncio.to_thread(fetcher.extract)
            return await asyncio.to_thread(self.uploader.upload, dem_path)
        finally:
            await asyncio.to_thread(fetcher.cleanup)

    async def ingest_from_path(self, replay_path: Path, match_id: int = None) -> list[dict]:
        replay_path = Path(replay_path)
        if not replay_path.exists() or replay_path.suffix != ".bz2":
            raise ValueError(f"Invalid replay file path: {replay_path}")
        if replay_path.stat().st_size == 0:
            raise ValueError(f"Replay file is empty: {replay_path}")

        decompressed_path = replay_path.with_suffix("")
        try:
            print(f"Decompressing {replay_path} to {decompressed_path}")
            await asyncio.to_thread(self._decompress_bz2, replay_path, decompressed_path)
        except (EOFError, OSError) as e:
            print(f"❌ Decompression failed: {e}")
            if replay_path.exists():
                replay_path.unlink()
                print(f"🗑️ Deleted corrupted file: {replay_path}")
            if match_id:
                await self.db.reset_download_status(match_id)
                print(f"↩️ Reset download status for match {match_id}")
            raise ValueError(f"Decompression failed for {replay_path}: {e}")

        try:
            replay_events = await self.uploader.upload(decompressed_path)
            processor = EventProcessor(db_writer=self.db)
            await processor.process_events(replay_events, match_id)
        finally:
            if decompressed_path.exists():
                decompressed_path.unlink()

    def _decompress_bz2(self, replay_path: Path, decompressed_path: Path):
        with bz2.BZ2File(replay_path, 'rb') as bz2_file, open(decompressed_path, 'wb') as out_file:
            out_file.write(bz2_file.read())

async def process_match(ingestor: ReplayIngestor, db: PostgresDB, match_id):
    file_path = Path(REPLAYS_DIR) / f"{match_id}.dem.bz2"
    if not file_path.exists():
        logger.error(f"Replay file not found: {file_path}")
        return

    try:
        logger.info(f"Processing match {match_id}...")
        await ingestor.ingest_from_path(file_path, match_id=match_id)
        await db.mark_match_as_parsed(match_id)
        logger.info(f"✅ Match {match_id} parsed successfully.")
    except Exception as e:
        logger.exception(f"❌ Failed to process match {match_id}: {e}")
        await db.mark_match_as_failed(match_id)

async def main(db: PostgresDB, num_workers=16):
    ingestor = ReplayIngestor("http://127.0.0.1:5700/upload", db)

    while True:
        pending_replays = await db.get_pending_replays(limit=250)
        if not pending_replays:
            logger.info("No more pending replays to process.")
            break

        tasks = []
        semaphore = asyncio.Semaphore(num_workers)

        async def worker(match_id):
            async with semaphore:
                await process_match(ingestor, db, match_id)

        for (match_id,) in pending_replays:
            tasks.append(worker(match_id))

        await asyncio.gather(*tasks)
        exit()
async def run():
    num_workers = int(os.getenv("NUM_WORKERS", 20))
    logger.info(f"Starting with {num_workers} workers.")

    db = PostgresDB()

    try:
        await main(db, num_workers=num_workers)
    except Exception:
        logger.exception("An error occurred during execution.")
    finally:
        await db.close()
        logger.info("Database connection pool closed.")

if __name__ == "__main__":
    asyncio.run(run())
