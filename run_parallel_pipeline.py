import asyncio
import os
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv
from services.postgres import PostgresDB
import bz2
import time
import shutil

from asyncio import WindowsSelectorEventLoopPolicy
asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

# --- Config ---
load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
REPLAYS_DIR = os.getenv("REPLAYS_DIR", "F:\\dota_replays")
PARSER_URL = os.getenv("PARSER_URL", "http://127.0.0.1:5700/upload")
DECOMP_WORKERS = int(os.getenv("DECOMP_WORKERS", 16))
UPLOAD_WORKERS = int(os.getenv("UPLOAD_WORKERS", 6))
PROCESS_WORKERS = int(os.getenv("PROCESS_WORKERS", 16))

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)



def decompress_file(file_path: str) -> tuple[int, str] | None:
    try:
        match_id = int(Path(file_path).stem.split(".")[0])
        decompressed_path = Path(file_path).with_suffix('')
        with bz2.BZ2File(file_path, 'rb') as bz2_file, open(decompressed_path, 'wb') as out_file:
            out_file.write(bz2_file.read())
        return (match_id, str(decompressed_path))
    except Exception as e:
        logger.error(f"Decompression failed for {file_path}: {e}")
        return None


def upload_sync_wrapper(url: str, path: str) -> tuple[int, list[dict]] | None:
    import asyncio
    from services.uploader import ReplayUploader

    match_id = int(Path(path).stem.split(".")[0])

    async def run():
        uploader = ReplayUploader(url)
        return await uploader.upload(Path(path))

    try:
        return match_id, asyncio.run(run())
    except Exception as e:
        logger.error(f"Upload failed for match {match_id}: {e}")
        return None


def run_match_processor(replay_events, match_id):
    from services.processor import EventProcessor
    processor = EventProcessor()
    asyncio.run(processor.process_events(replay_events, match_id))
    return match_id

async def run_pipeline():
    db = PostgresDB()
    try:
        logger.info("Fetching pending replays")
        file_paths = []
        async for chunk in db.get_pending_replays(limit=200):
            start_time = time.time()
            file_paths.extend([str(Path(REPLAYS_DIR) / f"{match_id}.dem.bz2") for (match_id,) in chunk])

            # Phase 1: Decompression
            logger.info("Decompressing...")
            loop = asyncio.get_running_loop()
            with ProcessPoolExecutor(max_workers=DECOMP_WORKERS) as decomp_pool:
                decompressed = await asyncio.gather(*[
                    loop.run_in_executor(decomp_pool, decompress_file, path) for path in file_paths
                ])
            decompressed = [r for r in decompressed if r]
            logger.info(f"Decompressed {len(decompressed)} replays in {time.time() - start_time:.2f} seconds")

            # Phase 2: Upload
            start_time = time.time()
            logger.info("Uploading...")
            with ProcessPoolExecutor(max_workers=UPLOAD_WORKERS) as upload_pool:
                uploaded = await asyncio.gather(*[
                    loop.run_in_executor(upload_pool, upload_sync_wrapper, PARSER_URL, path)
                    for (_, path) in decompressed
                ])
            uploaded = [r for r in uploaded if r]
            logger.info(f"Uploaded {len(uploaded)} replays in {time.time() - start_time:.2f} seconds")

            # Phase 3: Process
            start_time = time.time()
            logger.info("Processing replay events...")
            with ProcessPoolExecutor(max_workers=PROCESS_WORKERS) as process_pool:
                results = await asyncio.gather(*[
                    loop.run_in_executor(process_pool, run_match_processor, replay_events, match_id)
                    for (match_id, replay_events) in uploaded
                ])
            logger.info(f"Processed replay events in {time.time() - start_time:.2f} seconds")

            # Phase 4: Record results
            start_time = time.time()
            completed_match_ids = set(results)
            for match_id in results:
                try:
                    await db.mark_match_as_parsed(match_id)
                except Exception:
                    logger.exception(f"Failed to mark match {match_id} as parsed")
            logger.info(f"Recorded results in {time.time() - start_time:.2f} seconds")

            # Phase 5: Move completed .bz2 files to H:
            start_time = time.time()
            moved_files = []
            for match_id in completed_match_ids:
                bz2_name = f"{match_id}.dem.bz2"
                src_path = os.path.join(REPLAYS_DIR, bz2_name)
                dst_path = os.path.join(r"H:\dota_replays", bz2_name)
                if os.path.exists(src_path):
                    try:
                        shutil.move(src_path, dst_path)
                        moved_files.append(bz2_name)
                    except Exception as e:
                        logger.error(f"Failed to move {bz2_name} to H: {e}")
            logger.info(f"Moved {len(moved_files)} .bz2 files to H: in {time.time() - start_time:.2f} seconds")
            if moved_files:
                for f in moved_files:
                    logger.info(f"  - {f}")
            file_paths = []  # Clear file paths after processing
    finally:
        await db.close()
        logger.info("Pipeline complete.")


if __name__ == "__main__":
    asyncio.run(run_pipeline())
