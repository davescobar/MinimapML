import asyncio
import aiohttp
import json
from pathlib import Path

class ReplayUploader:
    def __init__(self, upload_url: str):
        self.url = upload_url

    async def upload(self, dem_file_path: Path) -> list[dict]:
        headers = {"Content-Type": "application/octet-stream"}

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
            try:
                with open(dem_file_path, "rb") as f:
                    async with session.post(self.url, data=f, headers=headers) as resp:
                        resp.raise_for_status()
                        try:
                            raw = await resp.read()
                            text = raw.decode("utf-8")
                        except (aiohttp.ClientPayloadError, UnicodeDecodeError) as e:
                            raise RuntimeError(f"❌ Failed to read or decode response body: {e}") from e
            except aiohttp.ClientResponseError as e:
                error_details = await resp.text() if resp else "No response body"
                raise RuntimeError(f"❌ Server responded with error: {e.status} {e.message}. Details: {error_details}") from e
            except FileNotFoundError as e:
                raise RuntimeError(f"❌ File not found: {dem_file_path}") from e
            except asyncio.TimeoutError:
                raise RuntimeError(f"❌ Upload timed out after 90s for file: {dem_file_path}")
            except aiohttp.ClientPayloadError as e:
                raise RuntimeError(f"❌ Payload error: {e}")

            except Exception as e:
                raise RuntimeError(f"❌ Unexpected error during upload: {type(e).__name__}: {e}") from e

        return [json.loads(line) for line in text.splitlines() if line.strip()]


    @classmethod
    def upload_sync(cls, upload_url: str, file_path: str) -> list[dict]:
        """
        Runs the async uploader in a sync context — for ProcessPoolExecutor compatibility.
        """
        async def _run():
            uploader = cls(upload_url)
            return await uploader.upload(Path(file_path))

        return asyncio.run(_run())
