import bz2
import requests
from pathlib import Path
import asyncio

class ReplayFetcher:
    def __init__(self, replay_url: str, download_dir: str = "/tmp"):
        self.replay_url = replay_url
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.bz2_path = self.download_dir / Path(replay_url).name
        self.extracted_path = self.bz2_path.with_suffix('')

    async def download(self) -> Path:
        def _download():
            response = requests.get(self.replay_url, stream=True)
            response.raise_for_status()
            with open(self.bz2_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return self.bz2_path

        return await asyncio.to_thread(_download)

    async def extract(self) -> Path:
        if not self.bz2_path.exists():
            raise FileNotFoundError(f"Replay file not found: {self.bz2_path}")

        def _extract():
            with bz2.open(self.bz2_path, 'rb') as f_in:
                with open(self.extracted_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            return self.extracted_path

        return await asyncio.to_thread(_extract)

    async def cleanup(self):
        def _cleanup():
            if self.bz2_path.exists():
                self.bz2_path.unlink()
            if self.extracted_path.exists():
                self.extracted_path.unlink()

        await asyncio.to_thread(_cleanup)