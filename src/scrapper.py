import asyncio
import bz2
import datetime
import logging
import time
from pathlib import Path

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from yarl import URL

from src.const import HTTP_HEADERS, MAX_DOWNLOAD_THREADS
from src.exceptions import DownloadError
from src.utils import parse_file_name, batched

logger = logging.getLogger(__name__)


class MeteoScrapper:
    def __init__(self, data_url, download_dir):
        self.data_url = data_url
        self.download_dir = download_dir
        self.files_list = []
        logger.info(f"Scrapper started for {data_url=}.")

    async def get_page_data(self) -> str:
        async with aiohttp.ClientSession(headers=HTTP_HEADERS) as session:
            async with session.get(self.data_url) as response:
                if not response.ok:
                    raise DownloadError(response.reason)
                return await response.text()

    # slow # todo future?
    async def extract_files_list(self) -> list[str]:
        soup = BeautifulSoup(await self.get_page_data(), "html.parser")
        links = soup.find_all("a", href=True)
        self.files_list = [tag.get("href") for tag in links][1:]

        await self.filter_files()

        logger.info(f"Found {len(self.files_list) - 1} files.")
        return self.files_list

    async def filter_files(self):
        self.files_list = [name for name in self.files_list if parse_file_name(name).has_coords]

    # todo scheduling
    async def download_links(self):
        start_time = time.time()

        tasks = []
        files_names = await self.extract_files_list()
        folder_name = parse_file_name(files_names[0]).date_hour
        Path(Path(self.download_dir) / folder_name).mkdir(parents=True, exist_ok=True)
        save_path = Path(self.download_dir) / folder_name

        for file_name in files_names:
            tasks.append(asyncio.create_task(self.download(Path(save_path) / file_name)))

        for batch in batched(tasks, MAX_DOWNLOAD_THREADS):
            await asyncio.gather(*batch)

        time_diff = time.time() - start_time
        logger.info(f"Grab time: {time_diff:.2f}s")
        await self.write_report(save_path)

    async def download(self, path):
        async with aiohttp.ClientSession(headers=HTTP_HEADERS) as session:
            base_url = URL(self.data_url)
            async with session.get(base_url / path.name) as response:
                await self.save_file(path, await response.read())

    async def save_file(self, path, stream):
        async with aiofiles.open(path, mode="wb") as file:
            await file.write(stream)
            await file.flush()
            logger.info(f"File {path.name} saved.")
        await self.decompress_file(path)

    async def write_report(self, path):
        async with aiofiles.open(path / "log.txt", mode="w") as file:
            await file.write(f"Files count: {len(self.files_list)}\n")
            await file.write(f"Finished at: {datetime.datetime.today()}\n")
            await file.flush()
            logger.info(f"report {path.name} saved.")

    async def decompress_file(self, path):
        def decompress(path):
            with open(str(path)[:-4], mode="wb") as decompressed, bz2.BZ2File(path, "rb") as file:
                decompressed.write(file.read())
            logger.info(f"File {path} decompressed.")
        await asyncio.to_thread(decompress, path)

    async def run(self):
        await self.download_links()
        return True
