import asyncio
import logging

from src.const import *
from src.parser import MeteoParser
from src.scrapper import MeteoScrapper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def main():
    scrap_task = await asyncio.create_task(MeteoScrapper(METEO_DATA_URL, DOWNLOAD_DIR).run())
    parse_task = asyncio.create_task(MeteoParser(DOWNLOAD_DIR, PARSED_DIR).run())

    try:
        if scrap_task:
            await parse_task
    except Exception:
        logger.exception("Error")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
