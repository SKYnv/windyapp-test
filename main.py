import asyncio
import logging

from const import *
from parser import MeteoParser
from scrapper import MeteoScrapper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def main():
    #scrap_task = await asyncio.create_task(MeteoScrapper(METEO_DATA_URL, DOWNLOAD_DIR).run())
    parse_task = asyncio.create_task(MeteoParser(DOWNLOAD_DIR, PARSED_DIR).run())

    try:
        scrap_task = None
        if scrap_task.done():
            await parse_task
    except Exception:
        logger.exception("Error")
        await parse_task  # sic!

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
