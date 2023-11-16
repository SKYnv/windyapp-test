import asyncio
import bz2
import io
import logging
from pathlib import Path

import pandas as pd
import xarray as xr
from aiofiles import open
from aiofiles.os import listdir, scandir

from const import NO_DATA
from utils import parse_file_name

logger = logging.getLogger(__name__)


class MeteoParser:
    def __init__(self, input_folder: str, output_dir: str):
        self.input_folder: str = input_folder
        self.output_dir: str = output_dir
        self.directories: set | None = None

    async def scan_directories(self):
        list_dirs = await listdir(Path(self.input_folder))
        self.directories = set(list_dirs)
        return list_dirs

    async def scan_directory(self):
        for directory in self.directories:
            root_path = Path(self.input_folder) / directory
            path = root_path / "log.txt"
            if path.is_file():
                logger.info(f"Start to parse files in {root_path}.")
                await self.parse_directory(root_path)

    async def parse_directory(self, path):
        logger.info(f"Parse directory {path}")
        files_list = sorted([x.name for x in await scandir(Path(path)) if x.name.endswith("grib2")])

        parsed_main_directory_path = Path(self.output_dir)
        for model_name in self.get_models_name(files_list):
            (parsed_main_directory_path / model_name).mkdir(parents=True, exist_ok=True)

        for file_name in files_list:
            await self.parse_file(Path(path) / file_name)

    def get_models_name(self, files_list):
        models_name = set()
        for file_name in files_list:
            if file_name == "log.txt":
                continue
            file = parse_file_name(file_name)
            models_name.add(file.model_name)
        return models_name

    async def parse_file(self, path):
        data = await self.read_file(path)
        # todo есть датафрейм, фильтровать и сохранить
        print(data.keys())
        #print(data["time"].iloc[0])

        # f_ = data["time"].dt.minute == 0
        # print(data.loc[f_])

        #mock
        data = b"test"

        save_path = await self.get_save_path(path)
        await self.save_file(save_path, data)

    async def get_save_path(self, path):
        parsed_name = parse_file_name(path.name)
        directory = Path(self.output_dir) / parsed_name.model_name / parsed_name.output_date
        directory.mkdir(parents=True, exist_ok=True)
        return directory / "PRATE.wgf4"

    async def save_file(self, path, data):
        stream = await asyncio.to_thread(self.zip_file, data)

        async with open(path, mode="wb") as file:
            await file.write(stream)
            await file.flush()
            logger.info(f"File {path} saved.")

    def zip_file(self, data):
        stream = io.BytesIO(data)
        return bz2.compress(stream.read())

    def get_header(self, lat1, lat2, lon1, lon2, step_lat, step_lon, multiplier):
        """
        :param lat1: bottom latitude
        :param lat2: top latitue
        :param lon1: left longtitude
        :param lon2: right longtitude
        """
        return [lat1, lat2, lon1, lon2, step_lat, step_lon, multiplier, NO_DATA]

    async def read_file(self, path) -> pd.DataFrame:
        logger.info(f"Reading {path}")
        try:
            # backend_kwargs={"extra_coords": {"startStep": "step"}}
            ds = xr.open_dataset(path, engine="cfgrib", )
            return ds.to_dataframe()

        except Exception as exc:
            logger.exception("Unknown error")

    async def run(self):
        logger.info(f"MeteoParse run")
        await self.scan_directories()
        await self.scan_directory()