import asyncio
import io
import logging
from pathlib import Path
from struct import pack

import pandas as pd
import xarray as xr
from aiofiles import open
from aiofiles.os import listdir, scandir

from src.const import NO_DATA, DEFAULT_MULTIPLIER
from src.exceptions import ParseError
from src.utils import parse_file_name, process_nan, convert_float

logger = logging.getLogger(__name__)


class MeteoParser:
    def __init__(self, input_folder: str, output_dir: str):
        self.input_folder: str = input_folder
        self.output_dir: str = output_dir
        self.directories: set | None = None
        self.last_hour_data = pd.DataFrame()

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

        tasks = []
        for file_name in files_list:
            tasks.append(asyncio.create_task(self.parse_file(Path(path) / file_name)))
        await asyncio.gather(*tasks)

    def get_models_name(self, files_list):
        models_name = set()
        for file_name in files_list:
            if file_name == "log.txt":
                continue
            file = parse_file_name(file_name)
            models_name.add(file.model_name)
        return models_name

    # TODO refactoring
    # TODO xrange?
    async def parse_file(self, path):
        data = await self.read_file(path)

        # фильтруем данные на начало часа
        parsed_name = parse_file_name(path.name)
        time = parsed_name.dataframe_time
        at_start_hour = data["valid_time"] <= time
        data = data[at_start_hour]

        start_lat, start_lon, end_lat, end_lon = None, None, None, None
        lat_step, lon_step = 0, 0
        buffer = io.BytesIO()

        # костыль
        def shift_index_by_offset(index, offset):
            # Обработка последнего файла у которого почему-то отличается кол-во столбцов и индекс
            # WARNING:cfgrib.messages:Ignoring index file 'icon-d2_germany_regular-lat-lon_single-level_
            # 2023111612_048_2d_tot_prec.grib2.923a8.idx' older than GRIB file
            if offset == 48:
                index -= 1
            return index

        try:
            iterator = iter(data.loc[:, ["tp"]].iterrows())

            while not all([start_lat, start_lon, lat_step, lon_step]):
                row = next(iterator)
                if not start_lat:
                    start_lat = row[0][shift_index_by_offset(1, parsed_name.offset)]

                if not start_lon:
                    start_lon = row[0][shift_index_by_offset(2, parsed_name.offset)]

                if lat_step == 0:
                    lat_step = start_lat - row[0][shift_index_by_offset(1, parsed_name.offset)]

                if lon_step == 0:
                    lon_step = start_lon - row[0][shift_index_by_offset(2, parsed_name.offset)]

            for row in data.tail(10).loc[:, ["tp"]].iterrows():
                end_lat = row[0][shift_index_by_offset(1, parsed_name.offset)]
                end_lon = row[0][shift_index_by_offset(2, parsed_name.offset)]
        except Exception as exc:
            raise ParseError(f"Parsing error of {path.name}")

        header = self.get_header(start_lat, end_lat, start_lon, end_lon, lat_step, lon_step)

        current_hour_data = data["tp"]
        if len(self.last_hour_data):
            current_hour_data = data["tp"] - self.last_hour_data
        self.last_hour_data = data["tp"]

        for item in header:
            buffer.write(pack("<f", item))
        for item in current_hour_data.tolist():
            buffer.write(pack("<f", process_nan(item)))
        buffer.seek(0)

        save_path = self.get_save_path(path)
        await self.save_file(save_path, buffer.read())

    def get_save_path(self, path):
        parsed_name = parse_file_name(path.name)
        directory = Path(self.output_dir) / parsed_name.model_name / parsed_name.output_date
        directory.mkdir(parents=True, exist_ok=True)
        return directory / "PRATE.wgf4"

    async def save_file(self, path, stream):
        async with open(path, mode="wb") as file:
            await file.write(stream)
            await file.flush()
            logger.info(f"File {path} saved.")

    def get_header(self, lat1, lat2, lon1, lon2, step_lat, step_lon):
        """
        :param lat1: bottom latitude
        :param lat2: top latitue
        :param lon1: left longtitude
        :param lon2: right longtitude
        """
        return [
            convert_float(lat1, DEFAULT_MULTIPLIER),
            convert_float(lat2, DEFAULT_MULTIPLIER),
            convert_float(lon1, DEFAULT_MULTIPLIER),
            convert_float(lon2, DEFAULT_MULTIPLIER),
            convert_float(step_lat, DEFAULT_MULTIPLIER),
            convert_float(step_lon, DEFAULT_MULTIPLIER),
            DEFAULT_MULTIPLIER,
            NO_DATA
        ]

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
