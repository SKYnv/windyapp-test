from datetime import datetime, timedelta

from pydantic.dataclasses import dataclass

from src.const import DATE_HOUR_FORMAT, DATE_HOUR_OUTPUT_FORMAT, DATAFRAME_DATE_TIME_FORMAT


@dataclass
class FileName:
    model_name: str
    country_name: str
    type1: str
    level: str
    date_hour: str
    offset: int
    info: str

    @property
    def get_date_hour(self):
        return datetime.strptime(self.date_hour, DATE_HOUR_FORMAT) + timedelta(hours=self.offset)

    @property
    def output_date(self):
        return self.get_date_hour.strftime(DATE_HOUR_OUTPUT_FORMAT)

    @property
    def dataframe_time(self):
        return self.get_date_hour.strftime(DATAFRAME_DATE_TIME_FORMAT)

    @property
    def get_full_name(self):
        return "_".join(str(v) for v in self.__dict__.values())

    @property
    def has_coords(self):
        return self.type1 == "regular-lat-lon"
