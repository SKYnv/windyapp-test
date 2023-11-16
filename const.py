METEO_DATA_URL: str = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/12/tot_prec/"
NO_DATA: float = -100500
DATE_HOUR_FORMAT: str = "%Y%m%d%H"
DOWNLOAD_DIR: str = "downloads/"
PARSED_DIR: str = "parsed/"

HTTP_HEADERS: dict = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}
MAX_DOWNLOAD_THREADS: int = 10
