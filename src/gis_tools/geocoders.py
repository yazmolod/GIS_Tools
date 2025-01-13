import logging
import os
import re
import shutil
import time
from itertools import chain
from pathlib import Path

import pymongo
import requests
from geopandas import GeoDataFrame, GeoSeries
from shapely import wkt
from shapely.geometry import MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry

from .geo_utils import convert_to_local_csr

logger = logging.getLogger(__name__)
GEOCODING_CACHE_DB = pymongo.MongoClient()["GIS_Tools_Geocoders_cache"]


def cache(func):
    def wrapper(*args, **kwargs):
        collection = GEOCODING_CACHE_DB[func.__name__]
        cache_result = collection.find_one({"args": args, "kwargs": kwargs})
        if cache_result:
            return wkt.loads(cache_result["result"])
        else:
            result = func(*args, **kwargs)
            if isinstance(result, BaseGeometry):
                collection.insert_one(
                    {"args": args, "kwargs": kwargs, "result": result.wkt}
                )
            return result

    return wrapper


@cache
def here(search_string, return_attrs=False, additional_params=None, restart=0):
    """Геокодирует адрес с помощью HERE API (требуется ключ в переменных среды)

    Args:
        search_string (str): адрес
        additional_params (dict, optional): дополнительные параметры запроса

    Returns:
        shapely.geometry.Point: ответ сервера - точка в epsg4326
    """
    TRIES = 5
    # избавляемся от переносов строки, табуляции и прочего
    search_string = re.sub(r"\s+", " ", search_string)
    # убираем повторяющиеся лексемы из запроса
    args = search_string.split(",")
    args = [i.strip().split(" ") for i in args]
    args = chain(*args)
    unique_args = []
    for a in args:
        if a not in unique_args:
            unique_args.append(a)
    search_string = " ".join(unique_args)
    # генерируем запрос
    params = {"q": search_string, "apiKey": os.environ["HERE_API_KEY"], "lang": "ru-RU"}
    url = "https://geocode.search.hereapi.com/v1/geocode"
    if additional_params:
        params = {**params, **additional_params}
    r = None
    try:
        r = requests.get(url, params=params)
    except Exception:
        msg = "(HERE) Error"
        if r:
            msg += f", status_code {r.status_code}"
        logger.exception(msg)
        return
    if r.status_code in (429,):
        if restart > TRIES:
            raise RecursionError
        logger.info(f"Too many requests, sleep 10... [{restart}/{TRIES}], {r.text}")
        time.sleep(3)
        return here(
            search_string,
            return_attrs=return_attrs,
            additional_params=None,
            restart=restart + 1,
        )
    data = r.json()["items"]
    if len(data) > 0:
        if len(data) > 1:
            item = sorted(data, key=lambda x: x["scoring"]["queryScore"], reverse=True)[
                0
            ]
        else:
            item = data[0]
        if return_attrs:
            return item
        else:
            x = item["position"]["lng"]
            y = item["position"]["lat"]
            pt = Point(x, y)
            return pt
    else:
        logger.warning("(HERE) Geocoder return empty list")


@cache
def yandex(search_string):
    """Геокодирует адрес с помощью YANDEX MAP API (требуется ключ в переменных среды)

    Args:
        search_string (str): адрес

    Returns:
        shapely.geometry.Point: ответ сервера - точка в epsg4326
    """
    endpoint = "https://geocode-maps.yandex.ru/1.x"
    params = {
        "apikey": os.environ["YANDEX_GEOCODING_API_KEY"],
        "geocode": search_string,
        "format": "json",
    }
    r = requests.get(endpoint, params=params)
    data = r.json()
    if data.get("statusCode") == 403:
        logger.error("(YANDEX) API key blocked")
    else:
        features = data["response"]["GeoObjectCollection"]["featureMember"]
        if features:
            geodata = features[0]["GeoObject"]
            point = Point(list(map(float, geodata["Point"]["pos"].split(" "))))
            return point
        else:
            logger.warning(f'(YANDEX) Not found "{search_string}"')


def point_to_polygon(pt, radius):
    gs = GeoSeries([pt], crs=4326)
    gs = convert_to_local_csr(gs)
    gs = gs.buffer(radius)
    return gs.to_crs(4326).iloc[0]
