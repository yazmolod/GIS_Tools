import logging
import os
import re
import warnings
from pathlib import Path

import geopandas as gpd
import requests
from pypkk import PKK
from shapely.geometry import MultiPoint, Point

logger = logging.getLogger(__name__)
RUSSIA_CITIES = RUSSIA_REGIONS = RUSSIA_COUNTRY = None
RUSSIA_REGIONS_PATH = (
    Path(__file__).parent / "gpkg" / "Russia_boundaries.gpkg"
).resolve()


def get_country_gdf():
    """Возращает GeoDataFrame с границами России

    Returns:
        GeoDataFrame:
    """
    global RUSSIA_COUNTRY
    if RUSSIA_COUNTRY is None:
        RUSSIA_COUNTRY = gpd.read_file(RUSSIA_REGIONS_PATH, layer="russia")
    return RUSSIA_COUNTRY


def get_cities_gdf():
    """Возращает GeoDataFrame всех городов России

    Returns:
        GeoDataFrame: слой городов России с данными из Википедии
    """
    global RUSSIA_CITIES
    if RUSSIA_CITIES is None:
        RUSSIA_CITIES = gpd.read_file(RUSSIA_REGIONS_PATH, layer="cities")
    return RUSSIA_CITIES


def get_city_geometry(city_name):
    gdf = get_cities_gdf()
    gdf = gdf[gdf["Город"].str.strip().str.lower() == city_name.strip().lower()]
    if len(gdf) > 0:
        return gdf.geometry.iloc[0]


def get_regions_gdf():
    """Возращает GeoDataFrame всех регионов России

    Returns:
        GeoDataFrame: слой регионов России (OSM)
    """
    global RUSSIA_REGIONS
    if RUSSIA_REGIONS is None:
        RUSSIA_REGIONS = gpd.read_file(RUSSIA_REGIONS_PATH, layer="regions")
    return RUSSIA_REGIONS


def get_region_geometry(region_name):
    gdf = get_regions_gdf()
    gdf = gdf[gdf["name"].str.strip().str.lower() == region_name.strip().lower()]
    if len(gdf) > 0:
        return gdf.geometry.iloc[0]


def extract_city_by_address(address):
    """Функция поиска названия города в неподготовленной сырой строке

    Args:
        address (str): сырая строка адреса

    Returns:
        str: название города
    """
    if isinstance(address, str):
        cities = get_cities_gdf()["Город"].tolist()
        cities_pattern = "(" + "|".join(["\\b" + i + "\\b" for i in set(cities)]) + ")"
        result = re.findall(cities_pattern, address, flags=re.IGNORECASE)
        if result:
            return result[0].capitalize()


def extract_city_by_point(pt):
    """Находит город по точке

    Args:
        pt ([shapely.geometry.Point, shapely.geometry.MultiPoint]): точка поиска

    Returns:
        str: название города

    Raises:
        TypeError: неизвестный тип геометрии
    """
    if isinstance(pt, Point) or isinstance(pt, MultiPoint):
        russia_cities = get_cities_gdf()
        reg = russia_cities.loc[russia_cities["geometry"].contains(pt), "Город"]
        if len(reg) > 0:
            return reg.iloc[0]
    else:
        raise TypeError(f"Unknown type {type(pt)}")


def extract_region_by_address(address):
    """Функция поиска региона в неподготовленной сырой строке

    Args:
        address (str): сырая строка адреса

    Returns:
        str: название региона
    """
    if isinstance(address, str):
        regions = get_cities_gdf()["Регион_re"].tolist()
        regions_pattern = "(" + "|".join([i for i in set(regions)]) + ")"
        result = re.findall(regions_pattern, address, flags=re.IGNORECASE)
        if result:
            region = (
                get_cities_gdf()
                .loc[
                    get_cities_gdf()["Регион_re"].str.lower() == result[0].lower(),
                    "Регион",
                ]
                .iloc[0]
            )
            return region
        else:
            city = extract_city_by_address(address)
            if city:
                region = get_cities_gdf().loc[
                    get_cities_gdf()["Город"].str.lower() == city.lower(), "Регион"
                ]
                if len(region) > 0:
                    return region.iloc[0]


def extract_region_by_point(pt):
    """Находит регион по точке

    Args:
        pt ([shapely.geometry.Point, shapely.geometry.MultiPoint]): точка поиска

    Returns:
        str: название региона

    Raises:
        TypeError: неизвестный тип геометрии
    """
    if isinstance(pt, Point) or isinstance(pt, MultiPoint):
        russia_regions = get_regions_gdf()
        reg = russia_regions.loc[russia_regions["geometry"].contains(pt), "name"]
        if len(reg) > 0:
            return reg.iloc[0]
    else:
        raise TypeError(f"Unknown type {type(pt)}")


def kadastr_by_point(pt, types, min_tolerance=0, max_tolerance=3):
    """Summary

    Args:
        pt (TYPE): Description
        types (TYPE): Description
        min_tolerance (int, optional): Description
        max_tolerance (int, optional): Description

    Returns:
        TYPE: Description
    """
    warnings.warn(
        "Deprecated: use pypkk.search_at_point",
        category=DeprecationWarning,
        stacklevel=2,
    )
    with PKK() as pkk:
        resp = pkk.search_at_point(pt.x, pt.y, types)
        if resp.total:
            return resp.results


def here_address_by_point(pt):
    """Ищет адрес по точке при помощи HERE API. Требуется ключ api в переменных среды

    Args:
        pt (shapely.geometry.Point): точка поиска

    Returns:
        str: строковый адрес
    """
    params = {
        "at": f"{pt.y},{pt.x}",
        "apiKey": os.environ["HERE_API_KEY"],
        "lang": "ru-RU",
    }
    url = "https://revgeocode.search.hereapi.com/v1/revgeocode"
    r = requests.get(url, params=params)
    items = r.json()["items"]
    closest_item = min(items, key=lambda x: x.get("distance", 0))
    return closest_item["address"]["label"]
