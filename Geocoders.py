from GIS_Tools import ProxyGrabber
from GIS_Tools.config import HERE_API_KEY, YANDEX_API_KEY

import warnings
from rosreestr2coord import Area, VERSION
from rosreestr2coord.parser import TYPES
if 'yazmo' not in VERSION:
    warnings.warn("Use yazmolod/rosreestr2coord instead default rendrom/rosreestr2coord. It's too more safe and cooler", ImportWarning, stacklevel=2)

import time
import requests
from shapely.geometry import MultiPoint, Point, shape, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry
from shapely import wkt
from GIS_Tools.GeoUtils import convert_to_local_csr
from pathlib import Path
from geopandas import GeoSeries, GeoDataFrame
import json
from pyproj import Transformer
from itertools import chain
import re
import shutil
import logging
import pymongo
logger = logging.getLogger(__name__)
GEOCODING_CACHE_DB = pymongo.MongoClient()['GIS_Tools_Geocoders_cache']


def cache(func):
    def wrapper(*args, **kwargs):
        collection = GEOCODING_CACHE_DB[func.__name__]
        cache_result = collection.find_one({'args': args, 'kwargs': kwargs})
        if cache_result:
            return wkt.loads(cache_result['result'])
        else:
            result = func(*args, **kwargs)
            if isinstance(result, BaseGeometry):
                collection.insert_one({'args': args, 'kwargs': kwargs, 'result': result.wkt})
            return result
    return wrapper

def _validate_kadastr(kadastr):
    kad_check = re.findall(r'[\d:]+', kadastr)
    if len(kad_check) != 1:
        raise TypeError(f'"{kadastr}" не является поддерживаемым форматом для геокодирования')
    return kad_check[0]


KADASTR_TYPES = {
    4: (TYPES['Участки'], TYPES['ОКС']),
    3: (TYPES['Кварталы'], ),
}
def _rosreestr_geom(kadastr, center_only):
    """Обертка для функций библиотеки rosreestr2coord. Пытается найти геометрию и атрибуты кадастрового номера,
    перебирая все известные типы участков
    
    Args:
        kadastr (str): кадастровый номер
        center_only (bool): истина, если необходим только центроид, иначе полигон
    
    Returns:
        shapely.geometry: результат работы rosreestr2coord, переведенный из geojson в shapely-геометрию
        dict: атрибуты участка
    """
    geom_type = 'Point' if center_only else 'Polygon'
    kadastr = _validate_kadastr(kadastr)
    kadastr_len = len(kadastr.split(':'))
    for kadastr_type in KADASTR_TYPES.get(kadastr_len, []):
        logger.debug(f'(PKK Geom) Try geocode {kadastr} ({geom_type}), type {kadastr_type}')
        area = Area(code=kadastr, area_type=kadastr_type, use_cache=True, center_only=center_only, media_path=str(Path(__file__).resolve().parent))
        geom_method = area.to_geojson_center if center_only else area.to_geojson_poly
        attrs = area.get_attrs()
        if attrs:
            feature_string = geom_method()
            if not feature_string:
                logger.info(f'(PKK Geom) No coordinates, only attributes {kadastr} ({geom_type}), type {kadastr_type}')
                return None, attrs
            logger.info(f'(PKK Geom) Geocoded {kadastr} ({geom_type}), type {kadastr_type}')
            feature = json.loads(feature_string)
            if geom_type == 'Point':
                geom = shape(feature['features'][0]['geometry'])
            else:
                geom = shape(feature['geometry'])
            return geom, area.get_attrs()
        else:
            logger.debug(f'(PKK Geom) Nothing found for {kadastr} ({geom_type}), type {kadastr_type}')
    logger.warning(f'(PKK Geom) {geom_type} not found ({kadastr})')
    return None, None

def rosreestr_point(kadastr):
    """Возращает центроид и атрибуты кадастрового участка при помощи библиотеки rosreestr2coords
    
    Args:
        kadastr (str): кадастровый номер
    
    Returns:
        shapely.geometry.Point: центроид кадастрового участка
        dict: атрибуты участка
    """
    return _rosreestr_geom(kadastr, center_only=True)

def rosreestr_polygon(kadastr):
    """Возращает полигоны и атрибуты кадастрового участка при помощи библиотеки rosreestr2coords
    
    Args:
        kadastr (str): кадастровый номер
    
    Returns:
        shapely.geometry.MultiPolygon: полигоны кадастрового участка
        dict: атрибуты участка
    """
    return _rosreestr_geom(kadastr, center_only=False)

def rosreestr_multipolygon(cns):
    """Возращает полигоны и атрибуты кадастрового участка при помощи библиотеки rosreestr2coords

        Args:
            cns (str): строка с кадастровыми номерами

        Returns:
            shapely.geometry.MultiPolygon: полигоны кадастровых участков
            list[dict]: атрибуты участков
        """
    polygons = []
    all_attrs = []
    for cn in iterate_kadastrs(cns):
        geom, attrs = rosreestr_polygon(cn)
        if attrs:
            all_attrs.append(attrs)
        if isinstance(geom, Polygon):
            polygons.append(geom)
        elif isinstance(geom, MultiPolygon):
            for g in geom:
                polygons.append(g)
    return MultiPolygon(polygons), all_attrs

def rosreestr_geodataframe(cns):
    """Возращает полигоны и атрибуты кадастрового участка при помощи библиотеки rosreestr2coords

    Args:
        cns (str): кадастровые номера в свободном формате

    Returns:
        geopandas.GeoDataFrame:
    """
    gdf = GeoDataFrame()
    for cn in iterate_kadastrs(cns):
        geom, attrs = _rosreestr_geom(cn, center_only=False)
        if isinstance(attrs, dict):
            attrs['geometry'] = geom
            gdf = gdf.append(attrs, ignore_index=True)
    return gdf

def iterate_kadastrs(string):
    """Извлекает кадастровые номера из сырой строки
    
    Args:
        string (str): вводная строка
    
    Yields:
        str: кадастровый номер
    """
    if isinstance(string, str):
        for i in re.findall(r'[\d:]+', string):
            yield i.strip(':')

def delete_rosreestr_cache():
    """Удаляет кэш участков с росреестра
    """
    path = Path(__file__).parent.resolve() / 'rosreestr_cache'
    shutil.rmtree(str(path))

@cache
def here(search_string, return_attrs=False, additional_params=None):
    """Геокодирует адрес с помощью HERE API (требуется ключ в переменных среды)
    
    Args:
        search_string (str): адрес
        additional_params (dict, optional): дополнительные параметры запроса
    
    Returns:
        shapely.geometry.Point: ответ сервера - точка в epsg4326
    """
    # избавляемся от переносов строки, табуляции и прочего
    search_string = re.sub(r'\s+', ' ', search_string)
    # убираем повторяющиеся лексемы из запроса
    args = search_string.split(',')
    args = [i.strip().split(' ') for i in args]
    args = chain(*args)
    unique_args = []
    for a in args:
        if a not in unique_args:
            unique_args.append(a)
    search_string = ' '.join(unique_args)
    # генерируем запрос
    params = {
    'q': search_string,
    'apiKey': HERE_API_KEY,
    'lang': 'ru-RU'
    }
    url = f'https://geocode.search.hereapi.com/v1/geocode'
    if additional_params:
        params = {**params, **additional_params}
    r = None
    try:
        r = requests.get(url, params=params)
        if r.status_code in (429, ):
            time.sleep(1)
            return here(search_string, return_attrs=return_attrs, additional_params=None)
        data = r.json()['items']
    except Exception:
        msg = f'(HERE) Error'
        if r:
            msg += f', status_code {r.status_code}'
        logger.exception(msg)
    else:
        if len(data) > 0:
            if len(data) > 1:
                item = sorted(data, key = lambda x: x['scoring']['queryScore'], reverse=True)[0]
            else:
                item = data[0]
            if return_attrs:
                return item
            else:
                x = item['position']['lng']
                y = item['position']['lat']
                pt = Point(x, y)
                return pt
        else:
            logger.warning(f'(HERE) Geocoder return empty list')


@cache
def yandex(search_string):
    """Геокодирует адрес с помощью YANDEX MAP API (требуется ключ в переменных среды)
    
    Args:
        search_string (str): адрес
    
    Returns:
        shapely.geometry.Point: ответ сервера - точка в epsg4326
    """
    endpoint = 'https://geocode-maps.yandex.ru/1.x'
    params = {
        'apikey': YANDEX_API_KEY,
        'geocode': search_string,
        'format': 'json'
    }
    r = requests.get(endpoint, params=params)
    data = r.json()
    if data.get('statusCode') == 403:
        logger.error('(YANDEX) API key blocked')
    else:
        features = data['response']['GeoObjectCollection']['featureMember']
        if features:
            geodata = features[0]['GeoObject']
            point = Point(list(map(float, geodata['Point']['pos'].split(' '))))
            return point
        else:
            logger.warning(f'(YANDEX) Not found "{search_string}"')

def point_to_polygon(pt, radius):
    gs = GeoSeries([pt], crs=4326)
    gs = convert_to_local_csr(gs)
    gs = gs.buffer(radius)
    return gs.to_crs(4326).iloc[0]

def amo_style_geocoding(data, cn_key, address_key):
    cn = data.get(cn_key, None)
    address = data.get(address_key, None)
    if isinstance(cn, str):
        logger.debug(f'Try geocode by cns: {cn}')
        polygons = []
        for cur_cn in iterate_kadastrs(cn):
            logger.debug(f'Current cn: {cur_cn}')
            try:
                poly = rosreestr_polygon(cur_cn)
                if poly:
                    logger.debug(f'Success geocode cn: {cur_cn}')
                    polygons.append(poly)
            except:
                logger.exception(f'Except trying geocode by cadastr {cur_cn}')
        if polygons:
            return MultiPolygon(chain.from_iterable(polygons))
    if isinstance(address, str):
        logger.debug(f'Try geocode by address: {address}')
        try:
            pt = here(address)
            if pt is not None:
                logger.debug(f'Success geocode address: {address}')
                return point_to_polygon(pt, 25)
            else:
                logger.debug(f'Failed geocode address: {address}')
        except:
          logger.exception(f'Except trying geocode by address {address}')
    return MultiPolygon([Polygon([[0.0, 0.0],[0.0,0.1],[0.1,0.1],[0.1,0.0]])])


if __name__ == '__main__':
    r = yandex('область Ленинградская город Волосово улица Ленинградская дом 5')
    print(r)