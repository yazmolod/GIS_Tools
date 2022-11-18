from GIS_Tools.config import HERE_API_KEY
from GIS_Tools import ProxyGrabber
from GIS_Tools import Geocoders
import geopandas as gpd
from pathlib import Path
import re
from shapely.geometry import Point, MultiPoint, Polygon, MultiPolygon
import logging
logger = logging.getLogger(__name__)
import time
import requests
import warnings
import json

RUSSIA_CITIES = RUSSIA_REGIONS = None
RUSSIA_REGIONS_PATH = (Path(__file__).parent / "Russia_boundaries.gpkg").resolve()
def get_cities_gdf():
    """Возращает GeoDataFrame всех городов России
    
    Returns:
        GeoDataFrame: слой городов России с данными из Википедии
    """
    global RUSSIA_CITIES
    if RUSSIA_CITIES is None:
        RUSSIA_CITIES = gpd.read_file(RUSSIA_REGIONS_PATH, layer='cities')
    return RUSSIA_CITIES


def get_regions_gdf():
    """Возращает GeoDataFrame всех регионов России
    
    Returns:
        GeoDataFrame: слой регионов России (OSM)
    """
    global RUSSIA_REGIONS
    if RUSSIA_REGIONS is None:
        RUSSIA_REGIONS = gpd.read_file(RUSSIA_REGIONS_PATH, layer='regions')
    return RUSSIA_REGIONS


def extract_city_by_address(address):
    """Функция поиска названия города в неподготовленной сырой строке
    
    Args:
        address (str): сырая строка адреса
    
    Returns:
        str: название города
    """
    if isinstance(address, str):
        cities = get_cities_gdf()['Город'].tolist()
        cities_pattern = '(' + '|'.join(['\\b' + i + '\\b' for i in set(cities)]) + ')'
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
        reg = russia_cities.loc[russia_cities['geometry'].contains(pt), 'Город']
        if len(reg) > 0:
            return reg.iloc[0]
    else:
        raise TypeError(f'Unknown type {type(pt)}')

def extract_region_by_address(address):
    """Функция поиска региона в неподготовленной сырой строке
    
    Args:
        address (str): сырая строка адреса
    
    Returns:
        str: название региона
    """
    if isinstance(address, str):
        regions = get_cities_gdf()['Регион_re'].tolist()
        regions_pattern = '(' + '|'.join([i for i in set(regions)]) + ')'
        result = re.findall(regions_pattern, address, flags=re.IGNORECASE)
        if result:
            region = get_cities_gdf().loc[get_cities_gdf()['Регион_re'].str.lower() == result[0].lower(), 'Регион'].iloc[0]
            return region
        else:
            city = extract_city(address)
            if city:
                region = get_cities_gdf().loc[get_cities_gdf()['Город'].str.lower() == city.lower(), 'Регион']
                if len(region)>0:
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
        reg = russia_regions.loc[russia_regions['geometry'].contains(pt), 'name']
        if len(reg)>0:
            return reg.iloc[0]
    else:
        raise TypeError(f'Unknown type {type(pt)}')

def add_region_name_to_geodataframe(gdf: gpd.GeoDataFrame) -> gpd.GeoSeries: 
    """Добавляет название региона по точке
    
    Args:
        gdf (gpd.GeoDataFrame): исходный датафрейм с точками
    
    Returns:
        gpd.GeoSeries: название регионов

    Raises:
        TypeError: на входе не GeoDataFrame
    """
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError('Только GeoDataFrame')
    elif len(gdf) == 0:
        raise TypeError('Пустой GeoDataFrame')
    elif 'geometry' not in gdf.columns:
        raise TypeError('Нет колонки geometry')
    elif not isinstance(gdf['geometry'].dropna().iloc[0], Point):
        raise TypeError('Принимается только точечная геометрия')
    else:
        regions = get_regions_gdf().rename({'name': 'region'}, axis=1)
        merged = gpd.sjoin(gdf, regions, how='left', op='within')
        merged = merged.drop('index_right', axis=1)
        return merged


def make_pkk_request(method, url, **kwargs):
    recursion = kwargs.pop('recursion', 0)
    if recursion >= 10:
        raise RecursionError()
    else:
        try:
            r = requests.request(method, url, verify=False, timeout=5, **kwargs)
            return r
        except Exception as e:
            return make_pkk_request(method, url, recursion=recursion+1, **kwargs)


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
    url = 'https://pkk.rosreestr.ru/api/features/'
    for i in range(min_tolerance, max_tolerance):
        params = {
            'text':f'{pt.y} {pt.x}',
            'tolerance':2**i,
            'types':str(types),
            '_':round(time.time() * 1000),
        }
        r = make_pkk_request('get', url, params=params)
        if int(r.json()['total']):
            return r.json()['results']


def here_address_by_point(pt):
    """Ищет адрес по точке при помощи HERE API. Требуется ключ api в переменных среды
    
    Args:
        pt (shapely.geometry.Point): точка поиска
    
    Returns:
        str: строковый адрес
    """
    params = {
        'at':f'{pt.y},{pt.x}',
        'apiKey':HERE_API_KEY,
        'lang':'ru-RU'
    }
    url = f'https://revgeocode.search.hereapi.com/v1/revgeocode'
    r = requests.get(url, params=params)
    items = r.json()['items']
    closest_item = min(items, key=lambda x: x.get('distance', 0))
    return closest_item['address']['label']


def kadastr_in_boundary(geom, cn_type):
    """Функция обращается к api pkk.rosreestr, а именно к функции нахождение кадастровых участков 
    в границах геометрии, обозначенной пользователем
    
    Args:
        geom ([shapely.geometry.Polygon, geopandas.GeoDataFrame, geopandas.GeoSeries]): границы поиска
        cn_type (int): тип участка (перечислены в GIS_Tools.Geocoders.KADASTR_TYPES)
    
    Yields:
        dict: json-ответ сервера с атрибутами участка
    
    Raises:
        TypeError: Неизвестный тип геометрии границ участка
    """
    if isinstance(geom, Polygon):
        polygons = [geom]
    if isinstance(geom, MultiPolygon):
        polygons = [i for i in geom]
    elif isinstance(geom, gpd.GeoDataFrame):
        polygons = geom['geometry'].tolist()
    elif isinstance(geom, gpd.GeoSeries):
        polygons = geom.tolist()
    else:
        raise TypeError(f'Unsupported type {type(geom)}')
    iter_polygons = polygons[:]
    polygons = []
    for i in iter_polygons:
        if isinstance(i, Polygon):
            polygons.append(i)
        elif isinstance(i, MultiPolygon):
            for p in i:
                polygons.append(p)
        else:
            raise TypeError(f'Unsupported geometry type {type(i)}')
    geom_list = []
    for poly in polygons:
        x,y = poly.exterior.coords.xy
        coords = [list(i) for i in list(zip(x,y))]
        geom_list.append({"type":"Polygon","coordinates":[coords]})
    sq = {"type":"GeometryCollection","geometries":geom_list}
    data = {
      "limit":40,
      "skip":0,
      "nameTab":'undefined',
      "indexTab":'undefined',
      "inBounds":True,
      "tolerance":4,
      "searchInUserObjects":True,
      "sq": json.dumps(sq)
      }
    page = 0
    while True:
        logger.info(f'Extract cns from polygon: page {page+1}')
        data['skip'] = page*40
        params = {
            '_': round(time.time() * 1000),
        }
        r = make_pkk_request('post', f'https://pkk.rosreestr.ru/api/features/{cn_type}', params=params, files=data)
        result = r.json()
        if result['total'] == 0:
            logger.debug(f'Empty response')
            return
        else:
            logger.debug(f'Returned {result["total"]} features')
            for feature in result['features']:
                yield feature['attrs']
            page += 1


def kadastr_poly_in_boundary(geom, cn_type):
    """Функция аналогична kadastr_in_boundary, но также добавляет геометрию участка в ответ
    """
    for attr in kadastr_in_boundary(geom, cn_type):
        poly = Geocoders.rosreestr_polygon(attr['cn'])
        attr['geometry'] = poly
        yield attr