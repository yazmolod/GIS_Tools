from GIS_Tools.config import HERE_API_KEY
from GIS_Tools import ProxyGrabber
from GIS_Tools import Geocoders
import geopandas as gpd
from pathlib import Path
import re
from shapely.geometry import Point, MultiPoint, Polygon
import logging
logger = logging.getLogger(__name__)
import time
import requests
import warnings
import json
warnings.filterwarnings("ignore")


RUSSIA_CITIES = RUSSIA_REGIONS = None
RUSSIA_REGIONS_PATH = (Path(__file__).parent / "Russia_boundaries.gpkg").resolve()
def get_cities_gdf():
    global RUSSIA_CITIES
    if RUSSIA_CITIES is None:
        RUSSIA_CITIES = gpd.read_file(RUSSIA_REGIONS_PATH, layer='cities')
    return RUSSIA_CITIES


def get_regions_gdf():
    global RUSSIA_REGIONS
    if RUSSIA_REGIONS is None:
        RUSSIA_REGIONS = gpd.read_file(RUSSIA_REGIONS_PATH, layer='regions')
    return RUSSIA_REGIONS


def extract_city(address):
    if isinstance(address, str):
        cities = get_cities_gdf()['Город'].tolist()
        cities_pattern = '(' + '|'.join(['\\b' + i + '\\b' for i in set(cities)]) + ')'
        result = re.findall(cities_pattern, address, flags=re.IGNORECASE)
        if result:
            return result[0].capitalize()


def extract_region_by_address(address):
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
    if isinstance(pt, Point) or isinstance(pt, MultiPoint):
        russia_regions = get_regions_gdf()
        reg = russia_regions.loc[russia_regions['geometry'].contains(pt), 'name']
        if len(reg)>0:
            return reg.iloc[0]


def kadastr_by_point(pt, types, min_tolerance=0, max_tolerance=3):
    grabber = ProxyGrabber.get_grabber()
    url = 'https://pkk.rosreestr.ru/api/features/'
    for i in range(min_tolerance, max_tolerance):
        params = {
            'text':f'{pt.y} {pt.x}',
            'tolerance':2**i,
            'types':str(types),
            '_':round(time.time() * 1000),
        }
        try:
            r = requests.get(url, params=params, verify=False, proxies=grabber.get_proxy(), timeout=3)
            if int(r.json()['total']):
                return r.json()['results']
        except:
            grabber.next_proxy()
            return kadastr_by_point(pt, types, min_tolerance=i, max_tolerance=max_tolerance)


def here_address_by_point(pt):
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
    def make_request(cn_type, data):
        grabber = ProxyGrabber.get_grabber()
        try:
            r = requests.post(f'https://pkk.rosreestr.ru/api/features/{cn_type}?_={round(time.time() * 1000)}', 
                files=data,
                verify=False,
                proxies=grabber.get_proxy(),
                timeout=3,
                )
        except Exception:
            grabber.next_proxy()
            return make_request(cn_type, data)
        else:
            return r
    if isinstance(geom, Polygon):
        polygons = [geom]
    elif isinstance(geom, gpd.GeoDataFrame):
        polygons = geom['geometry'].tolist()
    elif isinstance(geom, gpd.GeoSeries):
        polygons = geom.tolist()
    else:
        raise TypeError(f'Unsupported type {type(geom)}')
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
        r = make_request(cn_type, data)
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
    for attr in kadastr_in_boundary(geom, cn_type):
        poly = Geocoders.rosreestr_polygon(attr['cn'])
        attr['geometry'] = poly
        yield attr


if __name__ == '__main__':
    from GIS_Tools import FastLogging
    _ = FastLogging.getLogger(__name__)
    _ = FastLogging.getLogger('GIS_Tools.Geocoders')
    gdf = gpd.read_file('test.gpkg')['geometry']
    gpd.GeoDataFrame([x for x in kadastr_poly_in_boundary(gdf, 1)]).to_file('test.gpkg', layer='1', crs=4326, driver='GPKG')
    # gpd.GeoDataFrame([x for x in kadastr_poly_in_boundary(gdf, 5)]).to_file('test.gpkg', layer='5', crs=4326, driver='GPKG')
