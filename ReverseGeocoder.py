from GIS_Tools.config import HERE_API_KEY
import geopandas as gpd
from pathlib import Path
import re
from shapely.geometry import Point, MultiPoint
import logging
logger = logging.getLogger(__name__)
from GIS_Tools import ProxyGrabber
import time
import requests
import warnings
warnings.filterwarnings("ignore")

RUSSIA_CITIES = RUSSIA_REGIONS = None
def get_cities_df():
    global RUSSIA_CITIES
    if RUSSIA_CITIES is None:
        RUSSIA_CITIES = gpd.pd.read_json((Path(__file__).parent / "russia_cities.json").resolve())
    return RUSSIA_CITIES

def get_regions_gdf():
    global RUSSIA_REGIONS
    if RUSSIA_REGIONS is None:
        RUSSIA_REGIONS_PATH = (Path(__file__).parent / "Russia_regions.gpkg").resolve()
        RUSSIA_REGIONS = gpd.read_file(RUSSIA_REGIONS_PATH)
    return RUSSIA_REGIONS

def extract_city(address):
    if isinstance(address, str):
        cities = get_cities_df()['Город'].tolist()
        cities_pattern = '(' + '|'.join(['\\b' + i + '\\b' for i in set(cities)]) + ')'
        result = re.findall(cities_pattern, address, flags=re.IGNORECASE)
        if result:
            return result[0].capitalize()

def extract_region_by_address(address):
    if isinstance(address, str):
        regions = get_cities_df()['Регион_re'].tolist()
        regions_pattern = '(' + '|'.join([i for i in set(regions)]) + ')'
        result = re.findall(regions_pattern, address, flags=re.IGNORECASE)
        if result:
            region = get_cities_df().loc[get_cities_df()['Регион_re'].str.lower() == result[0].lower(), 'Регион'].iloc[0]
            return region
        else:
            city = extract_city(address)
            if city:
                region = get_cities_df().loc[get_cities_df()['Город'].str.lower() == city.lower(), 'Регион']
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


if __name__ == '__main__':
    import ThreadsUtils
    import FastLogging
    from tqdm import tqdm
    tqdm.pandas()
    gdf = gpd.read_file(r"D:\Litovchenko\YandexDisk\ГИС\_quick tasks\гаражи\floors\garage.gpkg", layer='with_floors')
    gdf['here.address'] = gdf['geometry'].progress_apply(here_address_by_point)
    gdf.to_file(r"D:\Litovchenko\YandexDisk\ГИС\_quick tasks\гаражи\floors\garage.gpkg", layer='with_floors_and_address', driver='GPKG')