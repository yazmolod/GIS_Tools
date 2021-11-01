import geopandas as gpd
from pathlib import Path
import re
from shapely.geometry import Point, MultiPoint
import logging
logger = logging.getLogger(__name__)

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


if __name__ == '__main__':
    reg = extract_region_by_address('Самара')