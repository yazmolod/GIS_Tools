from shapely.geometry import Point, MultiPolygon
import logging

from gis_tools import geocoders

logger = logging.getLogger(__name__)

def test_rosreestr_point():
    geom, attrs = geocoders.rosreestr_point('77:09:0001003:54')
    logger.info(f"{geom=}, {attrs=}")
    assert attrs is not None
    assert isinstance(geom, Point) 
    
def test_rosreestr_polygon():
    geom, attrs = geocoders.rosreestr_polygon('77:09:0001003:54')
    logger.info(f"{geom=}, {attrs=}")
    assert attrs is not None
    assert isinstance(geom, MultiPolygon) 
    