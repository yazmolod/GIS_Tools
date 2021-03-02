import geopandas as gpd 
from shapely.geometry import * 
import pyproj
from math import floor
from itertools import product
import time
import numpy as np
from shapely.geometry import Polygon, MultiPolygon

def convertToLocalCsr(geom):
	crs = geom.crs
	if not crs:
		geom.crs = pyproj.CRS('EPSG:4326')
	elif crs.to_string()!='EPSG:4326':
		geom = geom.to_crs('EPSG:4326')
	polus = 6 if geom.geometry.y.min() > 0 else 7
	zone = floor((geom.geometry.x.min()+180)/6)
	utm = 'EPSG:32%d%02d' % (polus, zone)
	return geom.to_crs(utm)

def filterByIntersectProcent(main_polygon : gpd.GeoSeries, tiles : gpd.GeoSeries):
	intersections = gpd.GeoSeries(crs = main_polygon.crs)
	for i,v in tiles.iteritems():
		int_geom = main_polygon.intersection(v)
		if int_geom.area / v.area >= 0.5:
			intersections = intersections.append(v, sort=False)

def tilesOverShape(geom, delta_x=None, delta_y=None, x_count=None, y_count=None, fill_area_filter_factor = 0.0):
	dt = time.time()
	if not isinstance(geom, (Polygon, MultiPolygon)):
		raise TypeError(f'Unexpected type for geometry: {type(gdf)}')
	x1, y1, x2, y2 = geom.bounds
	if delta_x and delta_y:
		xs = np.arange(x1, x2, delta_x)
		ys = np.arange(y1, y2, delta_y)
	elif x_count and y_count:
		xs = np.linspace(x1, x2, x_count, endpoint=False)
		ys = np.linspace(y1, y2, y_count, endpoint=False)
		delta_x = xs[1] - xs[0]
		delta_y = ys[1] - ys[0]
	df = gpd.pd.DataFrame(product(xs,ys), columns=['minx', 'miny'])
	df['maxx'] = df['minx'] + delta_x
	df['maxy'] = df['miny'] + delta_y
	df['geometry'] = df.apply(lambda x: box(**x.to_dict()), axis=1)
	regions = gpd.GeoSeries(df['geometry'])
	if fill_area_filter_factor > 0:
		int_area = regions.intersection(geom).area
		area_ratio =  int_area / regions.area
		return regions[area_ratio > fill_area_filter_factor]
	else:
		regions = regions[regions.intersects(geom)]
		print(time.time() - dt)
		return regions

if __name__=='__main__':
	dx, dy = 0.05,0.018503645103786864
	mo = gpd.read_file(r"C:\Users\yazmo\YandexDisk\ГИС\Mосковская область.gpkg").geometry.iloc[0]
	tiles1 = tilesOverShape(mo, delta_x=dx, delta_y=dy)
	# tiles2 = tilesOverShape(mo, x_count=5, y_count=5)
	tiles1.to_file('geoportal.gpkg', layer='tiles1', driver='GPKG', crs=4326)
	# tiles2.to_file('geoportal.gpkg', layer='tiles2', driver='GPKG', crs=4326)
