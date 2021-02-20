import geopandas as gpd 
from shapely.geometry import * 
import pyproj
from math import floor

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

def tilesOverShape(gdf, delta_x=None, delta_y=None, x_count=None, y_count=None, fill_area_filter_factor = 0.1):
	geoseries = gdf.geometry
	min_bounds = geoseries.bounds.min()
	max_bounds = geoseries.bounds.max()
	x1 = min_bounds['minx']
	y1 = min_bounds['miny']
	x2 = max_bounds['maxx']
	y2 = max_bounds['maxy']
	if delta_x and delta_y:
		dx = delta_x
		dy = delta_y
		x_count = int((x2-x1)/delta_x)
		y_count = int((y2-y1)/delta_y)
	elif x_count and y_count:
		dy = (y2-y1)/y_count
		dx = (x2-x1)/x_count
	old_y = y1
	regions = gpd.GeoSeries(crs = gdf.crs)
	for ix in range(x_count):
		y1 = old_y
		for iy in range(y_count):
			new_geom = box(minx = x1, maxx = x1+dx, miny = y1, maxy = y1+dy)
			if geoseries.apply(lambda x: x.intersects(new_geom)).any():
				reg = gpd.GeoSeries([new_geom], crs = gdf.crs)
				regions = regions.append(reg)
			y1 += dy
		x1 += dx
	if fill_area_filter_factor > 0:
		int_geom = geoseries.intersection(regions)
		regions = regions[int_geom.area / regions.area > fill_area_filter_factor]
	return regions

if __name__=='__main__':
	m = gpd.read_file(r"D:\Litovchenko\YandexDisk\ГИС\Яндекс карты\ckad_edited.gpkg", layer='ckad_with_Lisa')
	g = tilesOverShape(m, 10, 12, 0.25)
	g.to_file('moscow_ckad_tile_test.geojson', driver='GeoJSON')