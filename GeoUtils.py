import geopandas as gpd 
from shapely.geometry import * 
import pyproj
from math import floor
from itertools import product
import numpy as np
from shapely.geometry import Polygon, MultiPolygon
import os
from ThreadsUtils import pool_execute
import gdal
from pathlib import Path
import shutil
import logging 

logger = logging.getLogger(__name__)

class TilerCreator:
	"""
	Данный класс позволяет быстро получить геопривязанный .tif из кучи разных тайлов
	"""
	def __init__(self, extent_geometry_path, crs_code, tile_crs_size, tile_image_size, download_handler, download_options={}):
		"""		
		Args:
		    extent_geometry_path (str): Путь к файлу с геометрией экстента
		    crs_code (str): Рабочая система координат (например, 'EPSG:3857')
		    tile_crs_size (int): Шаг для разбивочной сетки в рабочей системе координат
		    tile_image_size (int): Размер стороны геопривязанного изображения
		    download_handler (function): Функция, которая выполняет скачивание тайлов. На вход должна принимать координаты тайла (bounds) и kwargs. Должна возвращать bytes или None
		    download_options (dict, optional): Аргументы для download_handler
		"""
		self.script_folder = Path(__file__).resolve().parent
		self.tiles_folder = self.script_folder / '_tiles'
		self.geo_folder = self.tiles_folder / '_georeferenced'
		self.crs_code = crs_code
		self.tile_image_size = tile_image_size
		self.tile_crs_size = tile_crs_size
		self.extent_geometry_path = extent_geometry_path
		self.download_tile = download_handler
		self.download_options = download_options


	def georeferencing_tile(self, tile_path, tile_geom):
		self.geo_folder.mkdir(exist_ok=True)
		bounds = tile_geom.bounds
		bounds = [bounds[0], bounds[3], bounds[2], bounds[1]]
		gdal.Translate(str(self.geo_folder / (tile_path.stem + '.tif')),
	                   str(tile_path),
	                   outputSRS=self.crs_code,
	                   outputBounds=bounds,
	                   width=self.tile_image_size,
	                   height=self.tile_image_size,
	                   )


	def get_tile(self, tile_index, tile_geom):
		try:
			tile_bytes = self.download_tile(tile_geom.bounds, **self.download_options)
		except Exception as e:
			logger.exception(f'Index: {tile_index}, Bounds: {tile_geom.bounds}')
		else:
			if tile_bytes:
				self.tiles_folder.mkdir(exist_ok=True)
				tile_path = self.tiles_folder / ('%05d.png' % tile_index)
				with open(tile_path, 'wb') as file:
					file.write(tile_bytes)
				self.georeferencing_tile(tile_path, tile_geom)


	def merge_tiles(self, output_path):
		mosaic_path = self.script_folder / "mosaic.vrt"
		inputlist_path = self.script_folder / "inputlist.txt"
		tiles_glob = self.geo_folder.glob('*.tif')
		with open(inputlist_path, 'w', encoding='utf-8') as file:
			for tile in tiles_glob:
				file.write(str(tile))
				file.write('\n')
		os.system(f"gdalbuildvrt -input_file_list \"{str(inputlist_path)}\" \"{str(mosaic_path)}\"")
		os.system(f'gdal_translate -of GTiff -co "COMPRESS=JPEG" -co "TILED=YES" \"{str(mosaic_path)}\" \"{str(output_path)}\"')
		mosaic_path.unlink()
		inputlist_path.unlink()


	def delete_tiles(self):
		if self.tiles_folder.exists():
			shutil.rmtree(self.tiles_folder)


	def main(self, output_path):
		"""Производит полный цикл обработки тайлов
		
		Args:
		    output_path (pathlib.Path): Путь для сохранения результата
		"""
		self.delete_tiles()
		extent_gdf = gpd.read_file(self.extent_geometry_path)
		extent_gdf = extent_gdf.to_crs(self.crs_code)
		if len(extent_gdf) > 1:
			logger.warning('Файл экстента содержит больше одного элемента геометрии. Будут проигнорированы все, кроме первого')
		tiles = tilesOverShape(extent_gdf.iloc[0].geometry, delta_x=self.tile_crs_size, delta_y=self.tile_crs_size)
		tiles = tiles.set_crs(self.crs_code)
		pool_execute(self.get_tile, tiles.to_dict().items())
		self.merge_tiles(output_path)
		self.delete_tiles()


# ************FUNCTIONS********** #

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

def tilesOverShape(geom, delta_x=None, delta_y=None, x_count=None, y_count=None, filter_by_shape=True, fill_area_filter_factor = 0.0):
	if not isinstance(geom, (Polygon, MultiPolygon)):
		raise TypeError(f'Unexpected type for geometry: {type(geom)}')
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
	if filter_by_shape:
		if fill_area_filter_factor > 0:
			int_area = regions.intersection(geom).area
			area_ratio =  int_area / regions.area
			return regions[area_ratio > fill_area_filter_factor]
		else:
			regions = regions[regions.intersects(geom)]
			return regions
	else:
		return regions


if __name__=='__main__':
	pass
