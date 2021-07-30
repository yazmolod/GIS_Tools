import geopandas as gpd 
from shapely.geometry import * 
import pyproj
from math import floor
from itertools import product
import numpy as np
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkb
import os
from .ThreadsUtils import pool_execute
try:
	from osgeo import gdal
except:
	pass
from pathlib import Path
import shutil
import logging 
import mercantile

logger = logging.getLogger(__name__)

class GeoTiffer:
	"""
	Данный класс позволяет быстро получить геопривязанный .tif из кучи разных тайлов
	"""
	def __init__(self, grid_data, download_handler, download_options={}):
		"""
		Args:
		    grid_data (str|pathlib.Path|geopandas.GeoSeries|geopadnas.GeoDataFrame): Датасет с геометрией тайлов (или путь к нему)
		    download_handler (function): Функция, которая выполняет скачивание тайлов. На вход должна принимать координаты тайла (bounds) и kwargs. Должна возвращать bytes или None
		    download_options (dict, optional): Аргументы для download_handler
		"""
		self.script_folder = Path(__file__).resolve().parent
		self.tiles_folder = self.script_folder / '_tiles'
		self.geo_folder = self.tiles_folder / '_georeferenced'

		self.grid = _geo_input_handler(grid_data)
		self.crs_code = str(self.grid.crs)
		self.download_tile = download_handler
		self.download_options = download_options


	def georeferencing_tile(self, tile_path, tile_geom):		
		bounds = tile_geom.bounds
		bounds = [bounds[0], bounds[3], bounds[2], bounds[1]]
		gdal.Translate(str(self.geo_folder / (tile_path.stem + '.tif')),
	                   str(tile_path),
	                   outputSRS=self.crs_code,
	                   outputBounds=bounds,
	                   )


	def get_tile(self, tile_index, tile_geom):
		try:
			tile_bytes = self.download_tile(tile_geom.bounds, **self.download_options)
		except Exception as e:
			logger.exception(f'Index: {tile_index}, Bounds: {tile_geom.bounds}')
		else:
			if tile_bytes:				
				tile_path = self.tiles_folder / ('%05d.png' % tile_index)
				with open(tile_path, 'wb') as file:
					file.write(tile_bytes)
				self.georeferencing_tile(tile_path, tile_geom)
			else:
				logger.warning(f'No data (Index: {tile_index}, Bounds: {tile_geom.bounds})')


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


	def make_geotiff(self, output_path):
		"""Производит полный цикл обработки тайлов
		
		Args:
		    output_path (pathlib.Path): Путь для сохранения результата
		"""
		self.delete_tiles()
		self.tiles_folder.mkdir(exist_ok=True)
		self.geo_folder.mkdir(exist_ok=True)
		pool_execute(self.get_tile, self.grid.to_dict().items())
		self.merge_tiles(output_path)
		self.delete_tiles()

	@staticmethod
	def download_geotiff(output_path, grid_data, download_handler, **download_options):
		"""Summary
		
		Args:
		    output_path (TYPE): Путь для сохранения итогового geotiff'а 
		    grid_data (str|pathlib.Path|geopandas.GeoSeries|geopadnas.GeoDataFrame): Датасет с геометрией тайлов (или путь к нему)
		    download_handler (function): Функция, которая выполняет скачивание тайлов. На вход должна принимать координаты тайла (bounds) и kwargs. Должна возвращать bytes или None
		    **download_options: Аргументы для download_handler
		"""
		tiler = GeoTiffer(grid_data, download_handler, download_options)
		tiler.make_geotiff(output_path)


# ************FUNCTIONS********** #

def convert_to_local_csr(geom):
	crs = geom.crs
	if not crs:
		geom.crs = pyproj.CRS('EPSG:4326')
	elif crs.to_string()!='EPSG:4326':
		geom = geom.to_crs('EPSG:4326')
	polus = 6 if geom.geometry.y.min() > 0 else 7
	zone = floor((geom.geometry.x.min()+180)/6)
	utm = 'EPSG:32%d%02d' % (polus, zone)
	return geom.to_crs(utm)

def _tiles_filtering_by_geometry(tiles, geom, fill_area_filter_factor):
	if isinstance(fill_area_filter_factor, float) and 1 > fill_area_filter_factor > 0:
		int_area = tiles.intersection(geom).area
		area_ratio =  int_area / tiles.area
		return tiles[area_ratio > fill_area_filter_factor]
	else:
		# это хоть и аналогично fill_area_filter_factor == 0 но работает значительно быстрее
		return tiles[tiles.intersects(geom)]

def _tiles_filtering_by_series(tiles, extent_geoseries, fill_area_filter_factor):
	assert tiles.crs == extent_geoseries.crs
	filtered_tiles = tiles.iloc[0:0]	# копируем тип, но не копируем данные
	for geom in extent_geoseries:
		ft = _tiles_filtering_by_geometry(tiles, geom, fill_area_filter_factor)
		filtered_tiles = filtered_tiles.append(ft)
	return geopandas_drop_duplicates(filtered_tiles)

def _geo_input_handler(geodata, crs=None):
	if isinstance(geodata, gpd.GeoDataFrame):
		geoseries = geodata.geometry
	elif isinstance(geodata, gpd.GeoSeries):
		geoseries = geodata
	elif isinstance(geodata, Path) or isinstance(geodata, str):
		geoseries = gpd.read_file(str(geodata)).geometry
	else:
		raise TypeError(geodata)
	if crs:
		if geoseries.crs:
			geoseries = geoseries.to_crs(crs)
		else:
			geoseries = geoseries.set_crs(crs)
	return geoseries

def grid_over_shape(
	geodata, crs_code, 
	x_delta=None, y_delta=None, x_count=None, y_count=None, x_factor=None, y_factor=None, 
	filter_by_shape=True, fill_area_filter_factor = 0.0):
	"""Создает сетку тайлов поверх входных экстентов
	
	Args:
	    geoseries (geopandas.GeoSeries|geopandas.GeoDataFrame): Экстенты, по котором необходимо создать тайлы
	    crs_code (str|int): CRS для тайлов
	    x_delta (float, optional): Шаг тайла по оси Х
	    y_delta (float, optional): Шаг тайла по оси Y
	    x_count (int, optional): Количество тайлов по оси x, которые должны покрыть bbox экстента
	    y_count (int, optional): Количество тайлов по оси y, которые должны покрыть bbox экстента
	    x_factor (float, optional): Шаг по X будет равен этому значению умноженному на шаг Y. Нельзя использовать одновременно с y_factor!
	    y_factor (float, optional): Шаг по Y будет равен этому значению умноженному на шаг X. Нельзя использовать одновременно с x_factor!
	    filter_by_shape (bool, optional): если True, тайлы bbox'а, которые не пересекаются c экстентом, будут отсечены в противном случае тайлы будут распределены по всему bbox
	    fill_area_filter_factor (float, optional): коэффициент площади, меньше которой тайл отсекается при filter_by_shape. Например, при коэффициенте равном 0.25, тайл, который покрывает меньше 25% экстента, будет отсечен
	
	Returns:
	    geopandas.GeoSeries: серия с тайлами
	"""
	geoseries = _geo_input_handler(geodata, crs_code)
	x1,y1 = geoseries.bounds.min()[['minx', 'miny']]
	x2,y2 = geoseries.bounds.max()[['maxx', 'maxy']]
	if x_delta:
		xs = np.arange(x1, x2, x_delta)
	elif x_count:
		xs = np.linspace(x1, x2, x_count, endpoint=False)
		x_delta = xs[1] - xs[0]
	if y_delta:
		ys = np.arange(y1, y2, y_delta)
	elif y_count:
		ys = np.linspace(y1, y2, y_count, endpoint=False)
		y_delta = ys[1] - ys[0]
	if x_factor:
		x_delta = y_delta * x_factor
		xs = np.arange(x1, x2, x_delta)
	if y_factor:
		y_delta = x_delta * y_factor
		ys = np.arange(y1, y2, y_delta)

	df = gpd.pd.DataFrame(product(xs,ys), columns=['minx', 'miny'])
	df['maxx'] = df['minx'] + x_delta
	df['maxy'] = df['miny'] + y_delta
	df['geometry'] = df.apply(lambda x: box(**x.to_dict()), axis=1)
	tiles = gpd.GeoSeries(df['geometry'], crs=crs_code)
	if filter_by_shape:
		tiles = _tiles_filtering_by_series(tiles, geoseries, fill_area_filter_factor)
	return tiles

def tiles_over_shape(geodata, zoom, filter_by_shape=True, fill_area_filter_factor = 0.0):
	'''Generate slippy tile polygons in GeoDataFrame. Only EPSG:4326!
	
	Args:
	    geodata (geopandas.GeoSeries|geopandas.GeoDataFrame): Экстенты, по котором необходимо создать тайлы
	    zoom (int): Z-координата, от которой зависит дробление тайлов
	    filter_by_shape (bool, optional): если True, тайлы bbox'а, которые не пересекаются c экстентом, будут отсечены в противном случае тайлы будут распределены по всему bbox
	    fill_area_filter_factor (float, optional): коэффициент площади, меньше которой тайл отсекается при filter_by_shape. Например, при коэффициенте равном 0.25, тайл, который покрывает меньше 25% экстента, будет отсечен
	
	Returns:
	    geopandas.GeoDataFrame: тайлы и их x,y,z
	'''
	geoseries = _geo_input_handler(geodata, 4326)
	x1,y1 = geoseries.bounds.min()[['minx', 'miny']]
	x2,y2 = geoseries.bounds.max()[['maxx', 'maxy']]
	features = []
	for tile in mercantile.tiles(x1,y1,x2,y2, zoom):
		feature = mercantile.feature(tile)
		feature['properties'] = {
			'x': tile.x,
			'y': tile.y,
			'z': tile.z,
		}
		features.append(feature)
	tiles = gpd.GeoDataFrame.from_features({'type': 'FeatureCollection', 'features': features}, crs=4326)
	if filter_by_shape:
		tiles = _tiles_filtering_by_series(tiles, geoseries, fill_area_filter_factor)
	return tiles

def geopandas_drop_duplicates(geodata):
	'''https://github.com/geopandas/geopandas/issues/521

	!this only works if geometries are point-wise equal, and not topologically equal!
	'''
	if isinstance(geodata, gpd.GeoDataFrame):
		geodata["geometry"] = geodata["geometry"].apply(lambda geom: geom.wkb)
		geodata = geodata.drop_duplicates(["geometry"])
		geodata["geometry"] = geodata["geometry"].apply(lambda geom: wkb.loads(geom))
		return geodata
	elif isinstance(geodata, gpd.GeoSeries):
		return gpd.GeoSeries(geodata.apply(lambda geom: geom.wkb).drop_duplicates().apply(lambda geom: wkb.loads(geom)), crs=geodata.crs)
	else:
		raise TypeError(geodata)


if __name__=='__main__':
	extent = gpd.read_file("test_multi_extent.geojson")

	# gdf = grid_over_shape(extent, 3857, x_count=100, y_count=200, filter_by_shape=True, fill_area_filter_factor=0.1)
	gdf = tiles_over_shape(extent, 12)
	gdf.to_file('tiles_test.gpkg', layer='tiles_shapped', driver='GPKG')
