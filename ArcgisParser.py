import geopandas as gpd
from shapely.geometry import *
import requests
from urllib.parse import urlencode
from pathlib import Path

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("[%(asctime)s][%(name)s] %(levelname)s - %(message)s")

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)


class ArcgisNode:

	"""Базовый класс для отображения экземпляров и запросов на сервер
	"""
	
	def __str__(self):
		path = '/'.join([str(i) for i in self.path])
		return f"{self.__class__.__name__} (https://{path})"

	def __repr__(self):
		return self.__str__()

	def __init__(self):
		self.test = 1

	def _request(self, endpoint, path, **parameters):
		"""Обработчик запросов на сервер
		
		Args:
		    endpoint (str): Метод REST API
		    path (list): Список, указывающий на полный путь ссылки
		    **parameters: Параметры запроса
		
		Returns:
		    dict: JSON-ответ сервера
		
		Raises:
		    Exception: При коде ответа не равному 200
		"""
		path = [str(i) for i in path]
		url = f"https://{'/'.join(path)}/{endpoint}?{urlencode(parameters)}"
		self._last_response = requests.get(url)
		logger.debug(f'REQUEST {url} [{self._last_response.status_code}]')
		self._last_json = self._last_response.json()
		if 'error' in self._last_json:
			raise Exception(f"[{self._last_json['error']['code']}] {self._last_json['error']['message']}")
		elif self._last_response.status_code != 200:
			raise Exception(f"[{self._last_response.status_code}] {self._last_json.get('detail', '')}")
		return self._last_json


class ArcgisLayer(ArcgisNode):

	"""
	
	Attributes:
	    children_layers (list of ArcgisLayer): Дочерние слои
	    dict_attrs (dict): Description
	    parent_layer (ArcgisLayer): Родительский слой
	    parent_service (ArcgisService): Сервис в котором содержится слой
	    path (TYPE): Description
	"""
	
	def __init__(self, parent_service, **kwargs):
		"""		
		Args:
		    parent_service (ArcgisService): Сервис в котором содержится слой
		    **kwargs: Все данные, которые выдает Rest API по Layer
		"""
		self.parent_service = parent_service
		self.parent_layer = None
		self.children_layers = []
		self.dict_attrs = kwargs
		for k,v in kwargs.items():
			setattr(self, k, v)
		self.path = self.parent_service.path + [kwargs['id']]

	def __str__(self):
		return f"{self.__class__.__name__} (id={self.id}, name={self.name}, type={self.type})"

	def set_parent(self, parent_layer):
		"""Назначает в атрибут родителя и добавляет родителю дочерний слой. ВАЖНО: обработка реализована в ArcgisLayerTree
		
		Args:
		    parent_layer (ArcgisLayer): Родительский слой
		"""
		self.parent_layer = parent_layer
		self.parent_layer.children_layers.append(self)

	def get_features(self):
		"""Скачивает геоданные слоя из Feature Layer
		
		Returns:
		    geopandas.GeoDataFrame: EPSG:4326
		
		Raises:
		    NotImplementedError: Если обнаружена неизвестная геометрия
		    TypeError: Если тип слоя не соответствует Feature Layer
		"""
		if self.type == 'Feature Layer':
			features = []
			result_offset = 0
			result_record_count = self.maxRecordCount
			while True:
				# из-за ограничения выдачи требуется пагинация
				data = self._request(
					'query',
					self.path,
					f='json',
					where='1=1',
					returnGeometry='true',
					outFields='*',
					outSR=4326,
					resultOffset=result_offset,
					resultRecordCount=result_record_count 
					)
				features += data['features']
				if data.get('exceededTransferLimit', False):
					result_offset += result_record_count
				else:
					break
			raw_geometry = [i.get('geometry', None) for i in features]
			if data['geometryType'] == 'esriGeometryPoint':
				geoms = [Point(i['x'], i['y']) if i else Point() for i in raw_geometry]
			elif data['geometryType'] == 'esriGeometryPolygon':
				geoms = [MultiPolygon([Polygon(j) for j in i['rings']]) if i else Polygon() for i in raw_geometry]
			elif data['geometryType'] == 'esriGeometryPolyline':
				geoms = [MultiLineString(i['paths']) if i else MultiLineString() for i in raw_geometry]
			else:
				raise NotImplementedError(f'Неизвестный тип геометрии {data["geometryType"]}')
			geoms = gpd.GeoSeries(geoms, name='geometry')
			gdf = gpd.GeoDataFrame([i['attributes'] for i in features], geometry=geoms)
			gdf = gdf.set_crs(4326)
			return gdf
		else:
			raise TypeError(f'Неподходящий тип слоя {self.type}')

	def get_all_features(self):
		"""Скачивает геоданные со всех дочерних слоев 
		
		Yields:
		    tuple(ArcgisLayer, geopandas.GeoDataFrame): 
		
		Raises:
		    TypeError: Если тип слоя не соответствует Group Layer
		"""
		if self.type == 'Group Layer':
			for child in self.children_layers:
				if child.type == 'Feature Layer':
					yield child, child.get_features()
				elif child.type == 'Group Layer':
					yield from child.get_all_features()
				else:
					layer.warning(f'Layer {child.name} was skipped (Type: {child.type})')
		else:
			raise TypeError(f'Неподходящий тип слоя {self.type}')


class ArcgisLayerTree:

	"""Вспомогательный класс для хранения коллекции ArcgisLayer
	
	Attributes:
	    layers (list of ArcgisLayer):
	"""
	
	def __iter__(self):
		return iter(self.layers)

	def __getitem__(self, index):
		if isinstance(index, int):
			for l in self.layers:
				if l.id == index:
					return l
			raise IndexError(f'Index {index} not found')
		else:
			raise IndexError(f'Unknown index type {type(index)}')

	def __init__(self, service):
		"""		
		Args:
		    service (ArcgisService): Сервис, в котором хранятся слои
		"""
		layers_json, _ = service.explore_layers() 
		self.layers = [ArcgisLayer(service, **i) for i in layers_json]
		for l in self.layers:
			parent_item = l.parentLayer
			if parent_item:
				l.set_parent(self[parent_item['id']])


class ArcgisService(ArcgisNode):

	"""Следующий после папок (ArcgisFolder) в иерархии. На данный момент упор на MapServer
	"""
	
	def __init__(self, parent, name, type):
		"""Summary
		
		Args:
		    parent (ArcgisServer|ArcgisFolder): Сервер или папка, где лежит сервис
		    name (str): Имя сервиса
		    type (str): Тип сервиса (например, MapServer)
		"""
		self.name = name.split('/')[-1]
		self.type = type
		self.parent = parent 
		self.path = self.parent.path + [self.name, self.type]
		self.layers_tree = []
		if self.type == 'MapServer':
			self.layers_tree = ArcgisLayerTree(self)

	def explore_layers(self):
		"""Explore layers of service
		
		Returns:
		    list: service layers
		    list: service tables
		"""
		try:
			result = self._request(
				'layers',
				self.path,
				f='json'
				)
			return result['layers'], result['tables']
		except:
			return [], []

	def get_all_features(self):
		"""Возвращает слои с геоданными, если таковые имеются (слой типа Feature Layer)
		
		Yields:
		    tuple(ArcgisLayer, geopandas.GeoDataFrame): слой и геоданные, которые там содержатся
		"""
		for layer in self.layers_tree:
			if layer.type == 'Feature Layer':
				yield layer, layer.get_features()


class ArcgisFolder(ArcgisNode):

	"""Объект, в котором хранятся сервиса сервера или другие папки
	"""
	
	def __init__(self, parent, name):
		"""		
		Args:
		    parent (ArcgisFolder|ArcgisServer): Экземпляр родителя
		    name (str): Имя папки
		"""
		self.parent = parent
		self.name = name
		self.path = self.parent.path + [name]
		folders, services = self.explore()
		self.folders = [ArcgisFolder(self, i) for i in folders]
		self.services = [ArcgisService(self, **i) for i in services]

	def service_iter(self):
		"""Генератор, возвращающий сервисы в таком директории
		
		Yields:
		    ArcgisService: 
		
		Raises:
		    Exception: Для начала работы необходимо спарсить структуру сервера
		"""
		if not self.folders and not self.services:
			raise Exception('Server is not parsed yet')
		else:
			for service in self.services:
				yield service
			for folder in self.folders:
				yield from folder.service_iter()

	def folder_iter(self):
		"""Генератор, возвращающий папки в таком директории
		
		Yields:
		    ArcgisFolder: 
		
		Raises:
		    Exception: Для начала работы необходимо спарсить структуру сервера
		"""
		if not self.folders and not self.services:
			raise Exception('Server is not parsed yet')
		else:
			for folder in self.folders:
				yield folder
				yield from folder.folder_iter()

	def explore(self):
		"""Обнаружение вложенных папок и сервисов в данной директории
		
		Returns:
		    list: nested folders
		    list: nested services
		"""
		result = self._request('', self.path, f='json') 
		return result['folders'], result['services']


class ArcgisServer(ArcgisFolder):

	"""Входная точка для работы с парсером сервера Arcgis. Является корневым каталогом.
	Особый случай ArcgisFolder 
	"""
	
	def __init__(self, host, site):
		"""		
		Args:
		    host (str): Имя хоста, на котором расположен сервер
		    site (str): Тип сервера, например "arcgis3"
		"""
		self.host = host
		self.site = site
		self.path = [self.host, self.site, 'rest', 'services']
		self.folders = self.services = []

	def parse(self):
		"""Парсит структуру сервера. Необходимо выполнить перед началом любой работы с сервером
		"""
		folders, services = self.explore()
		for i in folders:
			try:
				self.folders.append(ArcgisFolder(self, i))
			except:
				pass
		for i in services:
			self.services.append(ArcgisService(self, **i))

	def find_layers(self, layer_name):
		"""Генератор. Позволяет найти слои с указанным именем на сервере
		
		Args:
		    layer_name (str): Имя слоя
		
		Yields:
		    ArcgisLayer: Слой с заданным именем
		"""
		for service in self.service_iter():
			for layer in service.layers_tree:
				if layer.name == layer_name:
					yield layer

	def find_services(self, service_name):
		"""Генератор. Позволяет найти сервисы с указанным именем на сервере
		
		Args:
		    service_name (str): Имя сервиса
		
		Yields:
		    ArcgisService: Сервис с заданным именем
		"""
		for service in self.service_iter():
			if service.name == service_name:
				yield service


if __name__ == '__main__':
	logger.debug('start')
	ARC_SERVER = ArcgisServer('pkk.rosreestr.ru', 'arcgis')
	ARC_SERVER.parse()
	logger.debug('end')
