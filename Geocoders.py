import time
import requests
from shapely.geometry import MultiPoint, Point
from shapely.ops import transform
import pyproj
import geopandas as gpd
import pandas as pd

from ProxyGrabber import ProxyGrabber
import ParserUtils

import re
import os
import json
import logging
import logging.config
folder = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(folder, 'loggers.config')) as fp:
    logging.config.dictConfig(json.load(fp))
logger = logging.getLogger(__name__)

GRABBER = ProxyGrabber()
HERE_API_KEY = 'uqDak72P-C8FYyYkI4vz8EDxbNFhBJw4jW-fg9P4lb8'

def _rosreestr_request(current_kadastr):
	url = 'https://pkk.rosreestr.ru/api/features/{req_type}?_={time}&text={kadastr}&limit=40&skip=0'
	req_type = 5 - len(current_kadastr.split(':'))
	formatted_url = url.format(kadastr=current_kadastr, time = round(time.time() * 1000), req_type = req_type)
	try:
		r = requests.get(formatted_url, proxies = GRABBER.get_proxy(), timeout=3)
	except:
		GRABBER.next_proxy()
		return _rosreestr_request(current_kadastr)
	if r.status_code == 403:
		GRABBER.next_proxy()
		return _rosreestr_request(current_kadastr)
	result = r.json()
	if result['features']:
		center = result['features'][0].get('center')
		if center:
			pt = Point(center['x'], center['y'])
			pt = gpd.GeoSeries(pt, crs=3857).to_crs(4326)[0]
			return pt

def rosreestr(kadastr, deep_search = False):
	kad_check = re.findall(r'[\d:]+', kadastr)
	if len(kad_check) != 1:
		return
	if not deep_search:
		return _rosreestr_request(kadastr)
	else:
		kad_numbers = kadastr.split(':')	
		for i in reversed(range(0, len(kad_numbers))):
			current_kadastr = ':'.join(kad_numbers[: i + 1 ])
			pt = _rosreestr_request(current_kadastr)
			if pt:
				return pt				
		

def here(search_string, additional_params = None):
	params = {
	'q': search_string,
	'apiKey': HERE_API_KEY,
	'lang': 'ru-RU'
	}
	url = f'https://geocode.search.hereapi.com/v1/geocode?q={params["q"]}&apiKey={params["apiKey"]}&lang=ru-RU'
	if additional_params:
		params = {**params, **additional_params}
	try:
		r = requests.get(url)
		data = r.json()['items']
	except:
		logger.exception('Не удалось найти геолокацию')
		return
	if len(data) == 0:
		return Point()
	if len(data) > 1:
		item = sorted(data, key = lambda x: x['scoring']['queryScore'], reverse=True)[0]
	else:
		item = data[0]
	x = item['position']['lng']
	y = item['position']['lat']
	pt = Point(x,y)
	return pt


def yandex_map(search_string):
	pass

if __name__ == '__main__':
	# kn = [
	# '78:36:0536701:2011',
	# '78:36:0536701',
	# '78:36',
	# '78',
	# ]
	# gdf = gpd.GeoDataFrame()
	# for k in kn:
	# 	pt = rosreestr(k)
	# 	print(type(pt))
	# 	gdf = gdf.append(gpd.GeoDataFrame([k], columns=['kn'], geometry=gpd.GeoSeries([pt])))
	# gdf.to_file('test210.geojson', driver='GeoJSON')
	# p = here('Башкортостан Респ., Стерлитамак г., Гоголя ул., 122, 453130')
	# df = pd.DataFrame(['Самара алексея толстого 26', 'Самара Никитинская 77', 'Самара Мориса Тореза 6'], columns=['Адрес'])

	add = 'Смоленская обл, Рославльский р-н юго-западная часть КК 67:15:0020401, юго-восточнее д. Липовка'
	data = here(add)