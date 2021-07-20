import requests 
from requests.exceptions import *
import ProxyGrabber
from pathlib import Path
from rosreestr2coord import Area
from shapely.geometry import shape
import json
import logging
logger = logging.getLogger(__name__)

item_type = {
	'ЗУ':1,
	'ОКС':5
}
__GRABBER = None
def get_grabber():
    global __GRABBER
    if __GRABBER is None:
        __GRABBER = ProxyGrabber()
    return __GRABBER
pkk_dicts = json.loads((Path(__file__).parent / 'pkk_dicts.json').read_bytes())

def cn_to_id(cn):
	f_parts = []
	for part in cn.split(':'):
		f_part = part.lstrip('0')
		if not f_part:
			f_part = '0'
		f_parts.append(f_part)
	return ":".join(f_parts)

def send_request(url, params=None):
	try:
		r = requests.get(url, params=params, proxies=__GRABBER.get_proxy(), timeout=3)
	except (ConnectionError, ReadTimeout):
		__GRABBER.next_proxy()
		return send_request(url, params)
	if r.status_code==200:
		return r.json()
	else:
		logger.warning(f'Status code {r.status_code}')

def get_coords(kadastr):
	url = f'https://pkk.rosreestr.ru/api/features/1'
	params = {
	'text':kadastr,
	'tolerance':4,
	'limit':1
	}
	data = send_request(url, params)
	if data:
		center = data['features'][0]['center']
		return center['x'], center['y']
	else:
		logger.warning(f'No data (cn {kadastr})')

def get_item_in_point(x,y, _type):
	url = f'https://pkk.rosreestr.ru/api/features/{item_type[_type]}'
	params = {
		'text': f'{y} {x}',
		'limit': 20,
		'skip': 0,
		'inPoint': 'true',
		'tolerance': 4
	}
	data = send_request(url, params)
	if data:
		if data.get('total', 0) > 0:
			point_attrs = data['features'][0]['attrs']
			return point_attrs
		else:
			logger.warning(f'POINT({x},{y}), type {_type}: nothing found')
	else:
		logger.warning(f'No data ({x} {y})')

def get_attrs(cn, _type):
	_id = cn_to_id(cn)
	url = f'https://pkk.rosreestr.ru/api/features/{item_type[_type]}/{_id}'
	data = send_request(url)
	if data:
		feature = data['feature']
		if feature:
			return feature['attrs']
		else:
			logger.warning(f'Cn {cn}, type {_type}: No attrs')
	else:
		logger.warning(f'No atrs (cn {cn})')
		return data

def get_polygon_by_cn(cn, _type):
	area = Area(code=cn, area_type=item_type[_type], with_log=False, use_cache=True)
	feature = area.to_geojson_poly()
	if feature:		
		feature = json.loads(feature)
		geom = shape(feature['geometry'])
		return geom
	else:
		logger.warning(f'Nothing found for {cn} ({_type})')


if __name__ == '__main__':
	geom = get_polygon_by_cn("78:11:0006019:19", 'ЗУ')