import requests
from shapely.geometry import *
from shapely.ops import linemerge, unary_union, polygonize
import geopandas as gpd
from fake_headers import Headers
import logging
import ProxyGrabber
from time import time, sleep
import re

logger = logging.getLogger(__name__)

overpass_url = "http://overpass-api.de/api/interpreter"
def make_request(query):
	start_time = time()
	logger.debug(f'Query: {query}')
	try:
		r = requests.get(overpass_url, 
                        params={'data': query},
                        headers=Headers().generate(),
                        proxies=ProxyGrabber.get_proxy()
                        )
	except:
		ProxyGrabber.next_proxy()
		return make_request(query)
	if r.status_code == 429:
		ProxyGrabber.next_proxy()
		return make_request(query)
	elif r.status_code == 200:
		data = r.json()['elements']
		logger.info('Done in %.1f seconds' % (time() - start_time))
		return data
	else:
		print ('Bad request')

def abort_requests():
	url = 'http://overpass-api.de/api/kill_my_queries'
	r = requests.get(url)
	return r

def overpass_status():
	url = 'http://overpass-api.de/api/status'
	r = requests.get(url)
	status_text = r.text
	print(r.text)
	available = bool(re.findall(r'\d slots available now', status_text))
	timings = re.findall(r'(\d+) seconds\.', status_text)
	return available, [int(i) for i in timings]

def node_handler(node):
	tags = node.get('tags', {})
	attributes = {**node, **tags}
	s = gpd.pd.Series(attributes)
	s['geometry'] = Point(node['lon'], node['lat'])
	return s


def way_handler(way):
	bounds = way.get('bounds', [])
	nodes = way.get('nodes', [])
	tags = way.get('tags', {})
	geometry = way.get('geometry', [])
	geometry = [(i['lon'], i['lat']) for i in geometry]
	line = LineString(geometry)
	attributes = {**way, **tags}
	s = gpd.pd.Series(attributes)
	s['geometry'] = line
	return s


def relation_handler(rel):
	bounds = rel.get('bounds', [])
	members = rel.get('members', [])
	tags = rel.get('tags')
	attributes = {**rel, **tags}
	s = gpd.pd.Series(attributes)
	outer_ways = [i for i in members if i['role'] == 'outer']
	inner_ways = [i for i in members if i['role'] == 'inner']
	outer_poly = ways_to_polygons(outer_ways)
	inner_poly = ways_to_polygons(inner_ways)
	diffs = []
	for outer in outer_poly:
		for inner in inner_poly:
			outer = outer.difference(inner)
		diffs.append(outer)
	s['geometry'] = MultiPolygon(diffs)
	return s


def ways_to_polygons(ways):	
	lss = []
	for way in ways:
		coords = [(i['lon'], i['lat']) for i in way['geometry']]
		ls = LineString(coords)
		lss.append(ls)
	merged = linemerge(lss) # merge LineStrings
	borders = unary_union(merged) # linestrings to a MultiLineString
	polygons = list(polygonize(borders))
	return polygons


def make_searh(
	area_id=None, area_prop=None, 
	node_id=None, node_prop=None, 
	way_id=None, way_prop=None, 
	rel_id=None, rel_prop=None
	):
	if area_id:
		area_str = f'area({area_id});'
	elif area_prop:
		area_str = 'area' + ''.join([f'["{k}"="{v}"]' for k,v in area_prop.items()]) + ';'
	else:
		area_str = ''

	if node_id:
		node_str = f'node({node_id});'
	elif node_prop:
		node_str = 'node' + ''.join([f'["{k}"="{v}"]' for k,v in node_prop.items()])
		if area_str:
			node_str += '(area);'
		else:
			node_str += ';'
	else:
		node_str = ''

	if way_id:
		way_str = f'way({way_id});'
	elif way_prop:
		way_str = 'way' + ''.join([f'["{k}"="{v}"]' for k,v in way_prop.items()])
		if area_str:
			way_str += '(area);'
		else:
			way_str += ';'
	else:
		way_str = ''

	if rel_id:
		rel_str = f'rel({rel_id});'
	elif rel_prop:
		rel_str = 'rel' + ''.join([f'["{k}"="{v}"]' for k,v in rel_prop.items()])
		if area_str:
			rel_str += '(area);'
		else:
			rel_str += ';'
	else:
		rel_str = ''

	query = f'[out:json];{area_str}({node_str}{way_str}{rel_str});out body geom qt;'
	return make_request(query)


def response_handler(response):
	for i,r in enumerate(response):
		if r['type'] == 'node':
			yield node_handler(r)
		elif r['type'] == 'way':
			yield way_handler(r)
		elif r['type'] == 'relation':
			yield relation_handler(r)


def debug_output(gdf):
	gdf.to_file('OverpassTest.gpkg', layer='debug', driver='GPKG')


def clean_gdf(gdf):
	for c in gdf.columns:
		sample = gdf[c].dropna().iloc[0]
		if isinstance(sample, list) or isinstance(sample, dict):
			gdf.drop(c, axis=1, inplace=True)


if __name__ == '__main__':
	admin_levels = [7]
	for admin_level in admin_levels:
		while True:
			available, timings = overpass_status()
			if True:
			# if available:
				area_prop = {
					"ISO3166-1":"RU",
				}
				rel_prop = {
				"admin_level": admin_level,
				"boundary": "administrative",
				}
				data = make_searh(rel_prop=rel_prop, area_prop=area_prop)
				g = response_handler(data)
				gdf = gpd.GeoDataFrame(list(g))
				clean_gdf(gdf)
				gdf.to_file('OverpassTest.gpkg', layer=f'admin_level_{admin_level}', driver='GPKG')
				break
			else:
				t = min(timings)
				logger.info(f'Sleep for {t} seconds')
				sleep(t)
