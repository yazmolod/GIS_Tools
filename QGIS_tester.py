import requests
import time

def rosreestr(kadastr):
    url = 'https://pkk.rosreestr.ru/api/features/2?_={time}&text={kadastr}&limit=40&skip=0'
    r = requests.get(url.format(kadastr=kadastr, time=round(time.time() * 1000))).json()
    print(r)
    coords = []
    if r['features']:
        for f in r['features']:
            center = f.get('center', None)
            if center:
                coords.append([f['center']['x'], f['center']['y']])
    pts = [QgsPointXY(*i) for i in coords]
    return pts
    
def rosreestr2(kadastr):
    kad_numbers = kadastr.split(':')
    url = 'https://pkk.rosreestr.ru/api/features/{req_type}?_={time}&text={kadastr}&limit=40&skip=0'
    pt = None
    for i in reversed(range(0, len(kad_numbers))):
        print(i)
        current_kadastr = ':'.join(kad_numbers[: i + 1 ])
        req_type = 4 - i
        formatted_url = url.format(kadastr=current_kadastr, time = round(time.time() * 1000), req_type = req_type)
        result = requests.get(formatted_url).json()
        if result['features']:
            center = result['features'][0]['center']
            pt = QgsPointXY(center['x'], center['y'])
            return pt 

crs = QgsCoordinateReferenceSystem(3857)
c = rosreestr2('39:03:080101:100')
if c:
    create_temp_layer([c], 'points', crs=crs)
else:
    print('Error')

