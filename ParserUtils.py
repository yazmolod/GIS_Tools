import pandas as pd
import os
import time
from stem.control import Controller
from stem import Signal
import json
import requests
from concurrent.futures import ThreadPoolExecutor
import subprocess
import webbrowser

tor_proxy = {'https':'socks5h://localhost:9050',
             'http':'socks5h://localhost:9050'
            }

#TOR
def get_current_ip():
    session = requests.session()
    # TO Request URL with SOCKS over TOR
    session.proxies = {}
    session.proxies['http']='socks5h://localhost:9050'
    session.proxies['https']='socks5h://localhost:9050'
    try:
        r = session.get('http://httpbin.org/ip')
    except Exception as e:
        print (str(e))
    else:
        return json.loads(r.text)['origin']

def change_tor_ip():
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate()
        controller.signal(Signal.NEWNYM)
###

#OpenVPN
def openvpn_parse_configs():
    path = os.environ['USERPROFILE'] + r'\OpenVPN\config'
    return os.listdir(path)

def openvpn_turn_on(config_name):
    config_url = 'https://www.freeopenvpn.org/logpass/' + config_name.split('_')[0].lower() + '.php'
    webbrowser.open_new(config_url)
    subprocess.run([r"C:\Program Files\OpenVPN\bin\openvpn-gui.exe", '--connect', config_name, '--auth-user-pass', 'auth.txt'])
    input('Hit something to continue when VPN will be ready...')

def openvpn_turn_off():
    subprocess.run(['taskkill.exe', '/F', '/IM', 'openvpn.exe'])

VPN_CONFIGS = []
VPN_INDEX = -1
def openvpn_next_vpn():
    global VPN_CONFIGS
    global VPN_INDEX
    VPN_INDEX += 1
    if VPN_INDEX >= len(VPN_CONFIGS):
        VPN_INDEX = -1
    openvpn_set_vpn()

def openvpn_set_vpn():
    global VPN_CONFIGS
    global VPN_INDEX
    if not VPN_CONFIGS:
        VPN_CONFIGS = openvpn_parse_configs()
    openvpn_turn_off()
    if VPN_INDEX >= 0:
        openvpn_turn_on(VPN_CONFIGS[VPN_INDEX])
###

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' %
                  (method.__name__, (te - ts) * 1000))
        return result
    return timed


def pd_file_read(filepath):
    if isinstance(filepath, str):
        extensions = {'csv': pd.read_csv,
                      'xlsx': pd.read_excel,
                      'json': pd.read_json,
                      'ftr': pd.read_feather}
        ext = os.path.splitext(os.path.basename(filepath))[-1].strip('.')
        func = extensions.get(ext, None)
        if func:
            df = func(filepath)
            df['filepath'] = filepath
        return df
    elif isinstance(filepath, pd.DataFrame):
        return filepath
    else:
        raise ValueError('Неверный тип переменной')

def merge_folder_files(path):
    df = pd.DataFrame()
    names = os.listdir(path)    
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(pd_file_read, os.path.join(path, name)) for name in names]
        df = pd.concat([i.result() for i in futures], sort=False)
    return df

if __name__ == '__main__':
    r = openvpn_parse_configs()