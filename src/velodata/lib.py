import requests
import time
import math
import base64
import pandas as pd
import io 
import copy
import json
import websockets

class client:
    def __init__(self, key, retry=2):
        self.key = "Basic " + str(base64.b64encode(b"api:" + key.encode("utf-8")))[2:-1]
        self.base_url = "https://api.velo.xyz/api/v1/"
        self.news_url = "https://api.velo.xyz/api/n/"
        self.news_wss = "wss://api.velo.xyz/api/w/connect"
        self.headers = {"Authorization": self.key}
        self.session = requests.Session()
        self.retry = retry
        self.news = self
        self.ws = None

    def version(self):
        return '1.5.5'
        
    def http_get(self, base_url, headers, params={}, again=0):
        request = self.session.get(base_url, params=params, headers=headers)
        code = request.status_code
        
        if code == 200:
            return request.text
        elif code == 429:
            time.sleep(2)
            return self.http_get(base_url, headers, params)
        elif code >= 500 and again < self.retry:
            return self.http_get(base_url, headers, params, again + 1)
        else:
            raise Exception("status " + str(code) + ": " + request.text)
        
    def get_status(self):
        return self.http_get(self.base_url + "status", headers=self.headers)

    def get_news(self, begin=0):
        request = self.http_get(self.news_url + "news", params={'begin':begin}, headers=self.headers)
        return json.loads(request)['stories']

    async def stream_news(self):
        async with websockets.connect(self.news_wss, extra_headers=self.headers, ssl=True) as websocket:
            self.ws = websocket
            await websocket.send('subscribe news_priority')

            yield 'connected'
            while True:
                try:
                    reply = await websocket.recv()

                    if reply == '{"heartbeat":true}':
                        yield 'heartbeat'
                    else:
                        yield reply

                except:
                    await self.close_stream()
                    yield 'closed'
                    return

    async def close_stream(self):
        if(self.ws):
            await self.ws.close()
            self.ws = None

    def get_products(self, product_type: str):        
        request = self.session.get(self.base_url + product_type, headers=self.headers).text
        
        try:
            return pd.read_csv(io.StringIO(request)).to_dict('records')
        except:
            print("\nPlease check that you have provided a product_type of futures, spot, or options.\n")
            raise Exception(request.content)

    def get_futures(self):
        return self.get_products('futures')
            
    def get_options(self):
        return self.get_products('options')
    
    def get_spot(self):
        return self.get_products('spot')

    def add_months(self, dt, months):
        dt = pd.to_datetime(dt, unit='ms', utc=True)
        for m in range(months):
            dt = pd.Period(pd.Period(dt, freq='M').end_time + pd.Timedelta(minutes=1), freq='M').start_time

        dt = (dt - pd.Timestamp("1970-01-01")) // pd.Timedelta('1 milliseconds')
        return dt

    def align_resolution(self, params: dict):
        if isinstance(params['resolution'], str) == False:
            return params

        try:
            begin = int(params['begin'])
            end = int(params['end'])
        except Exception as e:
            print('\nPlease ensure you have passed begin and end params properly as integers.\n')
            raise e

        units = {
            'm': 1000 * 60,
            'h': 1000 * 60 * 60,
            'd': 1000 * 60 * 60 * 24,
            'w': 'w',
            'M': 'M',
        }

        reso = params['resolution']
        try:
            unit = units[reso[-1]]
            reso = int(reso[:-1])
        except Exception as e:
            print('\nPlease ensure you have passed a valid resolution suffix, like m, h, d, w, or M.\n')
            raise e

        if isinstance(unit, int):
            step = reso * unit
            begin = begin - (begin % step)
            if end % step != 0:
                end = step + (end - (end % step)) 
            reso = (reso * unit) / (1000 * 60)
        
            params['resolution'] = reso
            params['begin'] = begin
            params['end'] = end
            return params

        if unit != 'w' and unit != 'M':
            print('\nPlease ensure you have passed a valid resolution suffix, like m, h, d, w, or M.\n')
            raise e
        
        begin = pd.to_datetime(begin, unit='ms', utc=True)
        end = pd.to_datetime(end, unit='ms', utc=True)

        if unit == 'w':
            params['begin'] = pd.Period(begin, freq='W').start_time 
            if end != pd.Period(end, freq='W').start_time:
                params['end'] = pd.Period(pd.Period(end, freq='W').end_time + pd.Timedelta(minutes=1), freq='W').start_time
            reso = 60 * 24 * 7 * reso

        if unit == 'M':
            params['begin'] = pd.Period(begin, freq='M').start_time 
            if end != pd.Period(end, freq='M').start_time:
                params['end'] = pd.Period(pd.Period(end, freq='M').end_time + pd.Timedelta(minutes=1), freq='M').start_time
            params['months'] = 'true'
            
        params['resolution'] = reso
        params['begin'] = int(params['begin'].timestamp() * 1000)
        params['end'] = int(params['end'].timestamp() * 1000)

        return params


    def batch_rows(self, params: dict):
        params = self.align_resolution(params)
        coins = False

        split_params = copy.deepcopy(params)
        if '3m_basis_ann' in params['columns']:
            if 'products' in params or 'exchanges' in params:
                return "To request basis data, please only specify coins. No products or exchanges should be specified."
            split_params['exchanges'] = ['']
            len_exchanges = 3
        else:
            len_exchanges = len(params['exchanges'])
        
        if 'coins' in params:
            if 'products' in params:
                del split_params['coins']
            else:
                split_params['products'] = split_params.pop('coins')
                coins = True            
            
        count = (math.ceil((params['end'] - params['begin']) / (1000 * 60 * params['resolution'])) *
                    len_exchanges * len(split_params['products']) * len(params['columns']))
        
        if count <= 22500 and 'months' not in params:
            split_params['columns'] = (",").join(params['columns'])
            split_params['exchanges'] = (",").join(split_params['exchanges'])
            split_params['products'] = (",").join(split_params['products'])
            if coins:
                split_params['coins'] = split_params.pop('products')
            return [split_params]
        
        count = math.floor(22500 / len_exchanges / len(split_params['products']) / len(params['columns']))
        
        split_params['columns'] = (",").join(params['columns'])
        split_params['exchanges'] = (",").join(split_params['exchanges'])
        split_params['products'] = (",").join(split_params['products'])

        if coins:
            split_params['coins'] = split_params.pop('products')
        
        batches = []
        step = copy.deepcopy(split_params)

        if('months' in step):
            step['end'] = self.add_months(step['begin'], params['resolution'])
        else:
            step['end'] = step['begin'] + ((1000 * 60 * step['resolution']) * count)
        
        batches.append(step)
        
        while step['end'] < params['end']:
            begin = step['end']
            step = copy.deepcopy(split_params)
            step['begin'] = begin
            step['end'] = step['begin'] + ((1000 * 60 * params['resolution']) * count)
            step['end'] = round(min(step['end'], params['end']))
            if 'months' in step:
                step['end'] = self.add_months(step['begin'], params['resolution'])
            batches.append(step)

        return batches
    
    def stream_rows(self, params: dict):        
        for param in params:
            try:
                request = self.http_get(self.base_url + 'rows', params=param, headers=self.headers)
                rows = pd.read_csv(io.StringIO(request))
                yield rows
                time.sleep(0.1)
            except Exception as e:
                if 'No columns to parse from file' not in str(e):
                    print("\nPlease ensure you have passed all required params properly.\n")
                    raise e
                else:
                    yield pd.DataFrame()
            
    def get_rows(self, params: dict):
        params = self.align_resolution(params)
        batches = self.batch_rows(params)        
        rows = pd.DataFrame()
        for param in batches:            
            try:
                request = self.http_get(self.base_url + 'rows', params=param, headers=self.headers)
                rows = pd.concat([rows, pd.read_csv(io.StringIO(request))])
                time.sleep(0.1)
            except Exception as e:
                if 'No columns to parse from file' not in str(e):
                    print("\nPlease ensure you have passed all required params properly.\n")
                    raise e
                else:
                    rows = pd.concat([rows, pd.DataFrame()])
    
        return rows

    def get_market_caps(self, coins):
        coins = {'coins' : (',').join(coins)}
        
        try:
            request = self.http_get(self.base_url + 'caps', params=coins, headers=self.headers)
            rows = pd.read_csv(io.StringIO(request))
        except Exception as e:
            print("\nPlease ensure you have passed all required params properly.\n")
            raise e

        return rows

    def get_term_structure(self, coins):
        coins = {'coins' : (',').join(coins)}

        try:
            request = self.http_get(self.base_url + 'terms', params=coins, headers=self.headers)
            rows = pd.read_csv(io.StringIO(request))
        except Exception as e:
            print("\nPlease ensure you have passed all required params properly.\n")
            raise e

        return rows
    
    
    def timestamp(self):
        return math.floor(time.time() * 1000)
    
    def get_futures_columns(self):
        return [
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'coin_volume',
            'dollar_volume',
            'buy_trades',
            'sell_trades',
            'total_trades',
            'buy_coin_volume',
            'sell_coin_volume',
            'buy_dollar_volume',
            'sell_dollar_volume',
            'coin_open_interest_high',
            'coin_open_interest_low',
            'coin_open_interest_close',
            'dollar_open_interest_high',
            'dollar_open_interest_low',
            'dollar_open_interest_close',
            'funding_rate',
            'funding_rate_avg',
            'premium',
            'buy_liquidations',
            'sell_liquidations',
            'buy_liquidations_coin_volume',
            'sell_liquidations_coin_volume',
            'liquidations_coin_volume',
            'buy_liquidations_dollar_volume',
            'sell_liquidations_dollar_volume',
            'liquidations_dollar_volume'
        ]
    
    def get_options_columns(self):
        return [
            'iv_1w',
            'iv_1m',
            'iv_3m',
            'iv_6m',
            'skew_1w',
            'skew_1m',
            'skew_3m',
            'skew_6m',
            'vega_coins',
            'vega_dollars',
            'call_delta_coins',
            'call_delta_dollars',
            'put_delta_coins',
            'put_delta_dollars',
            'gamma_coins',
            'gamma_dollars',
            'call_volume',
            'call_premium',
            'call_notional',
            'put_volume',
            'put_premium',
            'put_notional',
            'dollar_volume',
            'dvol_open',
            'dvol_high',
            'dvol_low',
            'dvol_close',
            'index_price'
        ]
    
    def get_spot_columns(self):
        return [
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'coin_volume',
            'dollar_volume',
            'buy_trades',
            'sell_trades',
            'total_trades',
            'buy_coin_volume',
            'sell_coin_volume',
            'buy_dollar_volume',
            'sell_dollar_volume',
        ]