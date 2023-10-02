import requests
import time
import math
import base64
import pandas as pd
import io 
import copy

class client:
    def __init__(self, key):
        self.key = "Basic " + str(base64.b64encode(b"api:" + key.encode("utf-8")))[2:-1]
        self.base_url = "https://velodata.app/api/v1/"
        self.headers = {"Authorization": self.key}
        self.session = requests.Session()
        
    def http_get(self, base_url, headers, params={}):
        request = self.session.get(base_url, params=params, headers=headers)
        code = request.status_code
        
        if code == 429:
            time.sleep(2)
            return self.http_get(base_url, headers, params)
        elif code == 200:
            return request.text
        else:
            return request
        
    def get_status(self):
        return self.http_get(self.base_url + "status", headers=self.headers)
        
    def get_products(self, product_type: str):        
        request = self.session.get(self.base_url + product_type, headers=self.headers).text
        
        try:
            return pd.read_csv(io.StringIO(request)).to_dict('records')
        except:
            print("Please check that you have provided a product_type of futures, spot, or options.")
            raise Exception(request)

    def get_futures(self):
        return self.get_products('futures')
            
    def get_options(self):
        return self.get_products('options')
    
    def get_spot(self):
        return self.get_products('spot')

    def batch_rows(self, params: dict):
        count = (math.ceil((params['end'] - params['begin']) / (1000 * 60 * params['resolution'])) *
                    len(params['exchanges']) * len(params['products']) * len(params['columns']))
        
        
        if count <= 22500:
            return [params]
        
        count = 22500 / len(params['exchanges']) / len(params['products']) / len(params['columns'])
        
        split_params = copy.deepcopy(params)
        split_params['columns'] = (",").join(params['columns'])
        split_params['exchanges'] = (",").join(params['exchanges'])
        split_params['products'] = (",").join(params['products'])
        
        batches = []
        step = copy.deepcopy(split_params)
        step['end'] = step['begin'] + ((1000 * 60 * step['resolution']) * count)
        
        batches.append(step)
        
        while step['end'] < params['end']:
            begin = step['end']
            step = copy.deepcopy(split_params)
            step['begin'] = begin
            step['end'] = step['begin'] + ((1000 * 60 * params['resolution']) * count)
            step['end'] = round(min(step['end'], params['end']))
            batches.append(step)
            
        return batches
        
    
    
    def stream_rows(self, params: dict):        
        for param in params:
            request = self.http_get(self.base_url + 'rows', params=param, headers=self.headers)
            rows = pd.read_csv(io.StringIO(request))
            yield rows
            time.sleep(0.1)
            
    def get_rows(self, params: dict):
        batches = self.batch_rows(params)        
                    
        rows = pd.DataFrame()
        for param in batches:            
            try:
                request = self.http_get(self.base_url + 'rows', params=param, headers=self.headers)
                rows = pd.concat([rows, pd.read_csv(io.StringIO(request))])
                time.sleep(0.1)
            except:
                print("Please ensure you have passed all required params properly.")
                raise Exception(request)
    
        return rows