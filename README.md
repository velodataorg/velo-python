# velo-python
Python library for Velo API access see full documentation [here](https://velodata.gitbook.io/velo-data-api/nodejs).

## Install
```
pip install velodata
```

## Usage
```python
import time, math, random
from velodata import lib as velo

client = velo.client('api_key')

def get_random_future():
    futures = client.get_futures()
    random_future = math.floor(random.random() * len(futures))
    random_future = futures[random_future]
    print('random future\n', random_future)
    return random_future

def doWork():
    random_future = get_random_future()

    params = {
      'type': 'futures',
      'columns': ['open_price', 'close_price'],
      'exchanges': [random_future['exchange']],
      'products': [random_future['product']],
      'begin': math.floor(time.time()*1000) - 1000 * 60 * 11, # 10 minutes
      'end': math.floor(time.time()*1000),
      'resolution': 1 # 1 minute
    }
    
    # stream batches
    batches = client.batch_rows(params)  
    for df in client.stream_rows(batches):
        print(df)
        
    # get back all in one
    print(client.get_rows(params))
    

doWork()
```

## License
Copyright 2023 Velo Data, license MIT
