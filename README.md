# velo-python
Python library for Velo API access see full documentation [here](https://velodata.gitbook.io/velo-data-api/nodejs).

## Install
```
pip install velodata
```

## Usage
```python
import time, math
from velodata import lib as velo

# new velo client
client = velo.client('api_key')

# get all futures and pick one
future = client.get_futures()[0] 

# last 10 minutes in 1 minute resolution
params = {
      'type': 'futures',
      'columns': ['open_price', 'close_price'],
      'exchanges': [future['exchange']],
      'products': [future['product']],
      'begin': math.floor(time.time() * 1000) - 1000 * 60 * 11,
      'end': math.floor(time.time() * 1000),
      'resolution': 1
    }
    
# returns dataframe
print(client.get_rows(params)) 
```

## Streaming rows
##### Row requests are broken up into batches past a certain size. If you don't want to wait for all your requests to finish before receiving any data, you can use this:


```python
batches = client.batch_rows(params)  
for df in client.stream_rows(batches):
  print(df)
```

## License
Copyright 2023 Velo Data, license MIT
