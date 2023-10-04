# velo-python
Python library for Velo API access. [See full documentation here](https://velodata.gitbook.io/velo-data-api/python).

## Install
```
pip install velodata
```

## Usage
```python
from velodata import lib as velo

# new velo client
client = velo.client('api_key')

# get futures and pick one
future = client.get_futures()[0] 

# get futures columns and pick two
columns = client.get_futures_columns()[:2]

# last 10 minutes in 1 minute resolution
params = {
      'type': 'futures',
      'columns': columns,
      'exchanges': [future['exchange']],
      'products': [future['product']],
      'begin': client.timestamp() - 1000 * 60 * 11,
      'end': client.timestamp(),
      'resolution': 1
    }
    
# returns dataframe
print(client.get_rows(params))
```

## Streaming rows
Row requests are broken up into batches past a certain size. If you don't want to wait for all your requests to finish before receiving any data, you can use this:


```python
batches = client.batch_rows(params)  
for df in client.stream_rows(batches):
  print(df)
```

## License
Copyright 2023 Velo Data, license MIT
