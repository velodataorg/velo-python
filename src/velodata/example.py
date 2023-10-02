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