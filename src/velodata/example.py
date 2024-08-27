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
      'resolution': '1m'
    }
    
# returns dataframe
print(client.get_rows(params))