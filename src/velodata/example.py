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
      'exchanges': [random_future[0]],
      'products': [random_future[2]],
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