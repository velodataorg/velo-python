from velodata import lib as velo

# new velo client
client = velo.client('api_key')

# get past stories
print(client.news.get_news())

# stream new stories
async for message in client.news.stream_news():
    if(message in ('connected', 'heartbeat', 'closed')):
        print(message)
    else:
        print(json.loads(message))