from riotwatcher import LolWatcher, ApiError
from astrapy.rest import create_client, http_methods

api_key = 'RGAPI-521709eb-7a32-4f43-aae5-f3641c938600'
watcher = LolWatcher(api_key)
my_region = 'la1'

summoners = [
    "Mezcal Cuishe",
    "Mezcal Tobala",
    "Mezcal Espadin",
    "Mezcal Tobaishe",
    "Mezcal Jabali",
    "RCG MrchomLee"
]

ASTRA_DB_ID='a16b65fe-1cb5-481b-9917-6954f9a991a6'
ASTRA_DB_REGION='us-east1'
ASTRA_DB_APPLICATION_TOKEN='AstraCS:eoieafCuPGsGIZTMFEqpwbqk:d5569624615e5b685bd2656af1bd2572c713798295d1c8a08988fdae3ea1480e'
ASTRA_DB_KEYSPACE='mezcal'

astra_http_client = create_client(
      astra_database_id=ASTRA_DB_ID,
      astra_database_region=ASTRA_DB_REGION,
      astra_application_token=ASTRA_DB_APPLICATION_TOKEN)

def get_summoner_rank(summoner):
  summoner_info = watcher.summoner.by_name(my_region, summoner)
  summoner_stats = watcher.league.by_summoner(my_region, summoner_info['id'])
  return summoner_stats

def send_data_to_astra(record):
  summoner_name = record.pop('summonerName')
  queue_type = record.pop('queueType')
  record_min = {clave.lower(): valor for clave, valor in record.items()}
  response = astra_http_client.request(
  method=http_methods.PUT,
  path=f"/api/rest/v2/keyspaces/mezcal/raking/{summoner_name}/{queue_type}",
  json_data=record_min)

def main(args):
    stats = []
    for summoner in summoners:
        stats = stats + get_summoner_rank(summoner)
    print(stats)
    for stat in stats:
      send_data_to_astra(stat)
    return {"body": stats}