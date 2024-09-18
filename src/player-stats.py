from nhlpy import NHLClient

import json

client = NHLClient()

#res = seasonSchedule = client.schedule.get_season_schedule('TOR', '20242025')


# detailed score for every players
#res=client.schedule.get_season_schedule('EDM', '20232024')
#res = client.stats.skater_stats_summary_simple(start_season='20222023', end_season='20222023', default_cayenne_exp='playerId=8479588')
res=client.game_center.landing('2023020003')

f = open("result2.json", "w")
f.write(json.dumps(res))
f.close()
print(len(res))