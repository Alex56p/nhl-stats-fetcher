from nhlpy import NHLClient

import json

client = NHLClient()

#res = seasonSchedule = client.schedule.get_season_schedule('TOR', '20242025')


# detailed score for every players
#res=client.schedule.get_season_schedule('EDM', '20232024')
res = client.stats.skater_stats_summary_simple('20232024', '20232024', fact_cayenne_exp='playerId=8476856')
#res=client.game_center.boxscore('2023010082')
f = open("result2.json", "w")
f.write(json.dumps(res))
f.close()
print(len(res))