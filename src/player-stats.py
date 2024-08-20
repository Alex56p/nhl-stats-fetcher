from nhlpy import NHLClient
import json

client = NHLClient()

res = client.stats.player_game_log(player_id="8480382", season_id='20232024', game_type=2)


# detailed score for every players
#res=client.game_center.boxscore(game_id="2023010104")
f = open("result.json", "w")
f.write(json.dumps(res))
f.close()
print(len(res))