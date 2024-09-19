from nhlpy import NHLClient
from espn_api.hockey import League

import json

client = NHLClient()

#res = seasonSchedule = client.schedule.get_season_schedule('TOR', '20242025')


# detailed score for every players
#res=client.schedule.get_season_schedule('EDM', '20232024')
#res = client.stats.skater_stats_summary_simple(start_season='20222023', end_season='20222023', default_cayenne_exp='playerId=8479588')
res=client.game_center.landing('2023020003')

league = League(22653, 2024, "AEBu7jT%2BfI0b5rByFcgyg%2B4zlqhboE8lQgH7TZ5BEYaQZIWVlrGKYDwbtGRVs65lrmT5eHAa%2FsajRgFTVycp3n70jwXsIcrMrg008bkvYVbbm%2FrHTpUzVfWbwJawrNaeCv8Cp9RoznBiGBkCvXrDyLgIdheH7xzjg%2BdRHwAezLQu90%2FxJoa4y2TG8cfJLNQVuPh2n8szfOjQT4YAv8esda9ktPvlH87xYffpM6SkJWBlb3H%2FVDW3o9nG0Isf1%2BFDca5N6Md25Rl3dZHHqZ1ph98Y" , "CCFEF2DB-0CC1-45DF-B36F-DAFD249EFB71")

draft=league.settings

for basePick in draft:
    print({
        basePick.team,
        basePick.playerId,
        basePick.playerName,
        basePick.round_num,
        basePick.round_pick,
        basePick.bid_amount,
        basePick.keeper_status,
        basePick.nominatingTeam,
    })

# f = open("result2.json", "w")
# f.write(json.dumps(res))
# f.close()
# print(len(res))