import os
import psycopg2
import traceback
from nhlpy import NHLClient
from datetime import datetime, timedelta
from controllers.game import *
from controllers.player import *
from controllers.goaler import *
client = NHLClient()

def updateSeasonGames():
    games = get_season_games()
    save_games_to_db(games)

def updateWeeklyStats():
    # 1. Get the stats from the last week.
    startDate = datetime(2023,10,10) - timedelta(days=6)
    
    gamelogs = []
    goalerlogs = []
    
    games = get_week_games(startDate)
    print ('Got ' + str(len(games)) + ' games to get gamelogs from')
    for game in games:
        # Game is not started, no gamelogs
        if game.gameOutcome != "FUT":
            boxScore = client.game_center.boxscore(game.gameId)
            gameGoals = client.game_center.landing(game.gameId)["summary"]["scoring"]

            # Away team
            # Forwards
            gamelogs += fetch_player_log(boxScore["playerByGameStats"]["awayTeam"]["forwards"], game, gameGoals, True)
            # defense
            gamelogs += fetch_player_log(boxScore["playerByGameStats"]["awayTeam"]["defense"], game, gameGoals, True)
            # goalies
            goalerlogs += fetch_goaler_log(boxScore["playerByGameStats"]["awayTeam"]["goalies"], game, gameGoals, True)

            # Home team
            # Forwards
            gamelogs += fetch_player_log(boxScore["playerByGameStats"]["homeTeam"]["forwards"], game, gameGoals, False)
            # defense
            gamelogs += fetch_player_log(boxScore["playerByGameStats"]["homeTeam"]["defense"], game, gameGoals, False)
            # goalies
            goalerlogs += fetch_goaler_log(boxScore["playerByGameStats"]["homeTeam"]["goalies"], game, gameGoals, False)

    
    print('Got: ' + str(len(gamelogs)) + ' gamelogs')
    save_player_gamelogs_to_db(gamelogs)
    print('Got: ' + str(len(goalerlogs)) + 'goaler gamelogs')
    save_goaler_gamelogs_to_db(goalerlogs)


if __name__ == '__main__':
    updateSeasonGames()
    updateWeeklyStats()