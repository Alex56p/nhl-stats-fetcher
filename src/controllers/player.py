import os
import psycopg2
import traceback
from nhlpy import NHLClient
from datetime import datetime
import requests
from controllers.game import Game, get_season_games, fetch_strength_points, fetch_ot_goals
from controllers.season import get_season_from_date

client = NHLClient()
playerIds = []


class Player:
    def __init__(
        self,
        playerId,
        fullName = None,
        goals = None,
        assists = None,
        evGoals = None,
        evPoints = None,
        faceoffWinPct = None,
        gameWinningGoals = None,
        gamesPlayed = None,
        otGoals = None,
        penaltyMinutes = None,
        plusMinus = None,
        points = None,
        pointsPerGame = None,
        positionCode = None,
        ppGoals = None,
        ppPoints = None,
        shGoals = None,
        shPoints = None,
        shootingPct = None,
        shootsCatches = None,
        shots = None,
        teamAbbrev = None,
        timeOnIcePerGame = None,
        headshot = None,
        age = None,
        height = None,
        weight = None,
        birthCountry = None,
    ):
        self.playerId = playerId
        self.fullName = fullName
        self.goals = goals
        self.assists = assists
        self.evGoals = evGoals
        self.evPoints = evPoints
        self.faceoffWinPct = faceoffWinPct
        self.gameWinningGoals = gameWinningGoals
        self.gamesPlayed = gamesPlayed
        self.otGoals = otGoals
        self.penaltyMinutes = penaltyMinutes
        self.plusMinus = plusMinus
        self.points = points
        self.pointsPerGame = pointsPerGame
        self.positionCode = positionCode
        self.ppGoals = ppGoals
        self.ppPoints = ppPoints
        self.shGoals = shGoals
        self.shPoints = shPoints
        self.shootingPct = shootingPct
        self.shootsCatches = shootsCatches
        self.shots = shots
        self.teamAbbrev = teamAbbrev
        self.timeOnIcePerGame = timeOnIcePerGame
        self.headshot = headshot
        self.age = age
        self.height = height
        self.weight = weight
        self.birthCountry = birthCountry

    def values(self):
        return (
            self.playerId,
            self.fullName,
            self.goals,
            self.assists,
            self.evGoals,
            self.evPoints,
            self.faceoffWinPct,
            self.gameWinningGoals,
            self.gamesPlayed,
            self.otGoals,
            self.penaltyMinutes,
            self.plusMinus,
            self.points,
            self.pointsPerGame,
            self.positionCode,
            self.ppGoals,
            self.ppPoints,
            self.shGoals,
            self.shPoints,
            self.shootingPct,
            self.shootsCatches,
            self.shots,
            self.teamAbbrev,
            self.timeOnIcePerGame,
            self.headshot,
            self.age,
            self.height,
            self.weight,
            self.birthCountry,
        )
        
    def update_infos(self, date: datetime, skater_stats = None):
        currentSeason = get_season_from_date(date)
        
        if skater_stats is None:
            skater_stats = client.stats.skater_stats_summary_simple(start_season=currentSeason, end_season=currentSeason, default_cayenne_exp='playerId=' + str(self.playerId))
            if len(skater_stats) > 0:
                skater_stats = skater_stats[0]
            else:
                print('player not found ' + str(self.playerId))
                return None

        self.playerId = skater_stats['playerId']
        self.fullName = skater_stats['skaterFullName']
        self.goals = skater_stats['goals']
        self.assists = skater_stats['assists']
        self.evGoals = skater_stats['evGoals']
        self.evPoints = skater_stats['evPoints']
        self.faceoffWinPct = skater_stats['faceoffWinPct']
        self.gameWinningGoals = skater_stats['gameWinningGoals']
        self.gamesPlayed = skater_stats['gamesPlayed']
        self.otGoals = skater_stats['otGoals']
        self.penaltyMinutes = skater_stats['penaltyMinutes']
        self.plusMinus = skater_stats['plusMinus']
        self.points = skater_stats['points']
        self.pointsPerGame = skater_stats['pointsPerGame']
        self.positionCode = skater_stats['positionCode']
        self.ppGoals = skater_stats['ppGoals']
        self.ppPoints = skater_stats['ppPoints']
        self.shGoals = skater_stats['shGoals']
        self.shPoints = skater_stats['shPoints']
        self.shootingPct = skater_stats['shootingPct']
        self.shootsCatches = skater_stats['shootsCatches']
        self.shots = skater_stats['shots']
        self.teamAbbrev = skater_stats['teamAbbrevs'][-3:] # keep the last 3 for most recent team
        self.timeOnIcePerGame = skater_stats['timeOnIcePerGame']
        self.headshot = 'https://assets.nhle.com/mugs/nhl/' + str(currentSeason) + '/' + str(self.playerId) + '/' + str(self.teamAbbrev) + '.png'
        
        url = 'https://api-web.nhle.com/v1/player/' + str(self.playerId) + '/landing'
        response = requests.get(url)

        if response.status_code == 200:
            result = response.json()
            birthDate = datetime.strptime(result['birthDate'], "%Y-%m-%d")
            today = datetime.now()
            self.age = today.year - birthDate.year - ((today.month, today.day) < (birthDate.month, birthDate.day))
            self.height = result['heightInInches']
            self.weight = result['weightInPounds']
            self.birthCountry = result['birthCountry']
            
        return self
    
def save_players_to_db(players, updateOnConflict=True):
    if len(players) == 0:
        return
    
    try:
        with psycopg2.connect(
            database=os.environ.get("DATABASE_URL", "nhl-stats-fetcher"),
            user=os.environ.get("DATABASE_USERNAME", "py-user"),
            password=os.environ.get("DATABASE_PASSWORD"),
            host=os.environ.get("DATABASE_URL", "127.0.0.1"),
            port=os.environ.get("DATABASE_PORT", "5433"),
        ) as conn:
            with conn.cursor() as cur:
                if updateOnConflict:
                    
                    insert_sql = """
                        INSERT INTO players values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (playerId) DO UPDATE
                            SET fullName = excluded.fullName,
                                goals = excluded.goals,
                                assists = excluded.assists,
                                evGoals = excluded.evGoals,
                                evPoints = excluded.evPoints,
                                faceoffWinPct = excluded.faceoffWinPct,
                                gameWinningGoals = excluded.gameWinningGoals,
                                gamesPlayed = excluded.gamesPlayed,
                                otGoals = excluded.otGoals,
                                penaltyMinutes = excluded.penaltyMinutes,
                                plusMinus = excluded.plusMinus,
                                points = excluded.points,
                                pointsPerGame = excluded.pointsPerGame,
                                positionCode = excluded.positionCode,
                                ppGoals = excluded.ppGoals,
                                ppPoints = excluded.ppPoints,
                                shGoals = excluded.shGoals,
                                shPoints = excluded.shPoints,
                                shootingPct = excluded.shootingPct,
                                shootsCatches = excluded.shootsCatches,
                                shots = excluded.shots,
                                teamAbbrev = excluded.teamAbbrev,
                                timeOnIcePerGame = excluded.timeOnIcePerGame,
                                headshot = excluded.headshot,
                                age = excluded.age,
                                height = excluded.height,
                                weight = excluded.weight,
                                birthCountry = excluded.birthCountry
                    """
                else:
                    insert_sql = """
                        INSERT INTO players values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (playerId) DO NOTHING
                    """
                print('Inserting ' + str(len(players)) + ' players')
                cur.executemany(insert_sql, [player.values() for player in players])
                print('Done.')
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()

def save_players_if_not_exists(playersIds, gameDate = datetime.now()):
    try:
        with psycopg2.connect(
            database=os.environ.get("DATABASE_URL", "nhl-stats-fetcher"),
            user=os.environ.get("DATABASE_USERNAME", "py-user"),
            password=os.environ.get("DATABASE_PASSWORD"),
            host=os.environ.get("DATABASE_URL", "127.0.0.1"),
            port=os.environ.get("DATABASE_PORT", "5433"),
        ) as conn:
            with conn.cursor() as cur:
                select_sql = """
                    SELECT playerId from players where playerId IN %s
                """
                strPlayersIds = [str(playerId) for playerId in playersIds]
                cur.execute(select_sql, (tuple(strPlayersIds),))
                saved_players = cur.fetchall()
                saved_players = [player[0] for player in saved_players]
                players_to_insert = [player for player in strPlayersIds if player not in saved_players]
                
                if len(players_to_insert) > 0:
                    save_players_to_db([Player(playerId=player).update_infos(gameDate) for player in players_to_insert])
                
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()

class PlayerGameLog:
    def __init__(
        self,
        gameLogsId,
        gameId,
        playerId,
        teamAbbrev,
        homeRoadFlag,
        gameDate,
        goals,
        assists,
        points,
        plusMinus,
        powerPlayGoals,
        powerPlayPoints,
        gameWinningGoals,
        otGoals,
        shots,
        shifts,
        shortHandedGoals,
        shortHandedPoints,
        opponentAbbrev,
        pim,
        toi,
        gameType,
    ):
        self.gameLogsId = gameLogsId
        self.gameId = gameId
        self.playerId = playerId
        self.teamAbbrev = teamAbbrev
        self.homeRoadFlag = homeRoadFlag
        self.gameDate = gameDate
        self.goals = goals
        self.assists = assists
        self.points = points
        self.plusMinus = plusMinus
        self.powerPlayGoals = powerPlayGoals
        self.powerPlayPoints = powerPlayPoints
        self.gameWinningGoals = gameWinningGoals
        self.otGoals = otGoals
        self.shots = shots
        self.shifts = shifts
        self.shortHandedGoals = shortHandedGoals
        self.shortHandedPoints = shortHandedPoints
        self.opponentAbbrev = opponentAbbrev
        self.pim = pim
        self.toi = toi
        self.gameType = gameType

    def values(self):
        return (
            self.gameLogsId,
            self.gameId,
            self.playerId,
            self.teamAbbrev,
            self.homeRoadFlag,
            self.gameDate,
            self.goals,
            self.assists,
            self.points,
            self.plusMinus,
            self.powerPlayGoals,
            self.powerPlayPoints,
            self.gameWinningGoals,
            self.otGoals,
            self.shots,
            self.shifts,
            self.shortHandedGoals,
            self.shortHandedPoints,
            self.opponentAbbrev,
            self.pim,
            self.toi,
            self.gameType,
        )

def save_player_gamelogs_to_db(playergamelogs):
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASSWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                    gameLogsSql = ''' INSERT INTO gamelogs(
                                        gameLogsId,                                
                                        gameId, 
                                        playerId, 
                                        teamAbbrev, 
                                        homeRoadFlag, 
                                        gameDate, 
                                        goals, 
                                        assists, 
                                        points, 
                                        plusMinus, 
                                        powerPlayGoals, 
                                        powerPlayPoints, 
                                        gameWinningGoals, 
                                        otGoals, 
                                        shots, 
                                        shifts, 
                                        shortHandedGoals, 
                                        shortHandedPoints, 
                                        opponentAbbrev, 
                                        pim, 
                                        toi, 
                                        gameType
                                    ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                                    ON CONFLICT (gameLogsId) DO UPDATE
                                        SET gameId = excluded.gameId, 
                                            playerId = excluded.playerId, 
                                            teamAbbrev = excluded.teamAbbrev, 
                                            homeRoadFlag = excluded.homeRoadFlag, 
                                            gameDate = excluded.gameDate, 
                                            goals = excluded.goals, 
                                            assists = excluded.assists, 
                                            points = excluded.points, 
                                            plusMinus = excluded.plusMinus, 
                                            powerPlayGoals = excluded.powerPlayGoals, 
                                            powerPlayPoints = excluded.powerPlayPoints, 
                                            gameWinningGoals = excluded.gameWinningGoals, 
                                            otGoals = excluded.otGoals, 
                                            shots = excluded.shots, 
                                            shifts = excluded.shifts, 
                                            shortHandedGoals = excluded.shortHandedGoals, 
                                            shortHandedPoints = excluded.shortHandedPoints, 
                                            opponentAbbrev = excluded.opponentAbbrev, 
                                            pim = excluded.pim, 
                                            toi = excluded.toi, 
                                            gameType = excluded.gameType
                                '''
                    print('Saving ' + str(len(playergamelogs)) + ' game logs')
                    cur.executemany(gameLogsSql, [playergamelog.values() for playergamelog in playergamelogs])
                    print ('Done.')
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()

def fetch_player_log(playersBoxScore, game: Game, gameGoals, away: bool):
    gamelogs = []
    playerIds = []
    for playerBoxScore in playersBoxScore:
        playerIds.append(playerBoxScore['playerId'])
        gamelogs.append(PlayerGameLog(
            str(game.gameId) + str(playerBoxScore['playerId']),
            game.gameId,
            playerBoxScore['playerId'],
            game.awayTeamAbbrev if away else game.homeTeamAbbrev,
            'R' if away else 'H',
            game.startTimeUTC,
            playerBoxScore['goals'],
            playerBoxScore['assists'],
            playerBoxScore['points'],
            playerBoxScore['plusMinus'],
            playerBoxScore['powerPlayGoals'],
            fetch_strength_points(playerBoxScore['playerId'], gameGoals, 'pp'),
            0,#1 if 'winningGoalScorer' in game and game['winningGoalScorer']['playerId'] == playerBoxScore['playerId'] else 0, # TODO: GAME
            0 if game.gameOutcome == 'REG' else fetch_ot_goals(playerBoxScore['playerId'], gameGoals),
            playerBoxScore['shots'],
            0, # shifts
            fetch_strength_points(playerBoxScore['playerId'], gameGoals, 'sh', True, False),
            fetch_strength_points(playerBoxScore['playerId'], gameGoals, 'sh'),
            game.homeTeamAbbrev if away else game.awayTeamAbbrev,
            playerBoxScore['pim'],
            playerBoxScore['toi'] if 'toi' in playerBoxScore else '00:00',
            game.gameType,
        ))
    
    save_players_if_not_exists(playerIds, datetime.strptime(game.startTimeUTC, '%Y-%m-%dT%H:%M:%SZ'))
    return gamelogs
