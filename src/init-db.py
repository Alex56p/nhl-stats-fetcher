import os
import requests
import psycopg2
import json
from nhlpy import NHLClient
from datetime import datetime
import traceback


def generate_db():

    commands = [
        '''
        CREATE TABLE teams (
            id INTEGER PRIMARY KEY,
            fullName VARCHAR(255),
            commonName VARCHAR(255),
            placeName VARCHAR(255),
            abbrev VARCHAR(10) UNIQUE,
            logo VARCHAR(1000)
        )
        ''',
        '''
        CREATE TABLE players (
            playerId VARCHAR(255) PRIMARY KEY,
            fullName VARCHAR(255) NOT NULL,
            goals INTEGER,
            assists INTEGER,
            evGoals INTEGER,
            evPoints INTEGER,
            faceoffWinPct NUMERIC,
            gameWinningGoals INTEGER,
            gamesPlayed INTEGER,
            otGoals INTEGER,
            penaltyMinutes INTEGER,
            plusMinus INTEGER,
            points INTEGER,
            pointsPerGame NUMERIC,
            positionCode VARCHAR(1),
            ppGoals INTEGER,
            ppPoints INTEGER,
            shGoals INTEGER,
            shPoints INTEGER,
            shootingPct NUMERIC,
            shootsCatches VARCHAR(1),
            shots INTEGER,
            teamAbbrev VARCHAR(10),
            timeOnIcePerGame NUMERIC,
            headshot VARCHAR(1000),
            age INTEGER,
            height NUMERIC,
            weight NUMERIC,
            birthCountry VARCHAR(100),
            
            FOREIGN KEY (teamAbbrev)
                REFERENCES teams (abbrev)
                ON UPDATE NO ACTION ON DELETE NO ACTION
        )
        ''',
        '''
        CREATE TABLE goalers (
            playerId VARCHAR(255) PRIMARY KEY,
            fullName VARCHAR(255) NOT NULL,
            assists INTEGER,
            gamesPlayed INTEGER,
            gamesStarted INTEGER,
            goals INTEGER,
            goalsAgainst INTEGER,
            goalsAgainstAverage NUMERIC,
            losses INTEGER,
            otLosses INTEGER,
            penaltyMinutes INTEGER,
            points INTEGER,
            savePct NUMERIC,
            saves INTEGER,
            shootsCatches VARCHAR(1),
            shotsAgainst INTEGER,
            shutouts INTEGER,
            teamAbbrev VARCHAR(3),
            ties INTEGER,
            timeOnIce NUMERIC,
            wins INTEGER,
            
            FOREIGN KEY (teamAbbrev)
            REFERENCES teams (abbrev)
            ON UPDATE NO ACTION ON DELETE NO ACTION
        )
        ''',
        '''
        CREATE TABLE games (
            gameId VARCHAR(255) PRIMARY KEY,
            season VARCHAR(255),
            gameType VARCHAR(1),
            startTimeUTC DATE,
            venue VARCHAR(255),
            awayTeamAbbrev VARCHAR(3),
            awayTeamScore INTEGER,
            homeTeamAbbrev VARCHAR(3),
            homeTeamScore INTEGER,
            gameOutcome VARCHAR(255),
            recapLink VARCHAR(1000),
            gameCenterLink VARCHAR(1000)
        )''',
        '''
        CREATE TABLE gamelogs (
            gameLogsId VARCHAR(255) PRIMARY KEY,
            gameId VARCHAR(255),
            playerId VARCHAR(255),
            teamAbbrev VARCHAR(10),
            homeRoadFlag VARCHAR(1),
            gameDate DATE,
            goals INTEGER,
            assists INTEGER,
            points INTEGER,
            plusMinus INTEGER,
            powerPlayGoals INTEGER,
            powerPlayPoints INTEGER,
            gameWinningGoals INTEGER,
            otGoals INTEGER,
            shots INTEGER,
            shifts INTEGER,
            shorthandedGoals INTEGER,
            shorthandedPoints INTEGER,
            opponentAbbrev VARCHAR(10),
            pim INTEGER,
            toi INTERVAL,
            gameType INTEGER,

            FOREIGN KEY (playerId)
                REFERENCES players (playerId)
                ON UPDATE NO ACTION ON DELETE NO ACTION,

            FOREIGN KEY (teamAbbrev)
                REFERENCES teams (abbrev)
                ON UPDATE NO ACTION ON DELETE NO ACTION,

            FOREIGN KEY (opponentAbbrev)
                REFERENCES teams (abbrev)
                ON UPDATE NO ACTION ON DELETE NO ACTION
        )''',
        '''
        CREATE TABLE goalerGameLogs(
            gameLogsId VARCHAR(255) PRIMARY KEY,
            gameId VARCHAR(255),
            playerId VARCHAR(255),
            teamAbbrev VARCHAR(3),
            homeRoadFlag VARCHAR(1),
            gameDate DATE,
            goals INTEGER,
            assists INTEGER,
            gamesStarted INTEGER,
            decision VARCHAR(10),
            shotsAgainst INTEGER,
            goalsAgainst INTEGER,
            savePctg NUMERIC,
            shutouts INTEGER,
            opponentAbbrev VARCHAR(3),
            pim INTEGER,
            toi INTERVAL,
            gameType INTEGER,
            
            FOREIGN KEY (playerId)
            REFERENCES goalers (playerId)
            ON UPDATE NO ACTION ON DELETE NO ACTION,

            FOREIGN KEY (teamAbbrev)
            REFERENCES teams (abbrev)
            ON UPDATE NO ACTION ON DELETE NO ACTION,

            FOREIGN KEY (opponentAbbrev)
            REFERENCES teams (abbrev)
            ON UPDATE NO ACTION ON DELETE NO ACTION,
            
            FOREIGN KEY (gameId)
            REFERENCES games (gameId)
            ON UPDATE NO ACTION ON DELETE NO ACTION
        )
        '''
    ]

    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                for command in commands:
                    result = cur.execute(command)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

    print('Init db done.')

def insertTeams():
    franchises_data = []
    with open('./data/franchises.json', 'r') as f:
        franchises = json.load(f)
        for franchise in franchises['data']:
            franchises_data.append((
                franchise['id'],
                franchise['fullName'],
                franchise['teamCommonName'],
                franchise['teamPlaceName'],
                franchise['abbrev']
            ))

    print(franchises_data)
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                sql = 'INSERT INTO teams(id, fullName, commonName, placeName, abbrev) values(%s, %s, %s, %s, %s) RETURNING *'
                result = cur.executemany(sql, franchises_data)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def insertPlayers(start_season, end_season):
    client = NHLClient()
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                for i in range(start_season, end_season):
                    current = i+1 == datetime.now().year
                    insert_sql = 'INSERT INTO players(playerId, fullName, positionCode, teamAbbrev) values(%s,%s,%s,%s) RETURNING *'

                    # We want all stats for current season
                    if current:
                        insert_sql = '''
                            INSERT INTO players(

                            ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                            RETURNING *
                        '''

                    season = str(i) + str(i + 1)
                    print('fetching season ' + season)
                    players = client.stats.skater_stats_summary_simple(start_season=season, end_season=season, limit=-1)

                    print('got ' + str(len(players)) + ' players')
                    players_db = []

                    for player in players:
                        # Check if player already exists
                        sql = 'SELECT 1 FROM players WHERE playerId = %s'
                        cur.execute(sql, (str(player['playerId']),))

                        if(cur.fetchone() is None):
                            if current:
                                players_db.append((
                                    player['playerId'],
                                    player['skaterFullName'],
                                    player['goals'],
                                    player['assists'],
                                    player['evGoals'],
                                    player['evPoints'],
                                    player['faceoffWinPct'],
                                    player['gameWinningGoals'],
                                    player['gamesPlayed'],
                                    player['otGoals'],
                                    player['penaltyMinutes'],
                                    player['plusMinus'],
                                    player['points'],
                                    player['pointsPerGame'],
                                    player['positionCode'],
                                    player['ppGoals'],
                                    player['ppPoints'],
                                    player['shGoals'],
                                    player['shPoints'],
                                    player['shootingPct'],
                                    player['shootsCatches'],
                                    player['shots'],
                                    player['teamAbbrevs'][len(player['teamAbbrevs']) - 3:], # keep the last 3 for most recent team
                                    player['timeOnIcePerGame']
                                ))
                            else:
                                players_db.append((
                                    player['playerId'],
                                    player['skaterFullName'],
                                    player['positionCode'],
                                    player['teamAbbrevs'][len(player['teamAbbrevs']) - 3:], # keep the last 3 for most recent team
                                ))

                    if(len(players_db) > 0):
                        print('inserting ' + str(len(players_db)) + ' players')
                        # insert query
                        cur.executemany(insert_sql, players_db)
                        conn.commit()
                    else:
                        print('nothing to insert')

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def insertGoalers(start_season, end_season):
    client = NHLClient()
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                for i in range(start_season, end_season):
                    current = i+1 == datetime.now().year
                    insert_sql = 'INSERT INTO goalers(playerId, fullName, shootsCatches, teamAbbrev) values(%s,%s,%s,%s) RETURNING *'

                    # We want all stats for current season
                    if current:
                        insert_sql = '''
                            INSERT INTO goalers values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                            RETURNING *
                        '''

                    season = str(i) + str(i + 1)
                    print('fetching season ' + season)
                    goalers = client.stats.goalie_stats_summary_simple(start_season=season, end_season=season, limit=-1)

                    print('got ' + str(len(goalers)) + ' goalers')
                    players_db = []

                    for goaler in goalers:
                        # Check if player already exists
                        sql = 'SELECT 1 FROM goalers WHERE playerId = %s'
                        cur.execute(sql, (str(goaler['playerId']),))

                        if(cur.fetchone() is None):
                            if current:
                                players_db.append((
                                    goaler['playerId'],
                                    goaler['goalieFullName'],
                                    goaler['assists'], 
                                    goaler['gamesPlayed'], 
                                    goaler['gamesStarted'], 
                                    goaler['goals'], 
                                    goaler['goalsAgainst'], 
                                    goaler['goalsAgainstAverage'], 
                                    goaler['losses'], 
                                    goaler['otLosses'], 
                                    goaler['penaltyMinutes'], 
                                    goaler['points'], 
                                    goaler['savePct'], 
                                    goaler['saves'], 
                                    goaler['shootsCatches'],
                                    goaler['shotsAgainst'], 
                                    goaler['shutouts'], 
                                    goaler['teamAbbrevs'][len(goaler['teamAbbrevs']) - 3:], # keep the last 3 for most recent team                                    
                                    goaler['ties'], 
                                    goaler['timeOnIce'], 
                                    goaler['wins'], 
                                ))
                            else:
                                players_db.append((
                                    goaler['playerId'],
                                    goaler['goalieFullName'],
                                    goaler['shootsCatches'],
                                    goaler['teamAbbrevs'][len(goaler['teamAbbrevs']) - 3:], # keep the last 3 for most recent team
                                ))

                    if(len(players_db) > 0):
                        print('inserting ' + str(len(players_db)) + ' goalers')
                        # insert query
                        cur.executemany(insert_sql, players_db)
                        conn.commit()
                    else:
                        print('nothing to insert')

    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()

def insertGameLogs(start_season, end_season):
    client = NHLClient()
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                select_sql = 'SELECT playerId from players'
                cur.execute(select_sql)
                players = cur.fetchall()
                index = 0
                for player in players:
                    for i in range(start_season, end_season):
                        insert_sql = ''' INSERT INTO gamelogs(
                                            gameLogsId                                
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
                                        ) values(%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                                        RETURNING *
                                    '''
                        season = str(i) + str(i + 1)
                        game_logs_db = []

                        for game_type in range(1,4):
                            try:
                                game_logs = client.stats.player_game_log(player_id=player[0], season_id=season, game_type=game_type)
                                for game_log in game_logs:
                                    # Check if player already exists
                                    sql = 'SELECT 1 FROM gamelogs WHERE playerId = %s AND gameId = %s'
                                    cur.execute(sql, (str(player[0]), str(game_log['gameId'])))
                                    if(cur.fetchone() is None):
                                        game_logs_db.append((
                                            game_log['gameId'] + player[0],
                                            game_log['gameId'],
                                            player[0],
                                            game_log['teamAbbrev'],
                                            game_log['homeRoadFlag'],
                                            game_log['gameDate'],
                                            game_log['goals'],
                                            game_log['assists'],
                                            game_log['points'],
                                            game_log['plusMinus'],
                                            game_log['powerPlayGoals'],
                                            game_log['powerPlayPoints'],
                                            game_log['gameWinningGoals'],
                                            game_log['otGoals'],
                                            game_log['shots'],
                                            game_log['shifts'],
                                            game_log['shorthandedGoals'],
                                            game_log['shorthandedPoints'],
                                            game_log['opponentAbbrev'],
                                            game_log['pim'],
                                            game_log['toi'],
                                            game_type
                                        ))
                            except(KeyError) as error:
                                yes = True
                                print('Cannot get game logs for player ' + player[0])
                                print(error)
                        
                        if(len(game_logs_db) > 0):
                            print('inserting ' + str(len(game_logs_db)) + ' gameLogs')
                            # insert query
                            cur.executemany(insert_sql, game_logs_db)
                            conn.commit()

                    print(str(index) + '/' + str(len(players)) + ' players')
                    index += 1
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()

def insertGames(start_season, end_season):
    client = NHLClient()
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                select_sql = 'SELECT abbrev from teams'
                cur.execute(select_sql)
                teams = cur.fetchall()
                index = 0
                for team in teams:
                    for i in range(start_season, end_season):
                        insert_sql = ''' INSERT INTO games(                                
                                            gameId,
                                            season,
                                            gameType,
                                            startTimeUTC,
                                            venue,
                                            awayTeamAbbrev,
                                            awayTeamScore,
                                            homeTeamAbbrev,
                                            homeTeamScore,
                                            gameOutcome,
                                            recapLink,
                                            gameCenterLink
                                        ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                                        RETURNING *
                                    '''
                        season = str(i) + str(i + 1)
                        games_db = []

                        try:
                            games = client.schedule.get_season_schedule(team[0], season)

                            if 'games' not in games:
                                print('No games for team ' + team[0] + ' for season ' + season)
                                continue
                            
                            for game in games['games']:

                                # Check if game already exists
                                sql = 'SELECT 1 FROM games WHERE gameId = %s'
                                cur.execute(sql, (str(game['id']),))
                                if(cur.fetchone() is None):
                                    games_db.append((
                                        game['id'],
                                        game['season'],
                                        game['gameType'],
                                        game['startTimeUTC'],
                                        game['venue']['default'],
                                        game['awayTeam']['abbrev'],
                                        game['awayTeam']['score'] if game['gameScheduleState'] != 'CNCL' else 0,
                                        game['homeTeam']['abbrev'],
                                        game['homeTeam']['score'] if game['gameScheduleState'] != 'CNCL' else 0,
                                        game['gameOutcome']['lastPeriodType'] if game['gameScheduleState'] != 'CNCL' else 'CANCELED',
                                        'https://nhl.com/' + game['threeMinRecap'] if 'threeMinRecap' in game else "",
                                        'https://nhl.com/' + game['gameCenterLink'] if 'gameCenterLink' in game else ""
                                    ))

                        except Exception as error:
                            yes = True
                            print('Cannot get games for team ' + team[0])
                            traceback.print_exc()
                        
                        if(len(games_db) > 0):
                            print('inserting ' + str(len(games_db)) + ' games')
                            # insert query
                            cur.executemany(insert_sql, games_db)
                            conn.commit()

                    print(str(index) + '/' + str(len(teams)) + ' teams')
                    index += 1
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()
        
def insertGoalerGameLogs(start_season, end_season):
    client = NHLClient()
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                select_sql = 'SELECT playerId from goalers'
                cur.execute(select_sql)
                goalers = cur.fetchall()
                index = 0
                for goaler in goalers:
                    for i in range(start_season, end_season):
                        insert_sql = ''' INSERT INTO goalerGamelogs(     
                                            gameLogsId,                           
                                            gameId,
                                            playerId,
                                            teamAbbrev,
                                            homeRoadFlag,
                                            gameDate,
                                            goals,
                                            assists,
                                            gamesStarted,
                                            decision,
                                            shotsAgainst,
                                            goalsAgainst,
                                            savePctg,
                                            shutouts,
                                            opponentAbbrev,
                                            pim,
                                            toi,
                                            gameType
                                        ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                                        RETURNING *
                                    '''
                        season = str(i) + str(i + 1)
                        game_logs_db = []

                        for game_type in range(1,4):
                            try:
                                game_logs = client.stats.player_game_log(player_id=goaler[0], season_id=season, game_type=game_type)
                                for game_log in game_logs:
                                    # Check if player already exists
                                    sql = 'SELECT 1 FROM goalerGamelogs WHERE playerId = %s AND gameId = %s'
                                    cur.execute(sql, (str(goaler[0]), str(game_log['gameId'])))
                                    if(cur.fetchone() is None):
                                        game_logs_db.append((
                                            game_log['gameId'] + goaler[0],
                                            game_log['gameId'],
                                            goaler[0],
                                            game_log['teamAbbrev'],
                                            game_log['homeRoadFlag'],
                                            game_log['gameDate'],
                                            game_log['goals'],
                                            game_log['assists'],
                                            game_log['gamesStarted'],
                                            game_log['decision'] if 'decision' in game_log else '',
                                            game_log['shotsAgainst'],
                                            game_log['goalsAgainst'],
                                            game_log['savePctg'] if 'savePctg' in game_log else 0,
                                            game_log['shutouts'],
                                            game_log['opponentAbbrev'],
                                            game_log['pim'],
                                            game_log['toi'],
                                            game_type
                                        ))
                            except(KeyError) as error:
                                yes = True
                                print('Cannot get game logs for goaler ' + goaler[0])
                                traceback.print_exc()
                        
                        if(len(game_logs_db) > 0):
                            print('inserting ' + str(len(game_logs_db)) + ' gameLogs')
                            # insert query
                            cur.executemany(insert_sql, game_logs_db)
                            conn.commit()

                    print(str(index) + '/' + str(len(goalers)) + ' players')
                    index += 1
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()
   
def insertPlayersHeadhots():
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                sql = 'SELECT playerId, teamAbbrev FROM players WHERE gamesPlayed > 0'
                cur.execute(sql)
                players = cur.fetchall()
                for player in players:
                    updatesql = 'UPDATE players set headshot = \'https://assets.nhle.com/mugs/nhl/20232024/' + player[1] + '/' + player[0] + '.png\''+ ' WHERE playerId = \'' + player[0] + '\''
                    cur.execute(updatesql)
                conn.commit()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def insertteamsLogo():
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                sql = 'SELECT abbrev FROM teams'
                cur.execute(sql)
                teams = cur.fetchall()
                for team in teams:
                    updatesql = 'UPDATE teams set logo = \'https://assets.nhle.com/logos/nhl/svg/' + team[0] + '_light.svg\'' + ' WHERE abbrev = \'' + team[0] + '\''
                    cur.execute(updatesql)
                conn.commit()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def updatePlayersAdditionalInfos():
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                sql = 'SELECT playerId FROM players where gamesPlayed > 0'
                cur.execute(sql)
                players = cur.fetchall()
                today = datetime.today()
                i = 1
                for player in players:
                    print(str(i) + '/' + str(len(players)))
                    i += 1                    
                    url = 'https://api-web.nhle.com/v1/player/' + player[0] + '/landing'
                    response = requests.get(url)

                    if response.status_code == 200:
                        result = response.json()
                        birthDate = datetime.strptime(result['birthDate'], "%Y-%m-%d")
                        age = today.year - birthDate.year - ((today.month, today.day) < (birthDate.month, birthDate.day))
                        height = result['heightInInches']
                        weight = result['weightInPounds']
                        birthCountry = result['birthCountry']
                        updatesql = '''
                            UPDATE players
                            SET age = %s,
                                height = %s,
                                weight = %s,
                                birthCountry = %s
                            WHERE playerId = %s
                        '''
                        cur.execute(updatesql, (str(age), str(height), str(weight), birthCountry, str(player[0])))
                    else:
                        print("Failed to retrieve data. Status code:", response.status_code)
                conn.commit()
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()

if __name__ == '__main__':
    #generate_db()
    #insertTeams()
    #insertPlayers(datetime.now().year - 10, datetime.now().year)
    #insertGoalers(datetime.now().year - 10, datetime.now().year)
    #insertGames(datetime.now().year - 2, datetime.now().year)
    #insertGameLogs(datetime.now().year - 2, datetime.now().year)
    #insertGoalerGameLogs(datetime.now().year - 2, datetime.now().year)
    #insertPlayersHeadhots()
    #insertteamsLogo()
    updatePlayersAdditionalInfos()