import os
import psycopg2
import traceback
from nhlpy import NHLClient
from datetime import datetime, timedelta
client = NHLClient()

class Game:
    def __init__(
        self,
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
        gameCenterLink,
    ):
        self.gameId = gameId
        self.season = season
        self.gameType = gameType
        self.startTimeUTC = startTimeUTC
        self.venue = venue
        self.awayTeamAbbrev = awayTeamAbbrev
        self.awayTeamScore = awayTeamScore
        self.homeTeamAbbrev = homeTeamAbbrev
        self.homeTeamScore = homeTeamScore
        self.gameOutcome = gameOutcome
        self.recapLink = recapLink
        self.gameCenterLink = gameCenterLink

    def values(self):
        return (
            self.gameId,
            self.season,
            self.gameType,
            self.startTimeUTC,
            self.venue,
            self.awayTeamAbbrev,
            self.awayTeamScore,
            self.homeTeamAbbrev,
            self.homeTeamScore,
            self.gameOutcome,
            self.recapLink,
            self.gameCenterLink,
        )


def save_games_to_db(games):
    try:
        with psycopg2.connect(
            database=os.environ.get("DATABASE_URL", "nhl-stats-fetcher"),
            user=os.environ.get("DATABASE_USERNAME", "py-user"),
            password=os.environ.get("DATABASE_PASSWORD"),
            host=os.environ.get("DATABASE_URL", "127.0.0.1"),
            port=os.environ.get("DATABASE_PORT", "5433"),
        ) as conn:
            with conn.cursor() as cur:
                insertGames_sql = """ INSERT INTO games values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                        ON CONFLICT (gameId) DO UPDATE
                            SET season = excluded.season,
                                gameType = excluded.gameType,
                                startTimeUTC = excluded.startTimeUTC,
                                venue = excluded.venue,
                                awayTeamAbbrev = excluded.awayTeamAbbrev,
                                awayTeamScore = excluded.awayTeamScore,
                                homeTeamAbbrev = excluded.homeTeamAbbrev,
                                homeTeamScore = excluded.homeTeamScore,
                                gameOutcome = excluded.gameOutcome,
                                recapLink = excluded.recapLink,
                                gameCenterLink = excluded.gameCenterLink
                        RETURNING *
                    """
                print('Saving ' + str(len(games)) + ' games')
                cur.executemany(insertGames_sql, [game.values() for game in games])
                print('Done.')
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()


def get_season_games():
    # 1. Get current season start and end date
    todaySchedule = client.schedule.get_schedule()
    startDate = datetime.strptime(todaySchedule["preSeasonStartDate"], "%Y-%m-%d")
    endDate = datetime.strptime(todaySchedule["playoffEndDate"], "%Y-%m-%d")

    games = []
    # 2. Loop every week for this season
    print('Getting games from ' + str(startDate) + ' to ' + str(endDate))
    currentDate = startDate
    while currentDate <= endDate:
        games += get_week_games(currentDate)
        currentDate += timedelta(days=7)
    
    print ('Got ' + str(len(games)) + ' games')
    return games

def get_week_games(date: datetime):
    games = []
    gameWeek = client.schedule.get_schedule(date.strftime("%Y-%m-%d"))
    for gameDay in gameWeek["gameWeek"]:
        for game in gameDay["games"]:
            games.append(Game(
                game['id'],
                game['season'],
                game['gameType'],
                game['startTimeUTC'],
                game['venue']['default'],
                game['awayTeam']['abbrev'],
                game['awayTeam']['score'] if game['gameState'] != 'FUT' else None,
                game['homeTeam']['abbrev'],
                game['homeTeam']['score'] if game['gameState'] != 'FUT' else None,
                game['gameOutcome']['lastPeriodType'] if game['gameState'] != 'FUT' else 'FUT',
                'https://nhl.com/' + game['threeMinRecap'] if 'threeMinRecap' in game else "",
                'https://nhl.com/' + game['gameCenterLink'] if 'gameCenterLink' in game else ""
            ))
    return games

def fetch_strength_points(playerId, gameGoals, strength, goals = True, assists = True):
    ppp = 0
    for period in gameGoals:
        for goal in period['goals']:
            if goal['strength'] == strength or strength == 'all':
                if goals and goal['playerId'] == playerId:
                    ppp += 1
                if assists:
                    for assist in goal['assists']:
                        if assist['playerId'] == playerId:
                            ppp += 1
    return ppp

def fetch_ot_goals(playerId, gameGoals):
    otGoals = 0
    for period in gameGoals:
        if period['periodDescriptor']['periodType'] == 'OT':
            for goal in period['goals']:
                if goal['playerId'] == playerId:
                    otGoals += 1
    return otGoals