import os
import psycopg2
import traceback
from nhlpy import NHLClient
from datetime import datetime
import requests
from controllers.game import Game, get_season_games, fetch_strength_points
from controllers.season import get_season_from_date

client = NHLClient()
playerIds = []


class Goaler:
    def __init__(
        self,
        playerId,
        fullName=None,
        assists=None,
        gamesPlayed=None,
        gamesStarted=None,
        goals=None,
        goalsAgainst=None,
        goalsAgainstAverage=None,
        losses=None,
        otLosses=None,
        penaltyMinutes=None,
        points=None,
        savePct=None,
        saves=None,
        shootsCatches=None,
        shotsAgainst=None,
        shutouts=None,
        teamAbbrev=None,
        ties=None,
        timeOnIce=None,
        wins=None,        
        headshot = None,
        age = None,
        height = None,
        weight = None,
        birthCountry = None,
    ):
        self.playerId = playerId
        self.fullName = fullName
        self.assists = assists
        self.gamesPlayed = gamesPlayed
        self.gamesStarted = gamesStarted
        self.goals = goals
        self.goalsAgainst = goalsAgainst
        self.goalsAgainstAverage = goalsAgainstAverage
        self.losses = losses
        self.otLosses = otLosses
        self.penaltyMinutes = penaltyMinutes
        self.points = points
        self.savePct = savePct
        self.saves = saves
        self.shootsCatches = shootsCatches
        self.shotsAgainst = shotsAgainst
        self.shutouts = shutouts
        self.teamAbbrev = teamAbbrev
        self.ties = ties
        self.timeOnIce = timeOnIce
        self.wins = wins
        self.headshot = headshot
        self.age = age
        self.height = height
        self.weight = weight
        self.birthCountry = birthCountry


    def values(self):
        return (
            self.playerId,
            self.fullName,
            self.assists,
            self.gamesPlayed,
            self.gamesStarted,
            self.goals,
            self.goalsAgainst,
            self.goalsAgainstAverage,
            self.losses,
            self.otLosses,
            self.penaltyMinutes,
            self.points,
            self.savePct,
            self.saves,
            self.shootsCatches,
            self.shotsAgainst,
            self.shutouts,
            self.teamAbbrev,
            self.ties,
            self.timeOnIce,
            self.wins,
            self.headshot,
            self.age,
            self.height,
            self.weight,
            self.birthCountry,
        )

    def update_infos(self, date: datetime, skater_stats=None):
        season = get_season_from_date(date)

        if skater_stats is None:
            skater_stats = client.stats.goalie_stats_summary_simple(
                start_season=season,
                end_season=season,
                default_cayenne_exp="playerId=" + str(self.playerId),
            )
            if len(skater_stats) > 0:
                skater_stats = skater_stats[0]
            else:
                print('Invalid goaler ' + str(self.playerId))

        self.playerId = skater_stats["playerId"]
        self.fullName = skater_stats["goalieFullName"]
        self.assists = skater_stats["assists"]
        self.gamesPlayed = skater_stats["gamesPlayed"]
        self.gamesStarted = skater_stats["gamesStarted"]
        self.goals = skater_stats["goals"]
        self.goalsAgainst = skater_stats["goalsAgainst"]
        self.goalsAgainstAverage = skater_stats["goalsAgainstAverage"]
        self.losses = skater_stats["losses"]
        self.otLosses = skater_stats["otLosses"]
        self.penaltyMinutes = skater_stats["penaltyMinutes"]
        self.points = skater_stats["points"]
        self.savePct = skater_stats["savePct"]
        self.saves = skater_stats["saves"]
        self.shootsCatches = skater_stats["shootsCatches"]
        self.shotsAgainst = skater_stats["shotsAgainst"]
        self.shutouts = skater_stats["shutouts"]
        self.teamAbbrev = skater_stats["teamAbbrevs"][
            len(skater_stats["teamAbbrevs"]) - 3 :
        ]  # keep the last 3 for most recent team
        self.ties = skater_stats["ties"]
        self.timeOnIce = skater_stats["timeOnIce"]
        self.wins = skater_stats["wins"]
        self.headshot = 'https://assets.nhle.com/mugs/nhl/' + str(season) + '/' + str(self.playerId) + '/' + str(self.teamAbbrev) + '.png'

        url = "https://api-web.nhle.com/v1/player/" + str(self.playerId) + "/landing"
        response = requests.get(url)

        if response.status_code == 200:
            result = response.json()
            birthDate = datetime.strptime(result["birthDate"], "%Y-%m-%d")
            today = datetime.now()
            self.age = (today.year - birthDate.year - ((today.month, today.day) < (birthDate.month, birthDate.day)))
            self.height = result["heightInInches"]
            self.weight = result["weightInPounds"]
            self.birthCountry = result["birthCountry"]

        return self


def save_goalers_to_db(goalers, updateOnConflict=True):
    if len(goalers) == 0:
        return
    
    try:
        with psycopg2.connect(
            database=os.environ.get("DATABASE_NAME", "nhl-stats-fetcher"),
            user=os.environ.get("DATABASE_USERNAME", "py-user"),
            password=os.environ.get("DATABASE_PASSWORD"),
            host=os.environ.get("DATABASE_URL", "127.0.0.1"),
            port=os.environ.get("DATABASE_PORT", "5433"),
        ) as conn:
            with conn.cursor() as cur:
                if updateOnConflict:

                    insert_sql = """
                        INSERT INTO goalers values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                        ON CONFLICT (playerId) DO UPDATE
                            SET fullName = excluded.fullName,
                                assists = excluded.assists,
                                gamesPlayed = excluded.gamesPlayed,
                                gamesStarted = excluded.gamesStarted,
                                goals = excluded.goals,
                                goalsAgainst = excluded.goalsAgainst,
                                goalsAgainstAverage = excluded.goalsAgainstAverage,
                                losses = excluded.losses,
                                otLosses = excluded.otLosses,
                                penaltyMinutes = excluded.penaltyMinutes,
                                points = excluded.points,
                                savePct = excluded.savePct,
                                saves = excluded.saves,
                                shootsCatches = excluded.shootsCatches,
                                shotsAgainst = excluded.shotsAgainst,
                                shutouts = excluded.shutouts,
                                teamAbbrev = excluded.teamAbbrev,
                                ties = excluded.ties,
                                timeOnIce = excluded.timeOnIce,
                                wins = excluded.wins,
                                headshot = excluded.headshot,
                                age = excluded.age,
                                height = excluded.height,
                                weight = excluded.weight,
                                birthCountry = excluded.birthCountry
                        RETURNING *
                    """
                else:
                    insert_sql = """
                        INSERT INTO goalers values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                        ON CONFLICT (playerId) DO NOTHING
                        RETURNING *
                    """
                print('Inserting ' + str(len(goalers)) + ' goalers')
                cur.executemany(insert_sql, [goaler.values() for goaler in goalers])
                print('Done.')
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()


def save_goalers_if_not_exists(goalersIds, gameDate: datetime):
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
                    SELECT playerId from goalers where playerId in %s
                """
                strGoalersIds = [str(playerId) for playerId in goalersIds]
                cur.execute(select_sql, (tuple(strGoalersIds),))
                saved_goalers = cur.fetchall()
                saved_goalers = [goaler[0] for goaler in saved_goalers]
                goalers_to_insert = [
                    goaler for goaler in strGoalersIds if goaler not in saved_goalers
                ]

                if len(goalers_to_insert) > 0:
                    save_goalers_to_db([ Goaler(playerId=goaler).update_infos(gameDate) for goaler in goalers_to_insert ])

    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()

class GoalerGameLog:
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
        gamesStarted,
        decision,
        shotsAgainst,
        goalsAgainst,
        savePctg,
        shutouts,
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
        self.gamesStarted = gamesStarted
        self.decision = decision
        self.shotsAgainst = shotsAgainst
        self.goalsAgainst = goalsAgainst
        self.savePctg = savePctg
        self.shutouts = shutouts
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
            self.gamesStarted,
            self.decision,
            self.shotsAgainst,
            self.goalsAgainst,
            self.savePctg,
            self.shutouts,
            self.opponentAbbrev,
            self.pim,
            self.toi,
            self.gameType,
        )


def save_goaler_gamelogs_to_db(goalergamelogs):
    try:
        with psycopg2.connect(
            database=os.environ.get("DATABASE_URL", "nhl-stats-fetcher"),
            user=os.environ.get("DATABASE_USERNAME", "py-user"),
            password=os.environ.get("DATABASE_PASSWORD"),
            host=os.environ.get("DATABASE_URL", "127.0.0.1"),
            port=os.environ.get("DATABASE_PORT", "5433"),
        ) as conn:
            with conn.cursor() as cur:
                gameLogsSql = """ INSERT INTO goalerGameLogs values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                                    ON CONFLICT (gameLogsId) DO UPDATE
                                        SET gameId = excluded.gameId,
                                            playerId = excluded.playerId,
                                            teamAbbrev = excluded.teamAbbrev,
                                            homeRoadFlag = excluded.homeRoadFlag,
                                            gameDate = excluded.gameDate,
                                            goals = excluded.goals,
                                            assists = excluded.assists,
                                            gamesStarted = excluded.gamesStarted,
                                            decision = excluded.decision,
                                            shotsAgainst = excluded.shotsAgainst,
                                            goalsAgainst = excluded.goalsAgainst,
                                            savePctg = excluded.savePctg,
                                            shutouts = excluded.shutouts,
                                            opponentAbbrev = excluded.opponentAbbrev,
                                            pim = excluded.pim,
                                            toi = excluded.toi,
                                            gameType = excluded.gameType
                                """
                print('Saving ' + str(len(goalergamelogs)) + ' goaler gamelogs')
                cur.executemany(gameLogsSql, [goalergamelog.values() for goalergamelog in goalergamelogs])
                print('Done.')
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()

def fetch_goaler_log(goalersBoxScore, game: Game, gameGoals, away: bool):
    gamelogs = []
    playerIds = []
    for goalerBoxScore in goalersBoxScore:
        
        # Don't add goalers that have not played.
        if goalerBoxScore['toi'] == "00:00":
            continue
        
        playerIds.append(goalerBoxScore["playerId"])
        gamelogs.append(
            GoalerGameLog(
                str(game.gameId) + str(goalerBoxScore['playerId']),
                game.gameId,
                goalerBoxScore['playerId'],
                game.awayTeamAbbrev if away else game.homeTeamAbbrev,
                'R' if away else 'H',
                game.startTimeUTC,
                fetch_strength_points(goalerBoxScore['playerId'], gameGoals, 'all', True, False),
                fetch_strength_points(goalerBoxScore['playerId'], gameGoals, 'all', False, True), 
                1 if 'starter' in goalerBoxScore and goalerBoxScore['starter'] == 'true' else 0,
                goalerBoxScore['decision'] if 'decision' in goalerBoxScore else '',
                int(goalerBoxScore['saveShotsAgainst'].split('/')[1]),
                goalerBoxScore['goalsAgainst'],
                goalerBoxScore['savePctg'] if 'savePctg' in goalerBoxScore else 0,
                1 if goalerBoxScore['goalsAgainst'] == 0 and 'decision' in goalerBoxScore and goalerBoxScore['decision'] == 'W' else 0,
                game.homeTeamAbbrev if away else game.awayTeamAbbrev,
                goalerBoxScore['pim'] if 'pim' in goalerBoxScore else None,
                goalerBoxScore['toi'],
                game.gameType
            )
        )

    save_goalers_if_not_exists(playerIds, datetime.strptime(game.startTimeUTC, '%Y-%m-%dT%H:%M:%SZ'))
    return gamelogs
