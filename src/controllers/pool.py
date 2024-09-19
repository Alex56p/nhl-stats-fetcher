import os
import psycopg2
import traceback

class Pool:
    def __init__(
        self,
        poolId: str,
        leagueId: int,
        season: str,
        name: str,
        forwards: int,
        defense: int,
        goalie: int,
        bench: int,
        goalieGamePlayed: int,
        forwardGamePlayed: int,
        defenseGamePlayed: int,
        scoringGoals: float,
        scoringAssists: float,
        scoringPPP: float,
        scoringSHP: float,
        scoringSOG: float,
        scoringHits: float,
        scoringGoalieWins: float,
        scoringGoalieLosses: float,
        scoringGoalieSaves: float,
        scoringGoalieShutouts: float,
        scoringGoalieOTL: float
    ):
        self.poolId = poolId
        self.leagueId = leagueId
        self.season = season
        self.name = name
        self.forwards = forwards
        self.defense = defense
        self.goalie = goalie
        self.bench = bench
        self.goalieGamePlayed = goalieGamePlayed
        self.forwardGamePlayed = forwardGamePlayed
        self.defenseGamePlayed = defenseGamePlayed
        self.scoringGoals = scoringGoals
        self.scoringAssists = scoringAssists
        self.scoringPPP = scoringPPP
        self.scoringSHP = scoringSHP
        self.scoringSOG = scoringSOG
        self.scoringHits = scoringHits
        self.scoringGoalieWins = scoringGoalieWins
        self.scoringGoalieLosses = scoringGoalieLosses
        self.scoringGoalieSaves = scoringGoalieSaves
        self.scoringGoalieShutouts = scoringGoalieShutouts
        self.scoringGoalieOTL = scoringGoalieOTL
        
    def values(self):
        return (
            self.poolId,
            self.leagueId,
            self.season,
            self.name,
            self.forwards,
            self.defense,
            self.goalie,
            self.bench,
            self.goalieGamePlayed,
            self.forwardGamePlayed,
            self.defenseGamePlayed,
            self.scoringGoals,
            self.scoringAssists,
            self.scoringPPP,
            self.scoringSHP,
            self.scoringSOG,
            self.scoringHits,
            self.scoringGoalieWins,
            self.scoringGoalieLosses,
            self.scoringGoalieSaves,
            self.scoringGoalieShutouts,
            self.scoringGoalieOTL,
        )
        
def createPoolTable():
    insertSql = '''
        CREATE TABLE pools (
            poolId VARCHAR(100) PRIMARY KEY,
            leagueId INTEGER,
            season VARCHAR(100),
            name VARCHAR(100),
            forwards INTEGER,
            defense INTEGER,
            goalie INTEGER,
            bench INTEGER,
            goalieGamePlayed INTEGER,
            forwardGamePlayed INTEGER,
            defenseGamePlayed INTEGER,
            scoringGoals NUMERIC,
            scoringAssists NUMERIC,
            scoringPPP NUMERIC,
            scoringSHP NUMERIC,
            scoringSOG NUMERIC,
            scoringHits NUMERIC,
            scoringGoalieWins NUMERIC,
            scoringGoalieLosses NUMERIC,
            scoringGoalieSaves NUMERIC,
            scoringGoalieShutouts NUMERIC,
            scoringGoalieOTL NUMERIC
        )
        '''
    
    try:
        with psycopg2.connect(database=os.environ.get("DATABASE_URL", 'nhl-stats-fetcher'), 
                              user=os.environ.get('DATABASE_USERNAME', 'py-user'), 
                              password=os.environ.get('DATABASE_PASSWORD'), 
                              host=os.environ.get('DATABASE_URL', '127.0.0.1'), 
                              port= os.environ.get('DATABASE_PORT', '5433')) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                result = cur.execute(insertSql)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        
def insertPools(pools: []):
    insert_sql = """
                        INSERT INTO pools values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (poolId) DO UPDATE
                            SET leagueId = excluded.leagueId,
                                season = excluded.season,
                                name = excluded.name,
                                forwards = excluded.forwards,
                                defense = excluded.defense,
                                goalie = excluded.goalie,
                                bench = excluded.bench,
                                goalieGamePlayed = excluded.goalieGamePlayed,
                                forwardGamePlayed = excluded.forwardGamePlayed,
                                defenseGamePlayed = excluded.defenseGamePlayed,
                                scoringGoals = excluded.scoringGoals,
                                scoringAssists = excluded.scoringAssists,
                                scoringPPP = excluded.scoringPPP,
                                scoringSHP = excluded.scoringSHP,
                                scoringSOG = excluded.scoringSOG,
                                scoringHits = excluded.scoringHits,
                                scoringGoalieWins = excluded.scoringGoalieWins,
                                scoringGoalieLosses = excluded.scoringGoalieLosses,
                                scoringGoalieSaves = excluded.scoringGoalieSaves,
                                scoringGoalieShutouts = excluded.scoringGoalieShutouts,
                                scoringGoalieOTL = excluded.scoringGoalieOTL
                    """
    try:
        with psycopg2.connect(
            database=os.environ.get("DATABASE_URL", "nhl-stats-fetcher"),
            user=os.environ.get("DATABASE_USERNAME", "py-user"),
            password=os.environ.get("DATABASE_PASSWORD"),
            host=os.environ.get("DATABASE_URL", "127.0.0.1"),
            port=os.environ.get("DATABASE_PORT", "5433"),
        ) as conn:
            with conn.cursor() as cur:
                print('Inserting ' + str(len(pools)) + ' pools.')
                cur.executemany(insert_sql, [pool.values() for pool in pools])
                print('Done.')
    except (psycopg2.DatabaseError, Exception) as error:
        traceback.print_exc()