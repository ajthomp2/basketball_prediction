import pymysql as sql
import rds_config

# ******************************************************** #
# ******************************************************** #
# NOTES:
#   -
# ******************************************************** #
# ******************************************************** #


def open_db():
    sql.install_as_MySQLdb()
    rds_host = rds_config.db_endpoint
    name = rds_config.db_username
    password = rds_config.db_password
    db_name = rds_config.db_name

    conn = sql.connect(rds_host, user=name,passwd=password, db=db_name, connect_timeout=5)
    return conn


# GAME STATS TABLES

# player_stats_cols = ['GameID', 'PlayerID', 'TeamID', 'MP', 'FG',
#                      'FGA', 'FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB',
#                      'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', '+/-', 'TS%', 'eFG%', '3PAr', 'FTr',
#                      'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%', 'ORtg', 'DRtg']


def create_player_game_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            GameID      INTEGER     NOT NULL,
            PlayerID    INTEGER     NOT NULL,
            PlayerLink  VARCHAR(255)    NOT NULL,
            TeamID      INTEGER     NOT NULL,
            MP          VARCHAR(10)     NOT NULL,
            FG          INTEGER     NOT NULL,
            FGA         INTEGER     NOT NULL,
            FGPercent         FLOAT    NOT NULL,
            3P          INTEGER     NOT NULL,
            3PA         INTEGER     NOT NULL,
            3PPercent         FLOAT    NOT NULL,
            FT          INTEGER     NOT NULL,
            FTA         INTEGER     NOT NULL,
            FTPercent         FLOAT    NOT NULL,
            ORB         INTEGER     NOT NULL,
            DRB         INTEGER     NOT NULL,
            TRB         INTEGER     NOT NULL,
            AST         INTEGER     NOT NULL,
            STL         INTEGER     NOT NULL,
            BLK         INTEGER     NOT NULL,
            TOV         INTEGER     NOT NULL,
            PF          INTEGER     NOT NULL,
            PTS         INTEGER     NOT NULL,
            PlusMinus         FLOAT,
            TSPercent         FLOAT    NOT NULL,
            eFGPercent        FLOAT    NOT NULL,
            3PAr        FLOAT    NOT NULL,
            FTr         FLOAT    NOT NULL,
            ORBPercent        FLOAT    NOT NULL,
            DRBPercent        FLOAT    NOT NULL,
            TRBPercent        FLOAT    NOT NULL,
            ASTPercent        FLOAT    NOT NULL,
            STLPercent        FLOAT    NOT NULL,
            BLKPercent        FLOAT    NOT NULL,
            TOVPercent        FLOAT    NOT NULL,
            USGPercent        FLOAT    NOT NULL,
            ORtg        FLOAT    NOT NULL,
            DRtg        FLOAT    NOT NULL,
            PRIMARY KEY(GameID, PlayerID, PlayerLink)
        );
        '''.format(tablename))


# team_stats_cols = ['GameID', 'Season', 'Date', 'GameTypeID',
#                    'TeamID', 'Q1Score', 'Q2Score', 'Q3Score', 'Q4Score',
#                    'OTScore', 'FinalScore', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
#                    'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV',
#                    'PF', 'TS%', 'eFG%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%',
#                    'STL%', 'BLK%', 'TOV%', 'ORtg', 'DRtg']


def create_team_game_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            GameID      INTEGER     NOT NULL,
            Season      INTEGER     NOT NULL,
            Date        TIMESTAMP    NOT NULL,
            GameTypeID  INTEGER     NOT NULL,
            Home        BIT         NOT NULL,
            Team        VARCHAR(3)    NOT NULL,
            TeamID      INTEGER     NOT NULL,
            Q1Score     INTEGER     NOT NULL,
            Q2Score     INTEGER     NOT NULL,
            Q3Score     INTEGER     NOT NULL,
            Q4Score     INTEGER     NOT NULL,
            OTScore     INTEGER,
            FinalScore  INTEGER     NOT NULL,
            FG          INTEGER     NOT NULL,
            FGA         INTEGER     NOT NULL,
            FGPercent         FLOAT    NOT NULL,
            3P          INTEGER     NOT NULL,
            3PA         INTEGER     NOT NULL,
            3PPercent         FLOAT    NOT NULL,
            FT          INTEGER     NOT NULL,
            FTA         INTEGER     NOT NULL,
            FTPercent         FLOAT    NOT NULL,
            ORB         INTEGER     NOT NULL,
            DRB         INTEGER     NOT NULL,
            TRB         INTEGER     NOT NULL,
            AST         INTEGER     NOT NULL,
            STL         INTEGER     NOT NULL,
            BLK         INTEGER     NOT NULL,
            TOV         INTEGER     NOT NULL,
            PF          INTEGER     NOT NULL,
            PACE        FLOAT       NOT NULL,
            TSPercent         FLOAT    NOT NULL,
            eFGPercent        FLOAT    NOT NULL,
            3PAr        FLOAT    NOT NULL,
            FTr         FLOAT    NOT NULL,
            ORBPercent        FLOAT    NOT NULL,
            DRBPercent        FLOAT    NOT NULL,
            TRBPercent        FLOAT    NOT NULL,
            ASTPercent        FLOAT    NOT NULL,
            STLPercent        FLOAT    NOT NULL,
            BLKPercent        FLOAT    NOT NULL,
            TOVPercent        FLOAT    NOT NULL,
            ORtg        FLOAT    NOT NULL,
            DRtg        FLOAT    NOT NULL,
            PRIMARY KEY(GameID, Team)
        );
        '''.format(tablename))

# TEAM STATS TABLES

# team_gen_info_cols = ['TeamID', 'Season', 'Team', 'Games', 'Wins', 'Losses', 'Division', 'DivisionID',
#                       'DivisionRank', 'Conference', 'ConferenceID', 'ConferenceRank',
#                       'HC1', 'HC1Ws', 'HC1Ls', 'HC2', 'HC2Ws', 'HC2Ls', 'Round1Wins', 'Round1Losses',
#                       'Round1Opp', 'Round1Won', 'Round2Wins', 'Round2Losses', 'Round2Opp', 'Round2Won',
#                       'Round3Wins', 'Round3Losses', 'Round3Opp', 'Round3Won', 'Round4Wins', 'Round4Losses',
#                       'Round4Opp', 'Round4Won']


def create_general_team_info_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            Team          VARCHAR(3)    NOT NULL,
            TeamID        INTEGER     NOT NULL,
            Season        INTEGER     NOT NULL,
            Games         INTEGER     NOT NULL,
            Wins          INTEGER     NOT NULL,
            Losses        INTEGER     NOT NULL,
            Division      VARCHAR(255),
            DivisionID    INTEGER,
            DivisionRank  INTEGER,
            Conference    VARCHAR(255)    NOT NULL,
            ConferenceID  INTEGER     NOT NULL,
            ConferenceRank INTEGER,
            HeadCoach1    VARCHAR(255)    NOT NULL,
            HeadCoach1Wins INTEGER    NOT NULL,
            HeadCoach1Losses INTEGER  NOT NULL,
            HeadCoach2    VARCHAR(255),
            HeadCoach2Wins INTEGER,
            HeadCoach2Losses INTEGER,
            Round1Wins    INTEGER,
            Round1Losses  INTEGER,
            Round1Opp     VARCHAR(255),
            Round1Won     INTEGER,
            Round2Wins    INTEGER,
            Round2Losses  INTEGER,
            Round2Opp     VARCHAR(255),
            Round2Won     INTEGER,
            Round3Wins    INTEGER,
            Round3Losses  INTEGER,
            Round3Opp     VARCHAR(255),
            Round3Won     INTEGER,
            Round4Wins    INTEGER,
            Round4Losses  INTEGER,
            Round4Opp     VARCHAR(255),
            Round4Won     INTEGER,
            PRIMARY KEY(TeamID, Season)
        );
        '''.format(tablename))


# team_stats_cols = ['TeamID', 'Season', 'FG/G', 'FGA/G', 'FG%', '3P/G', '3PA/G', '3P%', '2P/G',
#                    '2PA/G', '2P%', 'FT/G', 'FTA/G', 'FT%', 'ORB/G', 'DRB/G', 'TRB/G', 'AST/G', 'STL/G', 'BLK/G',
#                    'TOV/G', 'PF/G', 'PTS/G', 'FGA/GYOY%Change', 'FGA/GYOY%Change', 'FGA/GYOY%Change',
#                    '3P/GYOY%Change', '3PA/GYOY%Change', '3P%YOY%Change',
#                    '2P/GYOY%Change', '2PA/GYOY%Change', '2P%YOY%Change', 'FT/GYOY%Change', 'FTA/GYOY%Change',
#                    'FT%YOY%Change', 'ORB/GYOY%Change', 'DRB/GYOY%Change',
#                    'TRB/GYOY%Change', 'AST/GYOY%Change', 'STL/GYOY%Change', 'BLK/GYOY%Change', 'TOV/GYOY%Change',
#                    'PF/GYOY%Change', 'PTS/GYOY%Change']


def create_team_season_stats_table(c, table_name):
    c.execute('''
        CREATE TABLE {}
        (
            TeamID      INTEGER     NOT NULL,
            Season      INTEGER     NOT NULL,
            FGPerG        FLOAT    NOT NULL,
            FGAPerG       FLOAT    NOT NULL,
            FGPercent         FLOAT    NOT NULL,
            3PPerG        FLOAT    NOT NULL,
            3PAPerG       FLOAT    NOT NULL,
            3PPercent         FLOAT    NOT NULL,
            2PPerG        FLOAT    NOT NULL,
            2PAPerG       FLOAT    NOT NULL,
            2PPercent         FLOAT    NOT NULL,
            FTPerG        FLOAT    NOT NULL,
            FTAPerG       FLOAT    NOT NULL,
            FTPercent         FLOAT    NOT NULL,
            ORBPerG       FLOAT    NOT NULL,
            DRBPerG       FLOAT    NOT NULL,
            TRBPerG       FLOAT    NOT NULL,
            ASTPerG       FLOAT    NOT NULL,
            STLPerG       FLOAT    NOT NULL,
            BLKPerG       FLOAT    NOT NULL,
            TOVPerG       FLOAT    NOT NULL,
            PFPerG        FLOAT    NOT NULL,
            PTSPerG       FLOAT    NOT NULL,
            FGPerGYOYPercentChange FLOAT NOT NULL,
            FGAPerGYOYPercentChange FLOAT NOT NULL,
            FGPercentYOYPercentChange FLOAT    NOT NULL,
            3PPerGYOYPercentChange FLOAT    NOT NULL,
            3PAPerGYOYPercentChange FLOAT    NOT NULL,
            3PPercentYOYPercentChange FLOAT    NOT NULL,
            2PPerGYOYPercentChange FLOAT    NOT NULL,
            2PAPerGYOYPercentChange FLOAT    NOT NULL,
            2PPercentYOYPercentChange FLOAT    NOT NULL,
            FTPerGYOYPercentChange FLOAT    NOT NULL,
            FTAPerGYOYPercentChange FLOAT    NOT NULL,
            FTPercentYOYPercentChange FLOAT    NOT NULL,
            ORBPerGYOYPercentChange FLOAT    NOT NULL,
            DRBPerGYOYPercentChange FLOAT    NOT NULL,
            TRBPerGYOYPercentChange FLOAT    NOT NULL,
            ASTPerGYOYPercentChange FLOAT    NOT NULL,
            STLPerGYOYPercentChange FLOAT    NOT NULL,
            BLKPerGYOYPercentChange FLOAT    NOT NULL,
            TOVPerGYOYPercentChange FLOAT    NOT NULL,
            PFPerGYOYPercentChange FLOAT    NOT NULL,
            PTSPerGYOYPercentChange FLOAT    NOT NULL,
            PRIMARY KEY(TeamID, Season)
        );
        '''.format(table_name))


def create_team_season_lineup_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            TeamID          INTEGER     NOT NULL,
            Season          INTEGER     NOT NULL,
            GameTypeID      INTEGER     NOT NULL,
            NumPlayers      INTEGER     NOT NULL,
            Player1ID       INTEGER     NOT NULL,
            Player2ID       INTEGER     NOT NULL,
            Player3ID       INTEGER,
            Player4ID       INTEGER,
            Player5ID       INTEGER,
            MP              INTEGER     NOT NULL,
            PlusMinusFGPer100Poss   FLOAT    NOT NULL,
            PlusMinusFGAPer100Poss  FLOAT    NOT NULL,
            PlusMinusFGPercentPer100Poss  FLOAT    NOT NULL,
            PlusMinus3PPer100Poss   FLOAT    NOT NULL,
            PlusMinus3PAPer100Poss  FLOAT    NOT NULL,
            PlusMinus3PPercentPer100Poss  FLOAT    NOT NULL,
            PlusMinuseFGPercentPer100Poss FLOAT    NOT NULL,
            PlusMinusFTPer100Poss   FLOAT    NOT NULL,
            PlusMinusFTAPer100Poss  FLOAT    NOT NULL,
            PlusMinusFTPercentPer100Poss  FLOAT    NOT NULL,
            PlusMinusPTSPer100Poss  FLOAT    NOT NULL,
            PlusMinusORBPer100Poss  FLOAT    NOT NULL,
            PlusMinusORBPercentPer100Poss FLOAT    NOT NULL,
            PlusMinusDRBPer100Poss  FLOAT    NOT NULL,
            PlusMinusDRBPercentPer100Poss FLOAT    NOT NULL,
            PlusMinusTRBPer100Poss  FLOAT    NOT NULL,
            PlusMinusTRBPercentPer100Poss FLOAT    NOT NULL,
            PlusMinusASTPer100Poss  FLOAT    NOT NULL,
            PlusMinusSTLPer100Poss  FLOAT    NOT NULL,
            PlusMinusBLKPer100Poss  FLOAT    NOT NULL,
            PlusMinusTOVPer100Poss  FLOAT    NOT NULL,
            PlusMinusPFPer100Poss   FLOAT    NOT NULL
        );
        '''.format(tablename))


def create_team_season_adv_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            TeamID          INTEGER     NOT NULL,
            Season          INTEGER     NOT NULL,
            PythagoreanWins        INTEGER     NOT NULL,
            PythagoreanLosses      INTEGER     NOT NULL,
            MarginOfVictory FLOAT    NOT NULL,
            StrengthOfSchedule FLOAT NOT NULL,
            SimpleRatingSystem FLOAT NOT NULL,
            ORtg            FLOAT    NOT NULL,
            DRtg            FLOAT    NOT NULL,
            Pace            FLOAT    NOT NULL,
            FTr             FLOAT    NOT NULL,
            3PAr            FLOAT    NOT NULL,
            eFGPercent            FLOAT    NOT NULL,
            TOVPercent            FLOAT    NOT NULL,
            ORBPercent            FLOAT    NOT NULL,
            FTPerFGA          FLOAT    NOT NULL,
            eFGPercentAgainst     FLOAT    NOT NULL,
            TOVPercentAgainst     FLOAT    NOT NULL,
            DRBPercentAgainst     FLOAT    NOT NULL,
            FTPerFGAAgainst   FLOAT    NOT NULL,
            PRIMARY KEY(TeamID, Season)
        );
        '''.format(tablename))

# PLAYER STATS TABLES


def create_general_player_info_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            PlayerID    INTEGER         PRIMARY KEY     NOT NULL,
            Player      VARCHAR(255)    NOT NULL,
            PlayerLink  VARCHAR(255)    NOT NULL,
            BirthDate   VARCHAR(255)    NOT NULL,
            Position1   VARCHAR(2)      NOT NULL,
            Position2   VARCHAR(2),
            Position3   VARCHAR(2),
            DraftPick   INTEGER,
            Height      VARCHAR(4)      NOT NULL,
            Weight      INTEGER         NOT NULL,
            College     VARCHAR(255)
        );
        '''.format(tablename))


def create_per_game_player_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            PlayerID    INTEGER     NOT NULL,
            Season      INTEGER     NOT NULL,
            GameTypeID  INTEGER     NOT NULL,
            Age         INTEGER     NOT NULL,
            TeamID      INTEGER     NOT NULL,
            MPPerG        FLOAT    NOT NULL,
            FGPerG        FLOAT    NOT NULL,
            FGAPerG       FLOAT    NOT NULL,
            FGPercent         FLOAT    NOT NULL,
            3PPerG        FLOAT    NOT NULL,
            3PAPerG       FLOAT    NOT NULL,
            3PPercent         FLOAT    NOT NULL,
            2PPerG        FLOAT    NOT NULL,
            2PAPerG       FLOAT    NOT NULL,
            2PPercent         FLOAT    NOT NULL,
            eFGPercent        FLOAT    NOT NULL,
            FTPerG        FLOAT    NOT NULL,
            FTAPerG       FLOAT    NOT NULL,
            FTPercent         FLOAT    NOT NULL,
            ORBPerG       FLOAT    NOT NULL,
            DRBPerG       FLOAT    NOT NULL,
            TRBPerG       FLOAT    NOT NULL,
            ASTPerG       FLOAT    NOT NULL,
            STLPerG       FLOAT    NOT NULL,
            BLKPerG       FLOAT    NOT NULL,
            TOVPerG       FLOAT    NOT NULL,
            PFPerG        FLOAT    NOT NULL,
            PTSPerG       FLOAT    NOT NULL,
            PRIMARY KEY(PlayerID, Season, GameTypeID, TeamID)
        );
        '''.format(tablename))


# per_36_min_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'FG/36',
#                    'FGA/36', 'FG%', '3P/36', '3PA/36', '3P%', '2P/36', '2PA/36', '2P%',
#                    'FT/36', 'FTA/36', 'FT%', 'ORB/36', 'DRB/36', 'TRB/36', 'AST/36',
#                    'STL/36', 'BLK/36', 'TOV/36', 'PF/36', 'PTS/36']


def create_per_36_min_player_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            PlayerID    INTEGER     NOT NULL,
            Season      INTEGER     NOT NULL,
            GameTypeID  INTEGER     NOT NULL,
            Age         INTEGER     NOT NULL,
            TeamID      INTEGER     NOT NULL,
            FGPer36       FLOAT    NOT NULL,
            FGAPer36      FLOAT    NOT NULL,
            FGPercent         FLOAT    NOT NULL,
            3PPer36       FLOAT    NOT NULL,
            3PAPer36      FLOAT    NOT NULL,
            3PPercent         FLOAT    NOT NULL,
            2PPer36       FLOAT    NOT NULL,
            2PAPer36      FLOAT    NOT NULL,
            2PPercent         FLOAT    NOT NULL,
            FTPer36       FLOAT    NOT NULL,
            FTAPer36      FLOAT    NOT NULL,
            FTPercent         FLOAT    NOT NULL,
            ORBPer36      FLOAT    NOT NULL,
            DRBPer36      FLOAT    NOT NULL,
            TRBPer36      FLOAT    NOT NULL,
            ASTPer36      FLOAT    NOT NULL,
            STLPer36      FLOAT    NOT NULL,
            BLKPer36      FLOAT    NOT NULL,
            TOVPer36      FLOAT    NOT NULL,
            PFPer36       FLOAT    NOT NULL,
            PTSPer36      FLOAT    NOT NULL,
            PRIMARY KEY(PlayerID, Season, GameTypeID, TeamID)
        );
        '''.format(tablename))


# per_100_poss_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'FG/100Poss', 'FGA/100Poss',
#                      'FG%', '3P/100Poss', '3PA/100Poss', '3P%', '2P/100Poss', '2PA/100Poss', '2P%',
#                      'FT/100Poss', 'FTA/100Poss', 'FT%', 'ORB/100Poss', 'DRB/100Poss', 'TRB/100Poss',
#                      'AST/100Poss', 'STL/100Poss', 'BLK/100Poss', 'TOV/100Poss', 'PF/100Poss',
#                      'PTS/100Poss', 'ORtg/100Poss', 'DRtg/100Poss']


def create_per_100_poss_player_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            PlayerID    INTEGER     NOT NULL,
            Season      INTEGER     NOT NULL,
            GameTypeID  INTEGER     NOT NULL,
            Age         INTEGER     NOT NULL,
            TeamID      INTEGER     NOT NULL,
            FGPer100Poss  FLOAT    NOT NULL,
            FGAPer100Poss FLOAT    NOT NULL,
            FGPercent         FLOAT    NOT NULL,
            3PPer100Poss  FLOAT    NOT NULL,
            3PAPer100Poss FLOAT    NOT NULL,
            3PPercent         FLOAT    NOT NULL,
            2PPer100Poss  FLOAT    NOT NULL,
            2PAPer100Poss FLOAT    NOT NULL,
            2PPercent         FLOAT    NOT NULL,
            FTPer100Poss  FLOAT    NOT NULL,
            FTAPer100Poss FLOAT    NOT NULL,
            FTPercent         FLOAT    NOT NULL,
            ORBPer100Poss FLOAT    NOT NULL,
            DRBPer100Poss FLOAT    NOT NULL,
            TRBPer100Poss FLOAT    NOT NULL,
            ASTPer100Poss FLOAT    NOT NULL,
            STLPer100Poss FLOAT    NOT NULL,
            BLKPer100Poss FLOAT    NOT NULL,
            TOVPer100Poss FLOAT    NOT NULL,
            PFPer100Poss  FLOAT    NOT NULL,
            PTSPer100Poss FLOAT    NOT NULL,
            ORtgPer100Poss FLOAT    NOT NULL,
            DRtgPer100Poss FLOAT    NOT NULL,
            PRIMARY KEY(PlayerID, Season, GameTypeID, TeamID)
        );
        '''.format(tablename))

# adv_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'PER', 'TS%', '3PAr',
#             'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%', 'OWS', 'DWS',
#             'WS', 'WS/48', 'OBRM', 'DBPM', 'BPM', 'VORP']


def create_adv_player_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            PlayerID    INTEGER     NOT NULL,
            Season      INTEGER     NOT NULL,
            GameTypeID  INTEGER     NOT NULL,
            Age         INTEGER     NOT NULL,
            TeamID      INTEGER     NOT NULL,
            PER         FLOAT    NOT NULL,
            TSPercent         FLOAT    NOT NULL,
            3PAr        FLOAT    NOT NULL,
            FTr         FLOAT    NOT NULL,
            ORBPercent        FLOAT    NOT NULL,
            DRBPercent        FLOAT    NOT NULL,
            TRBPercent        FLOAT    NOT NULL,
            ASTPercent        FLOAT    NOT NULL,
            STLPercent        FLOAT    NOT NULL,
            BLKPercent        FLOAT    NOT NULL,
            TOVPercent        FLOAT    NOT NULL,
            USGPercent        FLOAT    NOT NULL,
            OWS         FLOAT    NOT NULL,
            DWS         FLOAT    NOT NULL,
            WS          FLOAT    NOT NULL,
            WSPer48       FLOAT    NOT NULL,
            OBRM        FLOAT    NOT NULL,
            DBPM        FLOAT    NOT NULL,
            BPM         FLOAT    NOT NULL,
            VORP        FLOAT    NOT NULL,
            PRIMARY KEY(PlayerID, Season, GameTypeID, TeamID)
        );
        '''.format(tablename))


# shooting_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'FG%', 'AvgShotDist',
#                  '2PA%', '%FGA0-2ft', '%FGA3-9ft', '%FGA10-15ft', '%FGA16+ft<3', '%FGA3P', '2PFG%',
#                  'FG%0-2ft', 'FG%3-9ft', 'FG%10-15ft', 'FG%16+ft<3', '3PFG%', '%2PAAstByOthers',
#                  '%3PAAstByOthers', '%3PAFromCorner', '3P%FromCorner']


def create_shooting_player_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            PlayerID    INTEGER     NOT NULL,
            Season      INTEGER     NOT NULL,
            GameTypeID  INTEGER     NOT NULL,
            Age         INTEGER     NOT NULL,
            TeamID      INTEGER     NOT NULL,
            FGPercent         FLOAT    NOT NULL,
            AvgShotDist FLOAT    NOT NULL,
            2PAPercent        FLOAT    NOT NULL,
            PercentFGA0to2ft   FLOAT    NOT NULL,
            PercentFGA3to9ft   FLOAT    NOT NULL,
            PercentFGA10to15ft FLOAT    NOT NULL,
            PercentFGA16Plusftto3 FLOAT    NOT NULL,
            PercentFGA3P      FLOAT    NOT NULL,
            2PFGPercent       FLOAT    NOT NULL,
            FGPercent0to2ft    FLOAT    NOT NULL,
            FGPercent3to9ft    FLOAT    NOT NULL,
            FGPercent10to15ft  FLOAT    NOT NULL,
            FGPercent16Plusftto3  FLOAT    NOT NULL,
            3PFGPercent       FLOAT    NOT NULL,
            Percent2PAAstByOthers FLOAT NOT NULL,
            Percent3PAAstByOthers FLOAT NOT NULL,
            Percent3PAFromCorner FLOAT NOT NULL,
            3PPercentFromCorner FLOAT  NOT NULL,
            PRIMARY KEY(PlayerID, Season, GameTypeID, TeamID)
        );
        '''.format(tablename))


# pbp_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'PG%', 'SG%', 'SF%',
#             'PF%', 'C%', '+/-Per100PossOnCourt', '+/-Per100PossOn-Off', 'BadPassTO', 'LostBallTO',
#             'OtherTO', 'ShootingFoulsCommitted', 'BlockingFoulsCommitted',
#             'OffensiveFoulsCommitted', 'TakeFoulsCommitted', 'PtsGenByAst', 'ShootingFoulsDrawn',
#             'And1s', 'BlockedFGA']


def create_pbp_player_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            PlayerID    INTEGER     NOT NULL,
            Season      INTEGER     NOT NULL,
            GameTypeID  INTEGER     NOT NULL,
            Age         INTEGER     NOT NULL,
            TeamID      INTEGER     NOT NULL,
            PGPercent         FLOAT    NOT NULL,
            SGPercent         FLOAT    NOT NULL,
            SFPercent         FLOAT    NOT NULL,
            PFPercent         FLOAT    NOT NULL,
            CPercent          FLOAT    NOT NULL,
            PlusMinusPer100PossOnCourt FLOAT NOT NULL,
            PlusMinusPer100PossOnOff FLOAT NOT NULL,
            BadPassTO   INTEGER     NOT NULL,
            LostBallTO  INTEGER     NOT NULL,
            OtherTO     INTEGER     NOT NULL,
            ShootingFoulsCommitted INTEGER   NOT NULL,
            BlockingFoulsCommitted INTEGER   NOT NULL,
            OffensiveFoulsCommitted INTEGER   NOT NULL,
            TakeFoulsCommitted INTEGER     NOT NULL,
            PtsGenByAst INTEGER     NOT NULL,
            ShootingFoulsDrawn INTEGER NOT NULL,
            And1s       INTEGER     NOT NULL,
            BlockedFGA  INTEGER     NOT NULL,
            PRIMARY KEY(PlayerID, Season, GameTypeID, TeamID)
        );
        '''.format(tablename))


# on_off_cols = ['PlayerID', 'Season', 'GameTypeID', 'TeamID', 'OnCourtTeameFG%', 'OnCourtTeamORB%',
#                'OnCourtTeamDRB%', 'OnCourtTeamTRB%', 'OnCourtTeamAST%', 'OnCourtTeamSTL%', 'OnCourtTeamBLK%',
#                'OnCourtTeamTOV%', 'OnCourtTeamORtg', 'OnCourtOppeFG%', 'OnCourtOppORB%', 'OnCourtOppDRB%',
#                'OnCourtOppTRB%', 'OnCourtOppAST%', 'OnCourtOppSTL%', 'OnCourtOppBLK%', 'OnCourtOppTOV%',
#                'OnCourtOppORtg', 'OnCourtDiffeFG%', 'OnCourtDiffORB%', 'OnCourtDiffDRB%', 'OnCourtDiffTRB%',
#                'OnCourtDiffAST%', 'OnCourtDiffSTL%', 'OnCourtDiffBLK%', 'OnCourtDiffTOV%', 'OnCourtDiffORtg',
#                'OffCourtTeameFG%', 'OffCourtTeamORB%', 'OffCourtTeamDRB%', 'OffCourtTeamTRB%', 'OffCourtTeamAST%',
#                'OffCourtTeamSTL%', 'OffCourtTeamBLK%', 'OffCourtTeamTOV%', 'OffCourtTeamORtg', 'OffCourtOppeFG%',
#                'OffCourtOppORB%', 'OffCourtOppDRB%', 'OffCourtOppTRB%', 'OffCourtOppAST%', 'OffCourtOppSTL%',
#                'OffCourtOppBLK%', 'OffCourtOppTOV%', 'OffCourtOppORtg', 'OffCourtDiffeFG%', 'OffCourtDiffORB%',
#                'OffCourtDiffDRB%', 'OffCourtDiffTRB%', 'OffCourtDiffAST%', 'OffCourtDiffSTL%',
#                'OffCourtDiffBLK%', 'OffCourtDiffTOV%', 'OffCourtDiffORtg', 'On-OffTeameFG%', 'On-OffTeamORB%',
#                'On-OffTeamDRB%', 'On-OffTeamTRB%', 'On-OffTeamAST%', 'On-OffTeamSTL%', 'On-OffTeamBLK%',
#                'On-OffTeamTOV%', 'On-OffTeamORtg', 'On-OffOppeFG%', 'On-OffOppORB%', 'On-OffOppDRB%',
#                'On-OffOppTRB%', 'On-OffOppAST%', 'On-OffOppSTL%', 'On-OffOppBLK%', 'On-OffOppTOV%',
#                'On-OffOppORtg', 'On-OffDiffeFG%', 'On-OffDiffORB%', 'On-OffDiffDRB%', 'On-OffDiffTRB%',
#                'On-OffDiffAST%', 'On-OffDiffSTL%', 'On-OffDiffBLK%', 'On-OffDiffTOV%', 'On-OffDiffORtg']


def create_on_off_player_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            PlayerID            INTEGER     NOT NULL,
            Season              INTEGER     NOT NULL,
            GameTypeID          INTEGER     NOT NULL,
            TeamID              INTEGER     NOT NULL,
            OnCourtTeameFGPercent     FLOAT    NOT NULL,
            OnCourtTeamORBPercent     FLOAT    NOT NULL,
            OnCourtTeamDRBPercent     FLOAT    NOT NULL,
            OnCourtTeamTRBPercent     FLOAT    NOT NULL,
            OnCourtTeamASTPercent     FLOAT    NOT NULL,
            OnCourtTeamSTLPercent     FLOAT    NOT NULL,
            OnCourtTeamBLKPercent     FLOAT    NOT NULL,
            OnCourtTeamTOVPercent     FLOAT    NOT NULL,
            OnCourtTeamORtg     FLOAT    NOT NULL,
            OnCourtOppeFGPercent     FLOAT    NOT NULL,
            OnCourtOppORBPercent     FLOAT    NOT NULL,
            OnCourtOppDRBPercent     FLOAT    NOT NULL,
            OnCourtOppTRBPercent     FLOAT    NOT NULL,
            OnCourtOppASTPercent     FLOAT    NOT NULL,
            OnCourtOppSTLPercent     FLOAT    NOT NULL,
            OnCourtOppBLKPercent     FLOAT    NOT NULL,
            OnCourtOppTOVPercent     FLOAT    NOT NULL,
            OnCourtOppORtg     FLOAT    NOT NULL,
            OnCourtDiffeFGPercent     FLOAT    NOT NULL,
            OnCourtDiffORBPercent     FLOAT    NOT NULL,
            OnCourtDiffDRBPercent     FLOAT    NOT NULL,
            OnCourtDiffTRBPercent     FLOAT    NOT NULL,
            OnCourtDiffASTPercent     FLOAT    NOT NULL,
            OnCourtDiffSTLPercent     FLOAT    NOT NULL,
            OnCourtDiffBLKPercent     FLOAT    NOT NULL,
            OnCourtDiffTOVPercent     FLOAT    NOT NULL,
            OnCourtDiffORtg     FLOAT    NOT NULL,
            OffCourtTeameFGPercent     FLOAT    NOT NULL,
            OffCourtTeamORBPercent     FLOAT    NOT NULL,
            OffCourtTeamDRBPercent     FLOAT    NOT NULL,
            OffCourtTeamTRBPercent     FLOAT    NOT NULL,
            OffCourtTeamASTPercent     FLOAT    NOT NULL,
            OffCourtTeamSTLPercent     FLOAT    NOT NULL,
            OffCourtTeamBLKPercent     FLOAT    NOT NULL,
            OffCourtTeamTOVPercent     FLOAT    NOT NULL,
            OffCourtTeamORtg     FLOAT    NOT NULL,
            OffCourtOppeFGPercent     FLOAT    NOT NULL,
            OffCourtOppORBPercent     FLOAT    NOT NULL,
            OffCourtOppDRBPercent     FLOAT    NOT NULL,
            OffCourtOppTRBPercent     FLOAT    NOT NULL,
            OffCourtOppASTPercent     FLOAT    NOT NULL,
            OffCourtOppSTLPercent     FLOAT    NOT NULL,
            OffCourtOppBLKPercent     FLOAT    NOT NULL,
            OffCourtOppTOVPercent     FLOAT    NOT NULL,
            OffCourtOppORtg     FLOAT    NOT NULL,
            OffCourtDiffeFGPercent     FLOAT    NOT NULL,
            OffCourtDiffORBPercent     FLOAT    NOT NULL,
            OffCourtDiffDRBPercent     FLOAT    NOT NULL,
            OffCourtDiffTRBPercent     FLOAT    NOT NULL,
            OffCourtDiffASTPercent     FLOAT    NOT NULL,
            OffCourtDiffSTLPercent     FLOAT    NOT NULL,
            OffCourtDiffBLKPercent     FLOAT    NOT NULL,
            OffCourtDiffTOVPercent     FLOAT    NOT NULL,
            OffCourtDiffORtg     FLOAT    NOT NULL,
            OnOffTeameFGPercent     FLOAT    NOT NULL,
            OnOffTeamORBPercent     FLOAT    NOT NULL,
            OnOffTeamDRBPercent     FLOAT    NOT NULL,
            OnOffTeamTRBPercent     FLOAT    NOT NULL,
            OnOffTeamASTPercent     FLOAT    NOT NULL,
            OnOffTeamSTLPercent     FLOAT    NOT NULL,
            OnOffTeamBLKPercent     FLOAT    NOT NULL,
            OnOffTeamTOVPercent     FLOAT    NOT NULL,
            OnOffTeamORtg     FLOAT    NOT NULL,
            OnOffOppeFGPercent     FLOAT    NOT NULL,
            OnOffOppORBPercent     FLOAT    NOT NULL,
            OnOffOppDRBPercent     FLOAT    NOT NULL,
            OnOffOppTRBPercent     FLOAT    NOT NULL,
            OnOffOppASTPercent     FLOAT    NOT NULL,
            OnOffOppSTLPercent     FLOAT    NOT NULL,
            OnOffOppBLKPercent     FLOAT    NOT NULL,
            OnOffOppTOVPercent     FLOAT    NOT NULL,
            OnOffOppORtg     FLOAT    NOT NULL,
            OnOffDiffeFGPercent     FLOAT    NOT NULL,
            OnOffDiffORBPercent     FLOAT    NOT NULL,
            OnOffDiffDRBPercent     FLOAT    NOT NULL,
            OnOffDiffTRBPercent     FLOAT    NOT NULL,
            OnOffDiffASTPercent     FLOAT    NOT NULL,
            OnOffDiffSTLPercent     FLOAT    NOT NULL,
            OnOffDiffBLKPercent     FLOAT    NOT NULL,
            OnOffDiffTOVPercent     FLOAT    NOT NULL,
            OnOffDiffORtg     FLOAT    NOT NULL,
            PRIMARY KEY(PlayerID, Season, GameTypeID, TeamID)
        );
        '''.format(tablename))


def create_tables():
    print('Starting...')
    conn = open_db()

    c1 = conn.cursor()

    create_player_game_stats_table(c1, 'PlayerGameStats')
    create_team_game_stats_table(c1, 'TeamGameStats')
    create_general_team_info_table(c1, 'GeneralTeamSeasonInfo')
    create_team_season_stats_table(c1, 'TeamSeasonStats')
    create_team_season_stats_table(c1, 'TeamOppSeasonStats')
    create_team_season_lineup_stats_table(c1, 'TeamSeasonLineupStats')
    create_team_season_adv_stats_table(c1, 'TeamSeasonAdvStats')
    create_general_player_info_table(c1, 'GeneralPlayerInfo')
    create_per_game_player_stats_table(c1, 'PlayerSeasonStatsPerGame')
    create_per_36_min_player_stats_table(c1, 'PlayerSeasonStatsPer36Minutes')
    create_per_100_poss_player_stats_table(c1, 'PlayerSeasonStatsPer100Poss')
    create_adv_player_stats_table(c1, 'PlayerSeasonAdvStats')
    create_shooting_player_stats_table(c1, 'PlayerSeasonShootingStats')
    create_pbp_player_stats_table(c1, 'PlayerSeasonPBPStats')
    create_on_off_player_stats_table(c1, 'PlayerSeasonOnOffStats')

    create_player_game_stats_table(c1, 'PlayerGameStatsYTD')
    create_team_game_stats_table(c1, 'TeamGameStatsYTD')
    create_team_season_stats_table(c1, 'TeamSeasonStatsYTD')
    create_team_season_stats_table(c1, 'TeamOppSeasonStatsYTD')
    create_team_season_lineup_stats_table(c1, 'TeamSeasonLineupStatsYTD')
    create_team_season_adv_stats_table(c1, 'TeamSeasonAdvStatsYTD')
    create_per_game_player_stats_table(c1, 'PlayerSeasonStatsPerGameYTD')
    create_per_36_min_player_stats_table(c1, 'PlayerSeasonStatsPer36MinutesYTD')
    create_per_100_poss_player_stats_table(c1, 'PlayerSeasonStatsPer100PossYTD')
    create_adv_player_stats_table(c1, 'PlayerSeasonAdvStatsYTD')
    create_shooting_player_stats_table(c1, 'PlayerSeasonShootingStatsYTD')
    create_pbp_player_stats_table(c1, 'PlayerSeasonPBPStatsYTD')
    create_on_off_player_stats_table(c1, 'PlayerSeasonOnOffStatsYTD')

    conn.commit()
    print('Done')
    conn.close()


def main():
    create_tables()


if __name__ == "__main__":
    main()
