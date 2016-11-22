import sqlite3 as lite

# ******************************************************** #
# ******************************************************** #
# NOTES:
#   -
# ******************************************************** #
# ******************************************************** #


def open_db(db_file):
    conn = lite.connect(db_file)
    print('Opened database in file {} successfully'.format(db_file))
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
            "GameID"      INT     NOT NULL,
            "PlayerID"    INT     NOT NULL,
            "PlayerLink"  TEXT    NOT NULL,
            "TeamID"      INT     NOT NULL,
            "MP"          INT     NOT NULL,
            "FG"          INT     NOT NULL,
            "FGA"         INT     NOT NULL,
            "FG%"         REAL    NOT NULL,
            "3P"          INT     NOT NULL,
            "3PA"         INT     NOT NULL,
            "3P%"         REAL    NOT NULL,
            "FT"          INT     NOT NULL,
            "FTA"         INT     NOT NULL,
            "FT%"         REAL    NOT NULL,
            "ORB"         INT     NOT NULL,
            "DRB"         INT     NOT NULL,
            "TRB"         INT     NOT NULL,
            "AST"         INT     NOT NULL,
            "STL"         INT     NOT NULL,
            "BLK"         INT     NOT NULL,
            "TOV"         INT     NOT NULL,
            "PF"          INT     NOT NULL,
            "PTS"         INT     NOT NULL,
            "+/-"         REAL,
            "TS%"         REAL    NOT NULL,
            "eFG%"        REAL    NOT NULL,
            "3PAr"        REAL    NOT NULL,
            "FTr"         REAL    NOT NULL,
            "ORB%"        REAL    NOT NULL,
            "DRB%"        REAL    NOT NULL,
            "TRB%"        REAL    NOT NULL,
            "AST%"        REAL    NOT NULL,
            "STL%"        REAL    NOT NULL,
            "BLK%"        REAL    NOT NULL,
            "TOV%"        REAL    NOT NULL,
            "USG%"        REAL    NOT NULL,
            "ORtg"        REAL    NOT NULL,
            "DRtg"        REAL    NOT NULL,
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
            "GameID"      INT     NOT NULL,
            "Season"      INT     NOT NULL,
            "Date"        TEXT    NOT NULL,
            "GameTypeID"  INT     NOT NULL,
            "Team"        TEXT    NOT NULL,
            "TeamID"      INT     NOT NULL,
            "Q1Score"     INT     NOT NULL,
            "Q2Score"     INT     NOT NULL,
            "Q3Score"     INT     NOT NULL,
            "Q4Score"     INT     NOT NULL,
            "OTScore"     INT,
            "FinalScore"  INT     NOT NULL,
            "FG"          INT     NOT NULL,
            "FGA"         INT     NOT NULL,
            "FG%"         REAL    NOT NULL,
            "3P"          INT     NOT NULL,
            "3PA"         INT     NOT NULL,
            "3P%"         REAL    NOT NULL,
            "FT"          INT     NOT NULL,
            "FTA"         INT     NOT NULL,
            "FT%"         REAL    NOT NULL,
            "ORB"         INT     NOT NULL,
            "DRB"         INT     NOT NULL,
            "TRB"         INT     NOT NULL,
            "AST"         INT     NOT NULL,
            "STL"         INT     NOT NULL,
            "BLK"         INT     NOT NULL,
            "TOV"         INT     NOT NULL,
            "PF"          INT     NOT NULL,
            "TS%"         REAL    NOT NULL,
            "eFG%"        REAL    NOT NULL,
            "3PAr"        REAL    NOT NULL,
            "FTr"         REAL    NOT NULL,
            "ORB%"        REAL    NOT NULL,
            "DRB%"        REAL    NOT NULL,
            "TRB%"        REAL    NOT NULL,
            "AST%"        REAL    NOT NULL,
            "STL%"        REAL    NOT NULL,
            "BLK%"        REAL    NOT NULL,
            "TOV%"        REAL    NOT NULL,
            "ORtg"        REAL    NOT NULL,
            "DRtg"        REAL    NOT NULL,
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
            "TeamID"        INT     NOT NULL,
            "Season"        INT     NOT NULL,
            "Team"          TEXT    NOT NULL,
            "Games"         INT     NOT NULL,
            "Wins"          INT     NOT NULL,
            "Losses"        INT     NOT NULL,
            "Division"      TEXT,
            "DivisionID"    INT,
            "DivisionRank"  INT,
            "Conference"    TEXT    NOT NULL,
            "ConferenceID"  INT     NOT NULL,
            "ConferenceRank" INT,
            "HeadCoach1"    TEXT    NOT NULL,
            "HeadCoach1Wins" INT    NOT NULL,
            "HeadCoach1Losses" INT  NOT NULL,
            "HeadCoach2"    TEXT,
            "HeadCoach2Wins" INT,
            "HeadCoach2Losses" INT,
            "Round1Wins"    INT,
            "Round1Losses"  INT,
            "Round1Opp"     INT,
            "Round1Won"     INT,
            "Round2Wins"    INT,
            "Round2Losses"  INT,
            "Round2Opp"     INT,
            "Round2Won"     INT,
            "Round3Wins"    INT,
            "Round3Losses"  INT,
            "Round3Opp"     INT,
            "Round3Won"     INT,
            "Round4Wins"    INT,
            "Round4Losses"  INT,
            "Round4Opp"     INT,
            "Round4Won"     INT,
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
            "TeamID"      INT     NOT NULL,
            "Season"      INT     NOT NULL,
            "FG/G"        REAL    NOT NULL,
            "FGA/G"       REAL    NOT NULL,
            "FG%"         REAL    NOT NULL,
            "3P/G"        REAL    NOT NULL,
            "3PA/G"       REAL    NOT NULL,
            "3P%"         REAL    NOT NULL,
            "2P/G"        REAL    NOT NULL,
            "2PA/G"       REAL    NOT NULL,
            "2P%"         REAL    NOT NULL,
            "FT/G"        REAL    NOT NULL,
            "FTA/G"       REAL    NOT NULL,
            "FT%"         REAL    NOT NULL,
            "ORB/G"       REAL    NOT NULL,
            "DRB/G"       REAL    NOT NULL,
            "TRB/G"       REAL    NOT NULL,
            "AST/G"       REAL    NOT NULL,
            "STL/G"       REAL    NOT NULL,
            "BLK/G"       REAL    NOT NULL,
            "TOV/G"       REAL    NOT NULL,
            "PF/G"        REAL    NOT NULL,
            "PTS/G"       REAL    NOT NULL,
            "FG/GYOY%Change" REAL NOT NULL,
            "FGA/GYOY%Change" REAL NOT NULL,
            "FG%YOY%Change" REAL    NOT NULL,
            "3P/GYOY%Change" REAL    NOT NULL,
            "3PA/GYOY%Change" REAL    NOT NULL,
            "3P%YOY%Change" REAL    NOT NULL,
            "2P/GYOY%Change" REAL    NOT NULL,
            "2PA/GYOY%Change" REAL    NOT NULL,
            "2P%YOY%Change" REAL    NOT NULL,
            "FT/GYOY%Change" REAL    NOT NULL,
            "FTA/GYOY%Change" REAL    NOT NULL,
            "FT%YOY%Change" REAL    NOT NULL,
            "ORB/GYOY%Change" REAL    NOT NULL,
            "DRB/GYOY%Change" REAL    NOT NULL,
            "TRB/GYOY%Change" REAL    NOT NULL,
            "AST/GYOY%Change" REAL    NOT NULL,
            "STL/GYOY%Change" REAL    NOT NULL,
            "BLK/GYOY%Change" REAL    NOT NULL,
            "TOV/GYOY%Change" REAL    NOT NULL,
            "PF/GYOY%Change" REAL    NOT NULL,
            "PTS/GYOY%Change" REAL    NOT NULL,
            PRIMARY KEY(TeamID, Season)
        );
        '''.format(table_name))


def create_team_season_lineup_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            "TeamID"          INT     NOT NULL,
            "Season"          INT     NOT NULL,
            "GameTypeID"      INT     NOT NULL,
            "NumPlayers"      INT     NOT NULL,
            "Player1ID"       INT     NOT NULL,
            "Player2ID"       INT     NOT NULL,
            "Player3ID"       INT,
            "Player4ID"       INT,
            "Player5ID"       INT,
            "MP"              INT     NOT NULL,
            "+/-FG/100Poss"   REAL    NOT NULL,
            "+/-FGA/100Poss"  REAL    NOT NULL,
            "+/-FG%/100Poss"  REAL    NOT NULL,
            "+/-3P/100Poss"   REAL    NOT NULL,
            "+/-3PA/100Poss"  REAL    NOT NULL,
            "+/-3P%/100Poss"  REAL    NOT NULL,
            "+/-eFG%/100Poss" REAL    NOT NULL,
            "+/-FT/100Poss"   REAL    NOT NULL,
            "+/-FTA/100Poss"  REAL    NOT NULL,
            "+/-FT%/100Poss"  REAL    NOT NULL,
            "+/-PTS/100Poss"  REAL    NOT NULL,
            "+/-ORB/100Poss"  REAL    NOT NULL,
            "+/-ORB%/100Poss" REAL    NOT NULL,
            "+/-DRB/100Poss"  REAL    NOT NULL,
            "+/-DRB%/100Poss" REAL    NOT NULL,
            "+/-TRB/100Poss"  REAL    NOT NULL,
            "+/-TRB%/100Poss" REAL    NOT NULL,
            "+/-AST/100Poss"  REAL    NOT NULL,
            "+/-STL/100Poss"  REAL    NOT NULL,
            "+/-BLK/100Poss"  REAL    NOT NULL,
            "+/-TOV/100Poss"  REAL    NOT NULL,
            "+/-PF/100Poss"   REAL    NOT NULL
        );
        '''.format(tablename))


def create_team_season_adv_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            "TeamID"          INT     NOT NULL,
            "Season"          INT     NOT NULL,
            "PythWins"        INT     NOT NULL,
            "PythLosses"      INT     NOT NULL,
            "MarginOfVictory" REAL    NOT NULL,
            "StrengthOfSchedule" REAL NOT NULL,
            "SimpleRatingSystem" REAL NOT NULL,
            "ORtg"            REAL    NOT NULL,
            "DRtg"            REAL    NOT NULL,
            "Pace"            REAL    NOT NULL,
            "FTr"             REAL    NOT NULL,
            "3PAr"            REAL    NOT NULL,
            "eFG%"            REAL    NOT NULL,
            "TOV%"            REAL    NOT NULL,
            "ORB%"            REAL    NOT NULL,
            "FT/FGA"          REAL    NOT NULL,
            "eFG%Against"     REAL    NOT NULL,
            "TOV%Against"     REAL    NOT NULL,
            "DRB%Against"     REAL    NOT NULL,
            "FT/FGAAgainst"   REAL    NOT NULL,
            PRIMARY KEY(TeamID, Season)
        );
        '''.format(tablename))

# PLAYER STATS TABLES


def create_general_player_info_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            "PlayerID"    INT     PRIMARY KEY     NOT NULL,
            "Player"      TEXT    NOT NULL,
            "PlayerLink"  TEXT    NOT NULL,
            "BirthDate"   TEXT    NOT NULL,
            "Position1"   TEXT    NOT NULL,
            "Position2"   TEXT,
            "Position3"   TEXT,
            "DraftPick"   INT,
            "Height"      INT     NOT NULL,
            "Weight"      INT     NOT NULL,
            "College"     TEXT
        );
        '''.format(tablename))


def create_per_game_player_stats_table(c, tablename):
    c.execute('''
        CREATE TABLE {}
        (
            "PlayerID"    INT     NOT NULL,
            "Season"      INT     NOT NULL,
            "GameTypeID"  INT     NOT NULL,
            "Age"         INT     NOT NULL,
            "TeamID"      INT     NOT NULL,
            "MP/G"        REAL    NOT NULL,
            "FG/G"        REAL    NOT NULL,
            "FGA/G"       REAL    NOT NULL,
            "FG%"         REAL    NOT NULL,
            "3P/G"        REAL    NOT NULL,
            "3PA/G"       REAL    NOT NULL,
            "3P%"         REAL    NOT NULL,
            "2P/G"        REAL    NOT NULL,
            "2PA/G"       REAL    NOT NULL,
            "2P%"         REAL    NOT NULL,
            "eFG%"        REAL    NOT NULL,
            "FT/G"        REAL    NOT NULL,
            "FTA/G"       REAL    NOT NULL,
            "FT%"         REAL    NOT NULL,
            "ORB/G"       REAL    NOT NULL,
            "DRB/G"       REAL    NOT NULL,
            "TRB/G"       REAL    NOT NULL,
            "AST/G"       REAL    NOT NULL,
            "STL/G"       REAL    NOT NULL,
            "BLK/G"       REAL    NOT NULL,
            "TOV/G"       REAL    NOT NULL,
            "PF/G"        REAL    NOT NULL,
            "PTS/G"       REAL    NOT NULL,
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
            "PlayerID"    INT     NOT NULL,
            "Season"      INT     NOT NULL,
            "GameTypeID"  INT     NOT NULL,
            "Age"         INT     NOT NULL,
            "TeamID"      INT     NOT NULL,
            "FG/36"       REAL    NOT NULL,
            "FGA/36"      REAL    NOT NULL,
            "FG%"         REAL    NOT NULL,
            "3P/36"       REAL    NOT NULL,
            "3PA/36"      REAL    NOT NULL,
            "3P%"         REAL    NOT NULL,
            "2P/36"       REAL    NOT NULL,
            "2PA/36"      REAL    NOT NULL,
            "2P%"         REAL    NOT NULL,
            "FT/36"       REAL    NOT NULL,
            "FTA/36"      REAL    NOT NULL,
            "FT%"         REAL    NOT NULL,
            "ORB/36"      REAL    NOT NULL,
            "DRB/36"      REAL    NOT NULL,
            "TRB/36"      REAL    NOT NULL,
            "AST/36"      REAL    NOT NULL,
            "STL/36"      REAL    NOT NULL,
            "BLK/36"      REAL    NOT NULL,
            "TOV/36"      REAL    NOT NULL,
            "PF/36"       REAL    NOT NULL,
            "PTS/36"      REAL    NOT NULL,
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
            "PlayerID"    INT     NOT NULL,
            "Season"      INT     NOT NULL,
            "GameTypeID"  INT     NOT NULL,
            "Age"         INT     NOT NULL,
            "TeamID"      INT     NOT NULL,
            "FG/100Poss"  REAL    NOT NULL,
            "FGA/100Poss" REAL    NOT NULL,
            "FG%"         REAL    NOT NULL,
            "3P/100Poss"  REAL    NOT NULL,
            "3PA/100Poss" REAL    NOT NULL,
            "3P%"         REAL    NOT NULL,
            "2P/100Poss"  REAL    NOT NULL,
            "2PA/100Poss" REAL    NOT NULL,
            "2P%"         REAL    NOT NULL,
            "FT/100Poss"  REAL    NOT NULL,
            "FTA/100Poss" REAL    NOT NULL,
            "FT%"         REAL    NOT NULL,
            "ORB/100Poss" REAL    NOT NULL,
            "DRB/100Poss" REAL    NOT NULL,
            "TRB/100Poss" REAL    NOT NULL,
            "AST/100Poss" REAL    NOT NULL,
            "STL/100Poss" REAL    NOT NULL,
            "BLK/100Poss" REAL    NOT NULL,
            "TOV/100Poss" REAL    NOT NULL,
            "PF/100Poss"  REAL    NOT NULL,
            "PTS/100Poss" REAL    NOT NULL,
            "ORtg/100Poss" REAL    NOT NULL,
            "DRtg/100Poss" REAL    NOT NULL,
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
            "PlayerID"    INT     NOT NULL,
            "Season"      INT     NOT NULL,
            "GameTypeID"  INT     NOT NULL,
            "Age"         INT     NOT NULL,
            "TeamID"      INT     NOT NULL,
            "PER"         REAL    NOT NULL,
            "TS%"         REAL    NOT NULL,
            "3PAr"        REAL    NOT NULL,
            "FTr"         REAL    NOT NULL,
            "ORB%"        REAL    NOT NULL,
            "DRB%"        REAL    NOT NULL,
            "TRB%"        REAL    NOT NULL,
            "AST%"        REAL    NOT NULL,
            "STL%"        REAL    NOT NULL,
            "BLK%"        REAL    NOT NULL,
            "TOV%"        REAL    NOT NULL,
            "USG%"        REAL    NOT NULL,
            "OWS"         REAL    NOT NULL,
            "DWS"         REAL    NOT NULL,
            "WS"          REAL    NOT NULL,
            "WS/48"       REAL    NOT NULL,
            "OBRM"        REAL    NOT NULL,
            "DBPM"        REAL    NOT NULL,
            "BPM"         REAL    NOT NULL,
            "VORP"        REAL    NOT NULL,
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
            "PlayerID"    INT     NOT NULL,
            "Season"      INT     NOT NULL,
            "GameTypeID"  INT     NOT NULL,
            "Age"         INT     NOT NULL,
            "TeamID"      INT     NOT NULL,
            "FG%"         REAL    NOT NULL,
            "AvgShotDist" REAL    NOT NULL,
            "2PA%"        REAL    NOT NULL,
            "%FGA0-2ft"   REAL    NOT NULL,
            "%FGA3-9ft"   REAL    NOT NULL,
            "%FGA10-15ft" REAL    NOT NULL,
            "%FGA16+ft<3" REAL    NOT NULL,
            "%FGA3P"      REAL    NOT NULL,
            "2PFG%"       REAL    NOT NULL,
            "FG%0-2ft"    REAL    NOT NULL,
            "FG%3-9ft"    REAL    NOT NULL,
            "FG%10-15ft"  REAL    NOT NULL,
            "FG%16+ft<3"  REAL    NOT NULL,
            "3PFG%"       REAL    NOT NULL,
            "%2PAAstByOthers" REAL NOT NULL,
            "%3PAAstByOthers" REAL NOT NULL,
            "%3PAFromCorner" REAL NOT NULL,
            "3P%FromCorner" REAL  NOT NULL,
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
            "PlayerID"    INT     NOT NULL,
            "Season"      INT     NOT NULL,
            "GameTypeID"  INT     NOT NULL,
            "Age"         INT     NOT NULL,
            "TeamID"      INT     NOT NULL,
            "PG%"         REAL    NOT NULL,
            "SG%"         REAL    NOT NULL,
            "SF%"         REAL    NOT NULL,
            "PF%"         REAL    NOT NULL,
            "C%"          REAL    NOT NULL,
            "+/-Per100PossOnCourt" REAL NOT NULL,
            "+/-Per100PossOn-Off" REAL NOT NULL,
            "BadPassTO"   INT     NOT NULL,
            "LostBallTO"  INT     NOT NULL,
            "OtherTO"     INT     NOT NULL,
            "ShootingFoulsCommitted" INT   NOT NULL,
            "BlockingFoulsCommitted" INT   NOT NULL,
            "OffensiveFoulsCommitted" INT   NOT NULL,
            "TakeFoulsCommitted" INT     NOT NULL,
            "PtsGenByAst" INT     NOT NULL,
            "ShootingFoulsDrawn" INT NOT NULL,
            "And1s"       INT     NOT NULL,
            "BlockedFGA"  INT     NOT NULL,
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
            "PlayerID"            INT     NOT NULL,
            "Season"              INT     NOT NULL,
            "GameTypeID"          INT     NOT NULL,
            "TeamID"              INT     NOT NULL,
            "OnCourtTeameFG%"     REAL    NOT NULL,
            "OnCourtTeamORB%"     REAL    NOT NULL,
            "OnCourtTeamDRB%"     REAL    NOT NULL,
            "OnCourtTeamTRB%"     REAL    NOT NULL,
            "OnCourtTeamAST%"     REAL    NOT NULL,
            "OnCourtTeamSTL%"     REAL    NOT NULL,
            "OnCourtTeamBLK%"     REAL    NOT NULL,
            "OnCourtTeamTOV%"     REAL    NOT NULL,
            "OnCourtTeamORtg"     REAL    NOT NULL,
            "OnCourtOppeFG%"     REAL    NOT NULL,
            "OnCourtOppORB%"     REAL    NOT NULL,
            "OnCourtOppDRB%"     REAL    NOT NULL,
            "OnCourtOppTRB%"     REAL    NOT NULL,
            "OnCourtOppAST%"     REAL    NOT NULL,
            "OnCourtOppSTL%"     REAL    NOT NULL,
            "OnCourtOppBLK%"     REAL    NOT NULL,
            "OnCourtOppTOV%"     REAL    NOT NULL,
            "OnCourtOppORtg"     REAL    NOT NULL,
            "OnCourtDiffeFG%"     REAL    NOT NULL,
            "OnCourtDiffORB%"     REAL    NOT NULL,
            "OnCourtDiffDRB%"     REAL    NOT NULL,
            "OnCourtDiffTRB%"     REAL    NOT NULL,
            "OnCourtDiffAST%"     REAL    NOT NULL,
            "OnCourtDiffSTL%"     REAL    NOT NULL,
            "OnCourtDiffBLK%"     REAL    NOT NULL,
            "OnCourtDiffTOV%"     REAL    NOT NULL,
            "OnCourtDiffORtg"     REAL    NOT NULL,
            "OffCourtTeameFG%"     REAL    NOT NULL,
            "OffCourtTeamORB%"     REAL    NOT NULL,
            "OffCourtTeamDRB%"     REAL    NOT NULL,
            "OffCourtTeamTRB%"     REAL    NOT NULL,
            "OffCourtTeamAST%"     REAL    NOT NULL,
            "OffCourtTeamSTL%"     REAL    NOT NULL,
            "OffCourtTeamBLK%"     REAL    NOT NULL,
            "OffCourtTeamTOV%"     REAL    NOT NULL,
            "OffCourtTeamORtg"     REAL    NOT NULL,
            "OffCourtOppeFG%"     REAL    NOT NULL,
            "OffCourtOppORB%"     REAL    NOT NULL,
            "OffCourtOppDRB%"     REAL    NOT NULL,
            "OffCourtOppTRB%"     REAL    NOT NULL,
            "OffCourtOppAST%"     REAL    NOT NULL,
            "OffCourtOppSTL%"     REAL    NOT NULL,
            "OffCourtOppBLK%"     REAL    NOT NULL,
            "OffCourtOppTOV%"     REAL    NOT NULL,
            "OffCourtOppORtg"     REAL    NOT NULL,
            "OffCourtDiffeFG%"     REAL    NOT NULL,
            "OffCourtDiffORB%"     REAL    NOT NULL,
            "OffCourtDiffDRB%"     REAL    NOT NULL,
            "OffCourtDiffTRB%"     REAL    NOT NULL,
            "OffCourtDiffAST%"     REAL    NOT NULL,
            "OffCourtDiffSTL%"     REAL    NOT NULL,
            "OffCourtDiffBLK%"     REAL    NOT NULL,
            "OffCourtDiffTOV%"     REAL    NOT NULL,
            "OffCourtDiffORtg"     REAL    NOT NULL,
            "On-OffTeameFG%"     REAL    NOT NULL,
            "On-OffTeamORB%"     REAL    NOT NULL,
            "On-OffTeamDRB%"     REAL    NOT NULL,
            "On-OffTeamTRB%"     REAL    NOT NULL,
            "On-OffTeamAST%"     REAL    NOT NULL,
            "On-OffTeamSTL%"     REAL    NOT NULL,
            "On-OffTeamBLK%"     REAL    NOT NULL,
            "On-OffTeamTOV%"     REAL    NOT NULL,
            "On-OffTeamORtg"     REAL    NOT NULL,
            "On-OffOppeFG%"     REAL    NOT NULL,
            "On-OffOppORB%"     REAL    NOT NULL,
            "On-OffOppDRB%"     REAL    NOT NULL,
            "On-OffOppTRB%"     REAL    NOT NULL,
            "On-OffOppAST%"     REAL    NOT NULL,
            "On-OffOppSTL%"     REAL    NOT NULL,
            "On-OffOppBLK%"     REAL    NOT NULL,
            "On-OffOppTOV%"     REAL    NOT NULL,
            "On-OffOppORtg"     REAL    NOT NULL,
            "On-OffDiffeFG%"     REAL    NOT NULL,
            "On-OffDiffORB%"     REAL    NOT NULL,
            "On-OffDiffDRB%"     REAL    NOT NULL,
            "On-OffDiffTRB%"     REAL    NOT NULL,
            "On-OffDiffAST%"     REAL    NOT NULL,
            "On-OffDiffSTL%"     REAL    NOT NULL,
            "On-OffDiffBLK%"     REAL    NOT NULL,
            "On-OffDiffTOV%"     REAL    NOT NULL,
            "On-OffDiffORtg"     REAL    NOT NULL,
            PRIMARY KEY(PlayerID, Season, GameTypeID, TeamID)
        );
        '''.format(tablename))


def create_tables():
    print('Starting...')
    conn1 = open_db('Database\\test.db')

    c1 = conn1.cursor()

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

    conn1.commit()
    print('Done')
    conn1.close()


def main():
    create_tables()


if __name__ == "__main__":
    main()
