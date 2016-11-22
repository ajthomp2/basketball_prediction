import pandas as pd
import sqlite3 as lite

# ******************************************************** #
# ******************************************************** #
# NOTES:
#   - TEAM TABLES WORK
#   - PLAYER TABLES WORK
#       - figure out why there is one extra row in the per game stats table
# ******************************************************** #
# ******************************************************** #


# create_player_game_stats_table(c1, 'PlayerGameStats')
# create_team_game_stats_table(c1, 'TeamGameStats')
# create_general_team_info_table(c1, 'GeneralTeamSeasonInfo')
# create_team_season_stats_table(c1, 'TeamSeasonStats')
# create_team_season_stats_table(c1, 'TeamOppSeasonStats')
# create_team_season_lineup_stats_table(c1, 'TeamSeasonLineupStats')
# create_team_season_adv_stats_table(c1, 'TeamSeasonAdvStats')
# create_general_player_info_table(c1, 'GeneralPlayerInfo')
# create_per_game_player_stats_table(c1, 'PlayerSeasonStatsPerGame')
# create_per_36_min_player_stats_table(c1, 'PlayerSeasonStatsPer36Minutes')
# create_per_100_poss_player_stats_table(c1, 'PlayerSeasonStatsPer100Poss')
# create_adv_player_stats_table(c1, 'PlayerSeasonAdvStats')
# create_shooting_player_stats_table(c1, 'PlayerSeasonShootingStats')
# create_pbp_player_stats_table(c1, 'PlayerSeasonPBPStats')
# create_on_off_player_stats_table(c1, 'PlayerSeasonOnOffStats')

# *******     TEAM TABLE COLUMNS     ******* #

team_stats_cols = ['TeamID', 'Season', 'FG/G', 'FGA/G', 'FG%', '3P/G', '3PA/G', '3P%', '2P/G',
                   '2PA/G', '2P%', 'FT/G', 'FTA/G', 'FT%', 'ORB/G', 'DRB/G', 'TRB/G', 'AST/G', 'STL/G', 'BLK/G',
                   'TOV/G', 'PF/G', 'PTS/G', 'FG/GYOY%Change', 'FGA/GYOY%Change', 'FG%YOY%Change',
                   '3P/GYOY%Change', '3PA/GYOY%Change', '3P%YOY%Change',
                   '2P/GYOY%Change', '2PA/GYOY%Change', '2P%YOY%Change', 'FT/GYOY%Change', 'FTA/GYOY%Change',
                   'FT%YOY%Change', 'ORB/GYOY%Change', 'DRB/GYOY%Change',
                   'TRB/GYOY%Change', 'AST/GYOY%Change', 'STL/GYOY%Change', 'BLK/GYOY%Change', 'TOV/GYOY%Change',
                   'PF/GYOY%Change', 'PTS/GYOY%Change']

team_adv_cols = ['TeamID', 'Season', 'PythWins', 'PythLosses', 'MarginOfVictory', 'StrengthOfSchedule',
                 'SimpleRatingSystem', 'ORtg', 'DRtg', 'Pace', 'FTr', '3PAr', 'eFG%', 'TOV%', 'ORB%',
                 'FT/FGA', 'eFG%Against', 'TOV%Against', 'DRB%Against', 'FT/FGAAgainst']

team_lineups_cols = ['TeamID', 'Season', 'GameTypeID', 'NumPlayers', 'Player1ID',
                     'Player2ID', 'Player3ID', 'Player4ID', 'Player5ID', 'MP', '+/-FG/100Poss',
                     '+/-FGA/100Poss', '+/-FG%/100Poss', '+/-3P/100Poss', '+/-3PA/100Poss', '+/-3P%/100Poss',
                     '+/-eFG%/100Poss', '+/-FT/100Poss', '+/-FTA/100Poss', '+/-FT%/100Poss', '+/-PTS/100Poss',
                     '+/-ORB/100Poss', '+/-ORB%/100Poss', '+/-DRB/100Poss', '+/-DRB%/100Poss', '+/-TRB/100Poss',
                     '+/-TRB%/100Poss', '+/-AST/100Poss', '+/-STL/100Poss', '+/-BLK/100Poss', '+/-TOV/100Poss',
                     '+/-PF/100Poss']

team_gen_info_cols = ['TeamID', 'Season', 'Team', 'Games', 'Wins', 'Losses', 'Division', 'DivisionID',
                      'DivisionRank', 'Conference', 'ConferenceID', 'ConferenceRank',
                      'HeadCoach1', 'HeadCoach1Wins', 'HeadCoach1Losses', 'HeadCoach2', 'HeadCoach2Wins',
                      'HeadCoach2Losses', 'Round1Wins', 'Round1Losses', 'Round1Opp', 'Round1Won', 'Round2Wins',
                      'Round2Losses', 'Round2Opp', 'Round2Won', 'Round3Wins', 'Round3Losses', 'Round3Opp',
                      'Round3Won', 'Round4Wins', 'Round4Losses', 'Round4Opp', 'Round4Won']

# *******     PLAYER TABLE COLUMNS     ******* #

pbp_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'PG%', 'SG%', 'SF%',
            'PF%', 'C%', '+/-Per100PossOnCourt', '+/-Per100PossOn-Off', 'BadPassTO', 'LostBallTO',
            'OtherTO', 'ShootingFoulsCommitted', 'BlockingFoulsCommitted',
            'OffensiveFoulsCommitted', 'TakeFoulsCommitted', 'PtsGenByAst', 'ShootingFoulsDrawn',
            'And1s', 'BlockedFGA']

per_36_min_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'FG/36',
                   'FGA/36', 'FG%', '3P/36', '3PA/36', '3P%', '2P/36', '2PA/36', '2P%',
                   'FT/36', 'FTA/36', 'FT%', 'ORB/36', 'DRB/36', 'TRB/36', 'AST/36',
                   'STL/36', 'BLK/36', 'TOV/36', 'PF/36', 'PTS/36']

per_100_poss_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'FG/100Poss', 'FGA/100Poss',
                     'FG%', '3P/100Poss', '3PA/100Poss', '3P%', '2P/100Poss', '2PA/100Poss', '2P%',
                     'FT/100Poss', 'FTA/100Poss', 'FT%', 'ORB/100Poss', 'DRB/100Poss', 'TRB/100Poss',
                     'AST/100Poss', 'STL/100Poss', 'BLK/100Poss', 'TOV/100Poss', 'PF/100Poss',
                     'PTS/100Poss', 'ORtg/100Poss', 'DRtg/100Poss']

shooting_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'FG%', 'AvgShotDist',
                 '2PA%', '%FGA0-2ft', '%FGA3-9ft', '%FGA10-15ft', '%FGA16+ft<3', '%FGA3P', '2PFG%',
                 'FG%0-2ft', 'FG%3-9ft', 'FG%10-15ft', 'FG%16+ft<3', '3PFG%', '%2PAAstByOthers',
                 '%3PAAstByOthers', '%3PAFromCorner', '3P%FromCorner']

# *******     GAME TABLE COLUMNS     ******* #

team_game_stats_cols = ['GameID', 'Season', 'Date', 'GameTypeID', 'Team',
                        'TeamID', 'Q1Score', 'Q2Score', 'Q3Score', 'Q4Score',
                        'OTScore', 'FinalScore', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                        'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV',
                        'PF', 'TS%', 'eFG%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%',
                        'STL%', 'BLK%', 'TOV%', 'ORtg', 'DRtg']

player_stats_cols = ['GameID', 'PlayerID', 'PlayerLink', 'TeamID', 'MP', 'FG',
                     'FGA', 'FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB',
                     'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', '+/-', 'TS%', 'eFG%', '3PAr', 'FTr',
                     'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%', 'ORtg', 'DRtg']


def open_db(db_file):
    conn = lite.connect(db_file)
    print('Opened database in file {} successfully'.format(db_file))
    return conn


def populate_team_tables(c):
    print('Populating General Team Info Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_gen_info.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = team_gen_info_cols
    df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    df.to_sql(name='GeneralTeamSeasonInfo', con=c, if_exists='append')

    print('Populating Advanced Team Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_adv_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = team_adv_cols
    df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    df.to_sql(name='TeamSeasonAdvStats', con=c, if_exists='append')

    print('Populating Team Lineups Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_lineups.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = team_lineups_cols
    df.to_sql(name='TeamSeasonLineupStats', con=c, if_exists='append', index=False)

    print('Populating Team Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = team_stats_cols
    df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    df.to_sql(name='TeamSeasonStats', con=c, if_exists='append')

    print('Populating Team Opponent Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_opp_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = team_stats_cols
    df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    df.to_sql(name='TeamOppSeasonStats', con=c, if_exists='append')


def populate_player_tables(c):
    print('Populating General Player Info Table')
    df = pd.read_excel('GeneratedSpreadsheets\general_player_info.xlsx', na_values=['N/A'], convert_float=True)
    df.set_index('PlayerID', inplace=True, verify_integrity=True)
    df.to_sql(name='GeneralPlayerInfo', con=c, if_exists='append')

    print('Populating Per Game Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\per_game_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonStatsPerGame', con=c, if_exists='append', index=False)

    print('Populating Adv Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\\adv_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonAdvStats', con=c, if_exists='append', index=False)

    print('Populating On Off Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\on_off_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonOnOffStats', con=c, if_exists='append', index=False)

    print('Populating PBP Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\pbp_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df = df[df.Season != 2017]
    df.columns = pbp_cols
    df.to_sql(name='PlayerSeasonPBPStats', con=c, if_exists='append', index=False)

    print('Populating Per 36 Minutes Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\per_36_min_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df = df[df.Season != 2017]
    df.columns = per_36_min_cols
    df.to_sql(name='PlayerSeasonStatsPer36Minutes', con=c, if_exists='append', index=False)

    print('Populating Per 100 Possession Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\per_100_poss_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df = df[df.Season != 2017]
    df.columns = per_100_poss_cols
    df.to_sql(name='PlayerSeasonStatsPer100Poss', con=c, if_exists='append', index=False)

    print('Populating Player Shooting Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\player_shooting_stats.xlsx', na_values=['N/A'], convert_float=True)
    df = df[df.Season != 2017]
    df.columns = shooting_cols
    df.to_sql(name='PlayerSeasonShootingStats', con=c, if_exists='append', index=False)


def populate_game_tables(c):
    print('Populating Player and Team Game Stats')
    for year in range(2001, 2017):
        name = 'GeneratedSpreadsheets\game_stats_' + str(year) + '.xlsx'
        print(name)
        df_team = pd.read_excel(io=name, sheetname='Sheet1', na_values=['N/A'], convert_float=True)
        df_team.drop('GameType', axis=1, inplace=True)
        df_team.to_sql(name='TeamGameStats', con=c, if_exists='append', index=False)
        df_player = pd.read_excel(io=name, sheetname='Sheet2', na_values=['N/A'], convert_float=True)
        df_player.drop('Player', axis=1, inplace=True)
        df_player.drop('Team', axis=1, inplace=True)
        df_player.to_sql(name='PlayerGameStats', con=c, if_exists='append', index=False)


def main():
    c = open_db('Database\\test.db')
    populate_player_tables(c)
    populate_team_tables(c)
    populate_game_tables(c)
    c.commit()
    c.close()


if __name__ == "__main__":
    main()
