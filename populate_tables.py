import pandas as pd
import pymysql as sql
import rds_config
from sqlalchemy import create_engine
import table_column_names

# ******************************************************** #
# ******************************************************** #
# NOTES:
#
#
# ******************************************************** #
# ******************************************************** #


def open_db():
    print("Creating DB Connection")
    sql.install_as_MySQLdb()
    rds_host = rds_config.db_endpoint
    name = rds_config.db_username
    password = rds_config.db_password
    db_name = rds_config.db_name

    conn_str = "mysql+pymysql://{nm}:{pswrd}@{host}:3306/{dbnm}"\
        .format(nm=name, pswrd=password, host=rds_host, dbnm=db_name)
    engine = create_engine(conn_str)

    print("Connection Successful")

    return engine


def populate_team_tables(c):
    print('Populating General Team Info Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_gen_info.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.team_gen_info_cols
    df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    df.to_sql(name='GeneralTeamSeasonInfo', con=c, if_exists='append', chunksize=100)

    print('Populating Advanced Team Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_adv_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.drop('Team', axis=1, inplace=True)
    df.columns = table_column_names.team_adv_cols
    df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    df.to_sql(name='TeamSeasonAdvStats', con=c, if_exists='append', chunksize=100)

    print('Populating Team Lineups Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_lineups.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.team_lineups_cols
    df.to_sql(name='TeamSeasonLineupStats', con=c, if_exists='append', chunksize=100, index=False)

    print('Populating Team Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.drop('Team', axis=1, inplace=True)
    df.columns = table_column_names.team_stats_cols
    df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    df.to_sql(name='TeamSeasonStats', con=c, if_exists='append', chunksize=100)

    print('Populating Team Opponent Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\\team_opp_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.drop('Team', axis=1, inplace=True)
    df.columns = table_column_names.team_stats_cols
    df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    df.to_sql(name='TeamOppSeasonStats', con=c, if_exists='append', chunksize=100)


def populate_player_tables(c):
    print('Populating General Player Info Table')
    df = pd.read_excel('GeneratedSpreadsheets\general_player_info.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.general_player_info_cols
    df.set_index('PlayerID', inplace=True, verify_integrity=True)
    df.to_sql(name='GeneralPlayerInfo', con=c, if_exists='append', chunksize=100)

    print('Populating Per Game Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\per_game_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.per_game_cols
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonStatsPerGame', con=c, if_exists='append', chunksize=100, index=False)

    print('Populating Adv Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\\adv_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.adv_cols
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonAdvStats', con=c, if_exists='append', chunksize=100, index=False)

    print('Populating On Off Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\on_off_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.on_off_cols
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonOnOffStats', con=c, if_exists='append', chunksize=100, index=False)

    print('Populating PBP Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\pbp_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.pbp_cols
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonPBPStats', con=c, if_exists='append', chunksize=100, index=False)

    print('Populating Per 36 Minutes Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\per_36_min_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.per_36_min_cols
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonStatsPer36Minutes', con=c, if_exists='append', chunksize=100, index=False)

    print('Populating Per 100 Possession Player Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\per_100_poss_player_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.per_100_poss_cols
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonStatsPer100Poss', con=c, if_exists='append', chunksize=100, index=False)

    print('Populating Player Shooting Stats Table')
    df = pd.read_excel('GeneratedSpreadsheets\player_shooting_stats.xlsx', na_values=['N/A'], convert_float=True)
    df.columns = table_column_names.shooting_cols
    df = df[df.Season != 2017]
    df.to_sql(name='PlayerSeasonShootingStats', con=c, if_exists='append', chunksize=100, index=False)


def populate_game_tables(c):
    print('Populating Player and Team Game Stats')

    df_team = pd.read_excel(io='GeneratedSpreadsheets\game_stats.xlsx', sheetname='Sheet1',
                            na_values=['N/A'], convert_float=True)
    df_team.columns = table_column_names.team_game_stats_cols
    df_team.to_sql(name='TeamGameStats', con=c, if_exists='append', chunksize=100, index=False)

    df_player = pd.read_excel(io='GeneratedSpreadsheets\game_stats.xlsx', sheetname='Sheet2',
                              na_values=['N/A'], convert_float=True)
    df_player.columns = table_column_names.player_stats_cols
    df_player.to_sql(name='PlayerGameStats', con=c, if_exists='append', chunksize=100, index=False)


def main():
    c = open_db()
    populate_player_tables(c)
    populate_team_tables(c)
    populate_game_tables(c)


if __name__ == "__main__":
    main()
