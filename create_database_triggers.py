import sqlite3 as lite

# ******************************************************** #
# ******************************************************** #
# NOTES:
#   -
# ******************************************************** #
# ******************************************************** #


# create_player_game_stats_table(c1, 'PlayerGameStatsYTD')
# create_team_game_stats_table(c1, 'TeamGameStatsYTD')
# create_general_team_info_table(c1, 'GeneralTeamSeasonInfoYTD')
# create_team_season_stats_table(c1, 'TeamSeasonStatsYTD')
# create_team_season_stats_table(c1, 'TeamOppSeasonStatsYTD')
# create_team_season_lineup_stats_table(c1, 'TeamSeasonLineupStatsYTD')
# create_team_season_adv_stats_table(c1, 'TeamSeasonAdvStatsYTD')
# create_general_player_info_table(c1, 'GeneralPlayerInfoYTD')
# create_per_game_player_stats_table(c1, 'PlayerSeasonStatsPerGameYTD')
# create_per_36_min_player_stats_table(c1, 'PlayerSeasonStatsPer36MinutesYTD')
# create_per_100_poss_player_stats_table(c1, 'PlayerSeasonStatsPer100PossYTD')
# create_adv_player_stats_table(c1, 'PlayerSeasonAdvStatsYTD')
# create_shooting_player_stats_table(c1, 'PlayerSeasonShootingStatsYTD')
# create_pbp_player_stats_table(c1, 'PlayerSeasonPBPStatsYTD')
# create_on_off_player_stats_table(c1, 'PlayerSeasonOnOffStatsYTD')


def open_db(db_file):
    conn = lite.connect(db_file)
    print('Opened database in file {} successfully'.format(db_file))
    return conn


def create_team_game_stats_trigger(c):
    c.execute('''
        CREATE TRIGGER AfterInsertGameStats
        AFTER INSERT ON TeamGameStatsYTD
        BEGIN
            INSERT INTO TableGameInsertLog
            (
                InsertDate,
                StartingGameDate,
                EndingGameDate,
                StartingGameID,
                EndingGameID,
                TotalGames
            )
            VALUES
            (
                CURRENT_DATE,
                MIN(NEW.Date),
                MAX(NEW.Date),
                MIN(NEW.GameID),
                MAX(NEW.GameID),
                COUNT(NEW.GameID)
            );
        END;
    ''')


def create_triggers():
    print('Starting...')
    conn = open_db('Database\\test.db')

    c = conn.cursor()

    create_team_game_stats_trigger(c)

    conn.commit()
    conn.close()


def main():
    create_triggers()


if __name__ == "__main__":
    main()