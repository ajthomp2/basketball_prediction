import pandas as pd
import table_column_names


def combine_tables():
    df_all_players = pd.DataFrame(columns=table_column_names.player_stats_cols)
    df_all_teams = pd.DataFrame(columns=table_column_names.team_game_stats_cols)

    for years in ['01_02', '03_04', '05_06', '07_08', '09_10', '11_12', '13_14', '15_16']:
        name = 'GeneratedSpreadsheets\game_stats_' + years + '.xlsx'
        print(name)

        df_team = pd.read_excel(io=name, sheetname='Sheet1', na_values=['N/A'], convert_float=True)
        df_team.columns = table_column_names.team_game_stats_cols
        df_all_teams = df_all_teams.append(df_team, ignore_index=True)

        df_player = pd.read_excel(io=name, sheetname='Sheet2', na_values=['N/A'], convert_float=True)
        df_player.columns = table_column_names.player_stats_cols
        df_all_players = df_all_players.append(df_player, ignore_index=True)

    writer = pd.ExcelWriter('game_stats_2.xlsx')
    df_all_teams.to_excel(writer, 'Sheet1')
    df_all_players.to_excel(writer, 'Sheet2')
    writer.close()


def match_pid():
    df_gen_info = pd.read_excel(io='GeneratedSpreadsheets\general_player_info.xlsx', sheetname='Sheet1',
                                na_values=['N/A'], convert_float=True)
    df_player_game_stats = pd.read_excel(io='GeneratedSpreadsheets\game_stats.xlsx', sheetname='Sheet2',
                                         na_values=['N/A'], convert_float=True)

    i = 0
    total = len(df_player_game_stats.index)
    for index, row in df_player_game_stats.iterrows():
        df_player_game_stats.set_value(index, 'PlayerID',
                                       df_gen_info.loc[df_gen_info['PlayerLink'] == row['PlayerLink']]['PlayerID'])
        i += 1
        print('{0}/{1}'.format(i, total))

    writer = pd.ExcelWriter('game_stats_new.xlsx')
    df_player_game_stats.to_excel(writer, 'Sheet1')
    writer.close()


def fill_missing_plusminus_rows():
    df_player_game_stats = pd.read_excel(io='GeneratedSpreadsheets\game_stats.xlsx', sheetname='Sheet2',
                                         na_values=['N/A'], convert_float=True)

    idx = []
    for index, row in df_player_game_stats.iterrows():
        if not pd.notnull(row['DRtg']):
            new_row = row
            adv_stats = row[22:-1]
            # print('ADV STATS: ', adv_stats)
            new_row[23:] = adv_stats
            new_row[23] = 'N/A'
            # print('NEW ROW: ', new_row)
            df_player_game_stats.iloc[index] = new_row
            # print('NEW ROW IN DF: ', df_player_game_stats.iloc[index])

    writer = pd.ExcelWriter('game_stats_new.xlsx')
    df_player_game_stats.to_excel(writer, 'Sheet1')
    writer.close()


def main():
    combine_tables()
    # match_pid()
    # fill_missing_plusminus_rows()


if __name__ == "__main__":
    main()