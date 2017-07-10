from bs4 import BeautifulSoup
from bs4 import Comment
import pandas as pd
from urllib.request import urlopen
from urllib.error import HTTPError
import pymysql as sql
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
import smtplib
import team_names_and_ids
import table_column_names
import gen_bball_info
import email_config
import rds_config

# **************************************************************************************************************** #
# **************************************************************************************************************** #
# NOTES:
#
# **************************************************************************************************************** #
# **************************************************************************************************************** #


# **************************************************************************************************************** #
# **************************************************************************************************************** #
#
#                                   GLOBAL VARIABLES
#
# **************************************************************************************************************** #
# **************************************************************************************************************** #

player_links = []
gen_info_player_links = []
player_link_to_player_id = {}
next_p_id = 1
next_game_id = 1
last_date = datetime.today()


# **************************************************************************************************************** #
# **************************************************************************************************************** #
#
#                                   UTILITY FUNCTIONS
#
# **************************************************************************************************************** #
# **************************************************************************************************************** #


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


def populate_playerlink_to_playerid_dict(c):
    result = c.execute("SELECT PlayerLink, PlayerID FROM GeneralPlayerInfo")
    result = result.fetchall()

    global player_link_to_player_id
    player_link_to_player_id = dict(result)


def get_player_links_from_gen_info(c):
    result = c.execute("SELECT PlayerLink FROM GeneralPlayerInfo")
    result = result.fetchall()
    global gen_info_player_links
    gen_info_player_links = [row[0] for row in result]


def set_next_p_id(c):
    result = c.execute("SELECT MAX(PlayerID) FROM GeneralPlayerInfo")
    result = result.fetchall()
    global next_p_id
    next_p_id = int(result[0][0]) + 1


def set_next_game_id(c):
    cur_year_result = c.execute("SELECT MAX(GameID) FROM TeamGameStatsYTD")
    cur_year_result = cur_year_result.fetchall()
    last_year_result = c.execute("SELECT MAX(GameID) FROM TeamGameStats")
    last_year_result = last_year_result.fetchall()
    global next_game_id
    if cur_year_result[0][0]:
        next_game_id = int(cur_year_result[0][0]) + 1
    else:
        next_game_id = int(last_year_result[0][0]) + 1


def get_last_date_updated(c):
    result = c.execute("""SELECT Date
                 FROM TeamGameStatsYTD""")
    result = result.fetchall()
    # dates = [datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S") for row in result]
    dates = [row[0] for row in result]
    global last_date
    if dates:
        last_date = max(dates)
    else:
        last_date = datetime.min


def send_confirmation_email():
    gmail_user = 'bballprediction@gmail.com'
    gmail_pswrd = email_config.password

    _from = gmail_user
    to = ['alex.thompson6@gmail.com']
    subject = 'Update Successful'
    body = 'Database Updated Successfully!'

    email_text = """
    From: {f}
    To: {to}
    Subject: {sub}

    {body}
    """.format(f=_from, to=', '.join(to), sub=subject, body=body)

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_pswrd)
        server.sendmail(_from, to, email_text)
        server.close()

        print('Email Sent!')
    except Exception:
        print('Something went wrong...')


def send_error_email():
    gmail_user = 'bballprediction@gmail.com'
    gmail_pswrd = email_config.password

    _from = gmail_user
    to = ['alex.thompson6@gmail.com']
    subject = 'Update Service Error'
    body = 'Error in database update service.  Please check service errors'

    email_text = """
    From: {f}
    To: {to}
    Subject: {sub}

    {body}
    """.format(f=_from, to=', '.join(to), sub=subject, body=body)

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_pswrd)
        server.sendmail(_from, to, email_text)
        server.close()

        print('Error Email Sent!')
    except Exception:
        print('Something went wrong...')


# **************************************************************************************************************** #
# **************************************************************************************************************** #
#
#                                   SCRAPE PLAYER STATS
#
# **************************************************************************************************************** #
# **************************************************************************************************************** #


def scrape_general_player_info(soup, link):
    name = soup.h1.get_text().strip()
    draft = 0
    manualentry = False

    rows = [p for p in soup.find_all('p')]
    for i, row in enumerate(rows):
        if len(row.get_text().strip().encode('utf-8')) > 300:
            print('Player Name: ', name)
            print('Please Enter Following Data...')
            p1 = input('Position 1 (PG, SG, SF, PF, or C): ')
            p2 = input('Position 2 (PG, SG, SF, PF, C, or N/A): ')
            p3 = input('Position 3 (PG, SG, SF, PF, C, or N/A): ')
            height = input('Height (ft-in, ex: 6-7): ')
            weight = int(input('Weight (in lbs): '))
            college = input('College: ')
            manualentry = True
            continue
        if 'Draft:' in row.get_text():
            if row.get_text().strip().split(':')[1].strip() == '':
                continue
            temp_draft = row.get_text().strip().split('(')[1].split(')')[0].split(',')[1].strip().split()[0]
            temp_draft = temp_draft.replace('st', '').replace('nd', '').replace('rd', '').replace('th', '')
            draft = int(temp_draft)
        elif 'Born:' in row.get_text():
            born = pd.to_datetime(row.span.get('data-birth'))
        elif 'Position:' in row.get_text():
            positions = row.get_text().split('and')
            num_pos = len(positions)
            if num_pos == 3:
                p1, p2, p3 = ' '.join(positions[0].strip().split()[-2:]).strip(), positions[1].strip(), \
                             ' '.join(positions[2].strip().split()[:2]).strip()
                if 'Center' in p1:
                    p1 = 'Center'
                elif 'Center' in p2:
                    p2 = 'Center'
                elif 'Center' in p3:
                    p3 = 'Center'
            elif num_pos == 2:
                p1, p2 = ' '.join(positions[0].strip().split()[-2:]).strip(), \
                         ' '.join(positions[1].strip().split()[:2]).strip()
                if 'Center' in p1:
                    p1 = 'Center'
                elif 'Center' in p2:
                    p2 = 'Center'
                p3 = np.nan
            else:
                p1 = ' '.join(positions[0].strip().split()[1:3]).strip()
                if 'Center' in p1:
                    p1 = 'Center'
                elif 'Guard' in p1 and 'Point' not in p1 and 'Shooting' not in p1:
                    p1 = 'Shooting Guard'
                elif 'Forward' in p1 and 'Small' not in p1 and 'Power' not in p1:
                    p1 = 'Small Forward'
                p2, p3 = np.nan, np.nan
            p1, p2, p3 = gen_bball_info.positions_to_abbr[p1], gen_bball_info.positions_to_abbr[p2],\
                         gen_bball_info.positions_to_abbr[p3]

            height, weight = rows[i + 1].get_text().strip().split('(')[0].split(',')
            weight = int(weight.strip().replace('lb', ''))

    if not manualentry:
        for row in rows:
            if 'College:' in row.get_text():
                college = row.get_text().strip().split(':')[1].strip()
                break
        else:
            college = np.nan

    if draft == 0:
        draft = np.nan

    # print([player_link_to_player_id[link], name, link, born, p1, p2, p3, draft, height, weight, college])

    return [player_link_to_player_id[link], name, link, born,
            p1, p2, p3, draft, height, weight, college]


def scrape_per_game_stats(soup, link, game_type):
    int_ind = [0, 4, 5]
    skip_ind = [2, 3, 4, 5]
    per_game_stats = []

    for tr in soup.tbody.find_all('tr'):
        if tr.th and tr.th.a:
            season = int(tr.th.a.get('href').strip().split('/')[-2])
            per_game_stats_season = [player_link_to_player_id[link], season, game_type]
        else:
            continue
        if season == 2017:
            for i, td in enumerate(tr.find_all('td')):
                if i in skip_ind:
                    if i == 2 and td.get_text().strip() != 'NBA':
                        break
                    continue
                elif i == 1:
                    team = td.get_text().strip()
                    if team == 'TOT':
                        break
                    per_game_stats_season.extend([team_names_and_ids.abbr_to_team_id[team]])
                elif i in int_ind:
                    if td.get_text().strip() != '':
                        per_game_stats_season.append(int(td.get_text().strip()))
                    else:
                        per_game_stats_season.append(0)
                else:
                    if td.get_text().strip() != '':
                        per_game_stats_season.append(float(td.get_text().strip()))
                    else:
                        per_game_stats_season.append(float(0))
            if len(per_game_stats_season) < 10:
                continue
            # print(per_game_stats_season)
            per_game_stats.append(per_game_stats_season)

    # print(per_game_stats)

    return per_game_stats


def scrape_stats(soup, link, ind, game_type, skip_ind=(2, 3, 4, 5, 6), int_ind=[0]):
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    comments = [comment for comment in comments if len(comment) > 5000]

    # for players that haven't played in a game yet
    if len(comments) < 2:
        return []

    # totals included on page vs not included on page
    if BeautifulSoup(comments[1], 'lxml').find('div', id='div_totals'):
        if len(comments) > ind + 1:
            soup = BeautifulSoup(comments[ind+1], 'lxml')
        else:
            return []
    else:
        if len(comments) > ind:
            soup = BeautifulSoup(comments[ind], 'lxml')
        else:
            return []

    stats = []
    for tr in soup.tbody.find_all('tr'):
        if tr.th and tr.th.a:
            season = int(tr.th.a.get('href').strip().split('/')[-2])
            stats_seasons = [player_link_to_player_id[link], season, game_type]
        else:
            continue
        if season == 2017:
            for i, td in enumerate(tr.find_all('td')):
                if i in skip_ind:
                    if i == 2 and td.get_text().strip() != 'NBA':
                        break
                    continue
                elif i in int_ind:
                    if td.get_text().strip() != '':
                        if '%' in td.get_text().strip():
                            stats_seasons.append(int(td.get_text().strip().rstrip('%')))
                        else:
                            stats_seasons.append(int(td.get_text().strip()))
                    else:
                        stats_seasons.append(0)
                elif i == 1:
                    team = td.get_text().strip()
                    if team == 'TOT':
                        break
                    stats_seasons.extend([team_names_and_ids.abbr_to_team_id[team]])
                else:
                    if td.get_text().strip() != '':
                        stats_seasons.append(float(td.get_text().strip()))
                    else:
                        stats_seasons.append(float(0))
            if len(stats_seasons) < 10:
                continue
            # print(stats_seasons)
            stats.append(stats_seasons)

        # print(stats)

    return stats


def scrape_on_off_stats(soup, link, gametype, season):
    if not soup:
        return []

    num_teams = len(soup.find_all('tr', class_='over_header thead'))
    on_off_stats = []

    for i in range(num_teams):
        team_on_off_stats = [player_link_to_player_id[link], season, gametype]
        for j, tr in enumerate(soup.find_all('tr')[5*i+2:5*i+5]):
            for k, td in enumerate(tr.find_all('td')):
                if j == 0 and k == 0:
                    team = td.a.get_text().strip()
                    team_on_off_stats.extend([team_names_and_ids.abbr_to_team_id[team]])
                elif k == 1 or (k == 0 and j != 0):
                    continue
                else:
                    if td.get_text().strip() != '':
                        team_on_off_stats.append(float(td.get_text().strip()))
                    else:
                        team_on_off_stats.append(float(0))
        on_off_stats.append(team_on_off_stats)

    return on_off_stats


def delete_player_stats_table_rows(c, tablename, playerid, season, gametypeid, teamid):
    c.execute("""DELETE FROM {tn}
                 WHERE PlayerID = {pid} AND
                 Season = {s} AND
                 GameTypeID = {gtid} AND
                 TeamID = {tid};
    """.format(tn=tablename, pid=playerid, s=season, gtid=gametypeid, tid=teamid))


def scrape_and_update_player_stats(c, con):
    base_addr = 'http://www.basketball-reference.com'
    general_info, per_36_min_stats, per_100_pos_stats, adv_stats = [], [], [], []
    per_game_stats, shooting_stats, pbp_stats, on_off_stats = [], [], [], []

    global player_links
    count = 1
    total = len(player_links)
    for link in player_links:
        print("{}/{}".format(count, total))
        addr = base_addr + link
        print(addr)
        try:
            resp = urlopen(addr)
        except HTTPError as e:
            print(e.code)
            continue
        soup = BeautifulSoup(resp.read(), 'html.parser')

        if link not in gen_info_player_links:
            general_info.append(scrape_general_player_info(soup.find(id='info'), link))
        if soup.find(id='per_game'):
            per_game_stats += scrape_per_game_stats(soup.find(id='per_game'), link, 1)
        per_36_min_stats += scrape_stats(soup, link, 1, 1)
        per_100_pos_stats += scrape_stats(soup, link, 2, 1, skip_ind=(2, 3, 4, 5, 6, 28))
        adv_stats += scrape_stats(soup, link, 3, 1, skip_ind=(2, 3, 4, 5, 18, 23))
        shooting_stats += scrape_stats(soup, link, 4, 1, skip_ind=(2, 3, 4, 5, 21, 22, 26, 27))
        pbp_stats += scrape_stats(soup, link, 5, 1, skip_ind=(2, 3, 4, 5),
                                  int_ind=[0, 6, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23])

        # ******    COMMENTED OUT SECTION IS FOR THE PLAYOFFS, SO WILL NOT BE INCLUDED DURING REGULAR SEASON    ****** #

        # if soup.find('div', id='all_playoffs_per_game'):
        #     #pass
        #     per_game_stats += scrape_stats(soup, link, 6, 2, skip_ind=(2, 3, 4, 5))
        #     #per_36_min_stats += scrape_stats(soup, link, 8, 2)
        #     #per_100_pos_stats += scrape_stats(soup, link, 9, 2, skip_ind=(2, 3, 4, 5, 6, 28))
        #     #adv_stats += scrape_stats(soup, link, 10, 2, skip_ind=(2, 3, 4, 5, 18, 23))
        # if soup.find('div', id='all_playoffs_shooting'):
        #     pass
        #     shooting_stats += scrape_stats(soup, link, 11, 2,
        #                                           skip_ind=(2, 3, 4, 5, 21, 22, 26, 27))
        #     pbp_stats += scrape_stats(soup, link, 12, 2, skip_ind=(2, 3, 4, 5),
        #                int_ind=[0,6,7,8,9,10,13,14,15,16,17,18,19,20,21,22,23])

        on_off_addr = base_addr + link.rstrip('.html') + '/on-off/2017'
        print(on_off_addr)
        try:
            resp = urlopen(on_off_addr)
        except HTTPError as e:
            print(e.code)
            continue
        soup = BeautifulSoup(resp.read(), 'html.parser')

        on_off_stats += scrape_on_off_stats(soup.find('table', id='on-off'), link, 1, 2017)
        # if soup.find('table', id='on-off-post'):
        #     on_off_stats += scrape_on_off_stats(soup.find('table', id='on-off-post'),
        #                                                link, 2, 2017)

        count += 1

    if general_info:
        general_info_df = pd.DataFrame(data=general_info, columns=table_column_names.general_player_info_cols)
        general_info_df.set_index('PlayerID', inplace=True, verify_integrity=True)
        general_info_df.to_sql(name='GeneralPlayerInfo', con=c, if_exists='append')

    per_game_stats_df = pd.DataFrame(data=per_game_stats, columns=table_column_names.per_game_cols)
    for ind, row in per_game_stats_df.iterrows():
        delete_player_stats_table_rows(c, 'PlayerSeasonStatsPerGameYTD', row['PlayerID'], row['Season'],
                                       row['GameTypeID'], row['TeamID'])
    per_game_stats_df.to_sql(name='PlayerSeasonStatsPerGameYTD', con=con, if_exists='append', index=False)

    per_36_mins_df = pd.DataFrame(data=per_36_min_stats, columns=table_column_names.per_36_min_cols)
    for ind, row in per_36_mins_df.iterrows():
        delete_player_stats_table_rows(c, 'PlayerSeasonStatsPer36MinutesYTD', row['PlayerID'], row['Season'],
                                       row['GameTypeID'], row['TeamID'])
    per_36_mins_df.to_sql(name='PlayerSeasonStatsPer36MinutesYTD', con=con, if_exists='append', index=False)

    per_100_pos_df = pd.DataFrame(data=per_100_pos_stats, columns=table_column_names.per_100_poss_cols)
    for ind, row in per_100_pos_df.iterrows():
        delete_player_stats_table_rows(c, 'PlayerSeasonStatsPer100PossYTD', row['PlayerID'], row['Season'],
                                       row['GameTypeID'], row['TeamID'])
    per_100_pos_df.to_sql(name='PlayerSeasonStatsPer100PossYTD', con=con, if_exists='append', index=False)

    adv_stats_df = pd.DataFrame(data=adv_stats, columns=table_column_names.adv_cols)
    for ind, row in adv_stats_df.iterrows():
        delete_player_stats_table_rows(c, 'PlayerSeasonAdvStatsYTD', row['PlayerID'], row['Season'],
                                       row['GameTypeID'], row['TeamID'])
    adv_stats_df.to_sql(name='PlayerSeasonAdvStatsYTD', con=con, if_exists='append', index=False)

    shooting_stats_df = pd.DataFrame(data=shooting_stats, columns=table_column_names.shooting_cols)
    for ind, row in shooting_stats_df.iterrows():
        delete_player_stats_table_rows(c, 'PlayerSeasonShootingStatsYTD', row['PlayerID'], row['Season'],
                                       row['GameTypeID'], row['TeamID'])
    shooting_stats_df.to_sql(name='PlayerSeasonShootingStatsYTD', con=con, if_exists='append', index=False)

    pbp_stats_df = pd.DataFrame(data=pbp_stats, columns=table_column_names.pbp_cols)
    for ind, row in pbp_stats_df.iterrows():
        delete_player_stats_table_rows(c, 'PlayerSeasonPBPStatsYTD', row['PlayerID'], row['Season'],
                                       row['GameTypeID'], row['TeamID'])
    pbp_stats_df.to_sql(name='PlayerSeasonPBPStatsYTD', con=con, if_exists='append', index=False)

    on_off_stats_df = pd.DataFrame(data=on_off_stats, columns=table_column_names.on_off_cols)
    for ind, row in on_off_stats_df.iterrows():
        delete_player_stats_table_rows(c, 'PlayerSeasonOnOffStatsYTD', row['PlayerID'], row['Season'],
                                       row['GameTypeID'], row['TeamID'])
    on_off_stats_df.to_sql(name='PlayerSeasonOnOffStatsYTD', con=con, if_exists='append', index=False)


# **************************************************************************************************************** #
# **************************************************************************************************************** #
#
#                                   SCRAPE TEAM STATS
#
# **************************************************************************************************************** #
# **************************************************************************************************************** #

def scrape_team_general_info(soup, team, year):
    general_info = [team, team_names_and_ids.abbr_to_team_id[team], year]
    offset = 0
    if 'Last Game:' in soup.get_text():
        offset = 1
    ps = soup.find_all('p')
    wins, losses = map(int, ps[2].get_text().split(':')[1].split(',')[0].strip().split('-'))
    general_info.extend([wins + losses, wins, losses])
    division_rank = int(ps[2].get_text().split('Finished')\
        [1].split(' in ')[0].split('th')[0].split('st')[0].split('nd')[0].split('rd')[0].strip())
    division = ps[2].get_text().split('NBA')[1].split()[0].strip()
    if division in ['Atlantic', 'Central', 'Southeast']:
        conference = 'Eastern'
        conference_id = gen_bball_info.conference_to_conf_id[conference]
    elif division in ['Northwest', 'Pacific', 'Southwest', 'Midwest']:
        conference = 'Western'
        conference_id = gen_bball_info.conference_to_conf_id[conference]
    if division in ['Eastern', 'Western']:
        conference = division
        conference_id = gen_bball_info.conference_to_conf_id[conference]
        conference_rank = division_rank
        division = 'N/A'
        division_id = 'N/A'
        division_rank = 'N/A'
    else:
        division_id = gen_bball_info.division_to_div_id[division]
        conference_rank = 'N/A'
    general_info.extend([division, division_id, division_rank,
                         conference, conference_id, conference_rank])

    coach_1 = ps[3 + offset].get_text().split(':')[1].split('(')[0].strip()
    coach_1_wins, coach_1_losses = map(int, ps[3 + offset].get_text().split('(')[1].split(')')[0].split('-'))
    if ',' in ps[3 + offset].get_text():
        coach_2 = ps[3 + offset].get_text().split(',')[1].split('(')[0].strip()
        coach_2_wins, coach_2_losses = map(int, ps[3 + offset].get_text().split(',')[1].split('(')[1].split(')')[0].split('-'))
    else:
        coach_2, coach_2_wins, coach_2_losses = 'N/A', 'N/A', 'N/A'
    general_info.extend([coach_1, coach_1_wins, coach_1_losses,
                         coach_2, coach_2_wins, coach_2_losses])

    if len(ps) == 10 + offset:
        playoffs = ps[9 + offset].get_text().split('(Series Stats)')[:-1]
        if len(playoffs) == 4:
            finals_wins, finals_losses = map(int, playoffs[0].split('(')[1].split(')')[0].split('-'))
            finals_opp = team_names_and_ids.teams_to_abbr[playoffs[0].split('versus')[1].strip()]
            won_finals = finals_wins > finals_losses
            conf_finals_wins, conf_finals_losses = map(int, playoffs[1].split('(')[1].split(')')[0].split('-'))
            conf_finals_opp = team_names_and_ids.teams_to_abbr[playoffs[1].split('versus')[1].strip()]
            won_conf_finals = True
            conf_semis_wins, conf_semis_losses = map(int, playoffs[2].split('(')[1].split(')')[0].split('-'))
            conf_semis_opp = team_names_and_ids.teams_to_abbr[playoffs[2].split('versus')[1].strip()]
            won_conf_semis = True
            first_round_wins, first_round_losses = map(int, playoffs[3].split('(')[1].split(')')[0].split('-'))
            first_round_opp = team_names_and_ids.teams_to_abbr[playoffs[3].split('versus')[1].strip()]
            won_first_round = True
        elif len(playoffs) == 3:
            finals_wins, finals_losses = 'N/A', 'N/A'
            finals_opp = 'N/A'
            won_finals = 'N/A'
            conf_finals_wins, conf_finals_losses = map(int, playoffs[0].split('(')[1].split(')')[0].split('-'))
            conf_finals_opp = team_names_and_ids.teams_to_abbr[playoffs[0].split('versus')[1].strip()]
            won_conf_finals = conf_finals_wins > conf_finals_losses
            conf_semis_wins, conf_semis_losses = map(int, playoffs[1].split('(')[1].split(')')[0].split('-'))
            conf_semis_opp = team_names_and_ids.teams_to_abbr[playoffs[1].split('versus')[1].strip()]
            won_conf_semis = True
            first_round_wins, first_round_losses = map(int, playoffs[2].split('(')[1].split(')')[0].split('-'))
            first_round_opp = team_names_and_ids.teams_to_abbr[playoffs[2].split('versus')[1].strip()]
            won_first_round = True
        elif len(playoffs) == 2:
            finals_wins, finals_losses = 'N/A', 'N/A'
            finals_opp = 'N/A'
            won_finals = 'N/A'
            conf_finals_wins, conf_finals_losses = 'N/A', 'N/A'
            conf_finals_opp = 'N/A'
            won_conf_finals = 'N/A'
            conf_semis_wins, conf_semis_losses = map(int, playoffs[0].split('(')[1].split(')')[0].split('-'))
            conf_semis_opp = team_names_and_ids.teams_to_abbr[playoffs[0].split('versus')[1].strip()]
            won_conf_semis = conf_semis_wins > conf_semis_losses
            first_round_wins, first_round_losses = map(int, playoffs[1].split('(')[1].split(')')[0].split('-'))
            first_round_opp = team_names_and_ids.teams_to_abbr[playoffs[1].split('versus')[1].strip()]
            won_first_round = True
        else:
            finals_wins, finals_losses = 'N/A', 'N/A'
            finals_opp = 'N/A'
            won_finals = 'N/A'
            conf_finals_wins, conf_finals_losses = 'N/A', 'N/A'
            conf_finals_opp = 'N/A'
            won_conf_finals = 'N/A'
            conf_semis_wins, conf_semis_losses = 'N/A', 'N/A'
            conf_semis_opp = 'N/A'
            won_conf_semis = 'N/A'
            first_round_wins, first_round_losses = map(int, playoffs[0].split('(')[1].split(')')[0].split('-'))
            first_round_opp = team_names_and_ids.teams_to_abbr[playoffs[0].split('versus')[1].strip()]
            won_first_round = first_round_wins > first_round_losses
    else:
        finals_wins, finals_losses = 'N/A', 'N/A'
        finals_opp = 'N/A'
        won_finals = 'N/A'
        conf_finals_wins, conf_finals_losses = 'N/A', 'N/A'
        conf_finals_opp = 'N/A'
        won_conf_finals = 'N/A'
        conf_semis_wins, conf_semis_losses = 'N/A', 'N/A'
        conf_semis_opp = 'N/A'
        won_conf_semis = 'N/A'
        first_round_wins, first_round_losses = 'N/A', 'N/A'
        first_round_opp = 'N/A'
        won_first_round = 'N/A'

    general_info.extend([first_round_wins, first_round_losses, first_round_opp, won_first_round,
                         conf_semis_wins, conf_semis_losses, conf_semis_opp, won_conf_semis,
                         conf_finals_wins, conf_finals_losses, conf_finals_opp, won_conf_finals,
                         finals_wins, finals_losses, finals_opp, won_finals])
    #print(general_info)

    return general_info


def scrape_team_statistics(soup, team, season):
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    comments = [comment for comment in comments if len(comment) > 200]

    if BeautifulSoup(comments[0], 'lxml').table.has_attr('id')\
            and BeautifulSoup(comments[0], 'lxml').table['id'] == 'injury':
        team_stats, team_yoy_improvement = \
            scrape_team_stats(BeautifulSoup(comments[2], 'lxml'))

        opp_stats, opp_yoy_improvement = \
            scrape_opp_stats(BeautifulSoup(comments[2], 'lxml'))

        team_adv_stats = scrape_team_adv_stats(BeautifulSoup(comments[3], 'lxml'), season)
    else:
        team_stats, team_yoy_improvement = \
            scrape_team_stats(BeautifulSoup(comments[1], 'lxml'))

        opp_stats, opp_yoy_improvement = \
            scrape_opp_stats(BeautifulSoup(comments[1], 'lxml'))

        team_adv_stats = scrape_team_adv_stats(BeautifulSoup(comments[2], 'lxml'), season)

    return [team_names_and_ids.abbr_to_team_id[team], season] + team_stats + team_yoy_improvement,\
           [team_names_and_ids.abbr_to_team_id[team], season] + opp_stats + opp_yoy_improvement,\
           [team_names_and_ids.abbr_to_team_id[team], season] + team_adv_stats


def scrape_team_stats(soup):
    trs = [tr for tr in soup.find_all('tr')]
    team_stats = [float(td.get_text()) if td.get_text() != '' else float(0)
                  for i, td in enumerate(trs[2].find_all('td'))][2:]
    team_yoy_improvement = [float(td.get_text().split('%')[0]) if td.get_text() != '' else
                            td.get_text() for td in trs[4].find_all('td')][2:]

    return team_stats, team_yoy_improvement


def scrape_opp_stats(soup):
    trs = [tr for tr in soup.find_all('tr')]
    opp_stats = [float(td.get_text()) if td.get_text() != '' else float(0)
                 for i, td in enumerate(trs[6].find_all('td'))][2:]
    opp_yoy_improvement = [float(td.get_text().split('%')[0]) if td.get_text() != '' else
                           td.get_text() for td in trs[8].find_all('td')][2:]

    return opp_stats, opp_yoy_improvement


def scrape_team_adv_stats(soup, year):
    # in 2017, adv stat table includes wins and losses as first two stats
    if year > 2016:
        return [int(td.get_text()) if i in [0, 1] else float(td.get_text())
                for i, td in enumerate(soup.find_all('tr')[2].find_all('td')[2:-2])]

    return [int(td.get_text()) if i in [0, 1] else float(td.get_text())
            for i, td in enumerate(soup.find_all('tr')[2].find_all('td')[:-2])]


def scrape_team_lineups(soup, team, year):
    team_lineups = []
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    comments = [comment for comment in comments if len(comment) > 1000]

    for i, comment in enumerate(comments):
        soup = BeautifulSoup(comment, 'html.parser')
        for tr in soup.find_all('tr'):
            if i <= 3:
                lineup = [team_names_and_ids.abbr_to_team_id[team], year, 1]
            else:
                lineup = [team_names_and_ids.abbr_to_team_id[team], year, 2]
            for j, td in enumerate(tr.find_all('td')):
                if j == 0:
                    players = []
                    if 'Team Average' in td.get_text():
                        break
                    for a in td.find_all('a'):
                        players.append(a.get('href'))
                    num_players = len(players)
                    if num_players == 5:
                        lineup.extend([num_players] + [player_link_to_player_id[player] for player in players])
                    elif num_players == 4:
                        lineup.extend([num_players] +
                                      [player_link_to_player_id[player] for player in players] + [np.nan])
                    elif num_players == 3:
                        lineup.extend([num_players] +
                                      [player_link_to_player_id[player] for player in players] + [np.nan, np.nan])
                    else:
                        lineup.extend([num_players] +
                                      [player_link_to_player_id[player] for player in players] +
                                      [np.nan, np.nan, np.nan])
                elif j == 1:
                    minutes = td.get_text().strip().split(':')[0]
                    lineup.append(int(minutes))
                else:
                    if td.get_text().strip() != '':
                        lineup.append(float(td.get_text().strip()))
                    else:
                        lineup.append(float(0))
            if len(lineup) > 10:
                # print(lineup)
                team_lineups.append(lineup)

    return team_lineups


def delete_all_team_table_rows(c, tablename):
    c.execute("DELETE FROM {};".format(tablename))


def scrape_and_update_team_stats(c, con):
    base_addr = 'http://www.basketball-reference.com/teams/'
    general_info, team_stats, opp_stats, team_adv_stats, team_lineups = [], [], [], [], []
    for team in team_names_and_ids.teams:
        year = 2017
        print(year, team)
        addr = base_addr + team + '/' + str(year) + '.html'
        try:
            resp = urlopen(addr)
        except HTTPError as e:
            print(e.code)
            continue
        soup = BeautifulSoup(resp.read(), 'lxml')
        # team_general_info = scrape_team_general_info(soup.find(id='info'), team, year)
        # general_info.append(team_general_info)
        team_stat, opp_stat, adv_stat = scrape_team_statistics(soup, team, year)
        team_stats.append(team_stat)
        opp_stats.append(opp_stat)
        team_adv_stats.append(adv_stat)

        lineup_addr = base_addr + team + '/' + str(year) + '/lineups/'
        try:
            resp = urlopen(lineup_addr)
        except HTTPError as e:
            print(e.code)
            continue
        soup = BeautifulSoup(resp.read(), 'lxml')
        team_lineup_combos = scrape_team_lineups(soup, team, year)
        team_lineups += team_lineup_combos

    # general_info_df = pd.DataFrame(data=general_info, columns=table_column_names.team_gen_info_cols)
    team_stats_df = pd.DataFrame(data=team_stats, columns=table_column_names.team_stats_cols)
    opp_stats_df = pd.DataFrame(data=opp_stats, columns=table_column_names.team_stats_cols)
    team_adv_stats_df = pd.DataFrame(data=team_adv_stats, columns=table_column_names.team_adv_cols)
    team_lineups_df = pd.DataFrame(data=team_lineups, columns=table_column_names.team_lineups_cols)

    # delete_all_team_table_rows(c, 'GeneralTeamSeasonInfo')
    # general_info_df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    # general_info_df.to_sql(name='GeneralTeamSeasonInfo', con=c, if_exists='append')

    delete_all_team_table_rows(c, 'TeamSeasonAdvStatsYTD')
    team_adv_stats_df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    team_adv_stats_df.to_sql(name='TeamSeasonAdvStatsYTD', con=con, if_exists='append')

    delete_all_team_table_rows(c, 'TeamSeasonLineupStatsYTD')
    team_lineups_df.to_sql(name='TeamSeasonLineupStatsYTD', con=con, if_exists='append', index=False)

    delete_all_team_table_rows(c, 'TeamSeasonStatsYTD')
    team_stats_df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    team_stats_df.to_sql(name='TeamSeasonStatsYTD', con=con, if_exists='append')

    delete_all_team_table_rows(c, 'TeamOppSeasonStatsYTD')
    opp_stats_df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    opp_stats_df.to_sql(name='TeamOppSeasonStatsYTD', con=con, if_exists='append')


# **************************************************************************************************************** #
# **************************************************************************************************************** #
#
#                                   SCRAPE GAME STATS
#
# **************************************************************************************************************** #
# **************************************************************************************************************** #


def scrape_player_stats_per_game(soup, game_id, season):
    player_stats = []

    home_team, away_team = get_home_away_teams(soup.h1, season)

    home_team_div_id = "box_" + home_team.lower() + "_basic"
    away_team_div_id = "box_" + away_team.lower() + "_basic"
    basic_player_stats = scrape_basic_player_stats(soup.find(id=home_team_div_id).find('tbody'),
                                                   soup.find(id=away_team_div_id).find('tbody'),
                                                   home_team, away_team)

    home_team_div_id = "box_" + home_team.lower() + "_advanced"
    away_team_div_id = "box_" + away_team.lower() + "_advanced"
    adv_player_stats = scrape_adv_player_stats(soup.find(id=home_team_div_id).find('tbody'),
                                               soup.find(id=away_team_div_id).find('tbody'))

    for basic, adv in zip(basic_player_stats, adv_player_stats):
        player_stats.append([game_id] + basic + adv)

    return player_stats


def scrape_adv_player_stats(home_soup, away_soup):
    home_player_stats, away_player_stats = [], []
    int_ind = [13, 14]
    global next_p_id
    global player_links
    for tr in home_soup.find_all('tr'):
        if tr.find('th').get_text().strip() == 'Reserves':
            continue
        player_link = tr.a.get('href')
        if player_link not in player_link_to_player_id:
            player_link_to_player_id[player_link] = next_p_id
            next_p_id += 1
        if player_link not in player_links:
            player_links.append(player_link)
        temp_home_player_stats = []
        for i, td in enumerate(tr.find_all('td')):
            if i == 0 and td.get_text().strip() == '':
                break
            if i == 0:
                continue
            elif i in int_ind:
                if td.get_text() != '':
                    temp_home_player_stats.append(int(td.get_text()))
                else:
                    temp_home_player_stats.append(int(0))
            else:
                if td.get_text() != '':
                    temp_home_player_stats.append(float(td.get_text()))
                else:
                    temp_home_player_stats.append(float(0))
        if temp_home_player_stats:
            home_player_stats.append(temp_home_player_stats)

    for tr in away_soup.find_all('tr'):
        if tr.find('th').get_text().strip() == 'Reserves':
            continue
        player_link = tr.a.get('href')
        if player_link not in player_link_to_player_id:
            player_link_to_player_id[player_link] = next_p_id
            next_p_id += 1
        if player_link not in player_links:
            player_links.append(player_link)
        temp_away_player_stats = []
        for i, td in enumerate(tr.find_all('td')):
            if i == 0 and td.get_text().strip() == '':
                break
            if i == 0:
                continue
            elif i in int_ind:
                if td.get_text() != '':
                    temp_away_player_stats.append(int(td.get_text()))
                else:
                    temp_away_player_stats.append(int(0))
            else:
                if td.get_text() != '':
                    temp_away_player_stats.append(float(td.get_text()))
                else:
                    temp_away_player_stats.append(float(0))
        if temp_away_player_stats:
            away_player_stats.append(temp_away_player_stats)

    # print(home_player_stats + away_player_stats)

    return home_player_stats + away_player_stats


def scrape_basic_player_stats(home_soup, away_soup, home_team, away_team):
    home_player_stats, away_player_stats = [], []
    float_ind = [3, 6, 9]
    global next_p_id
    global player_links
    for tr in home_soup.find_all('tr'):
        if tr.find('th').get_text().strip() == 'Reserves':
            continue
        player_link = tr.a.get('href')
        if player_link in player_link_to_player_id:
            player_id = player_link_to_player_id[player_link]
        else:
            player_link_to_player_id[player_link] = next_p_id
            player_id = next_p_id
            next_p_id += 1
        if player_link not in player_links:
            player_links.append(player_link)
        temp_home_player_stats = []
        for i, td in enumerate(tr.find_all('td')):
            if i == 0:
                if ':' not in td.get_text():
                    break
                temp_home_player_stats.append(td.get_text().strip())
            elif i in float_ind:
                if td.get_text() == '':
                    temp_home_player_stats.append(float(0))
                else:
                    temp_home_player_stats.append(float(td.get_text()))
            else:
                if td.get_text() == '':
                    temp_home_player_stats.append(int(0))
                else:
                    temp_home_player_stats.append(int(td.get_text()))
        if temp_home_player_stats:
            temp_home_player_stats.insert(0, team_names_and_ids.abbr_to_team_id[home_team])
            temp_home_player_stats.insert(0, player_link)
            temp_home_player_stats.insert(0, player_id)
            if len(temp_home_player_stats) == 22:
                temp_home_player_stats.append(np.nan)
            home_player_stats.append(temp_home_player_stats)

    for tr in away_soup.find_all('tr'):
        if tr.find('th').get_text().strip() == 'Reserves':
            continue
        player_link = tr.a.get('href')
        if player_link in player_link_to_player_id:
            player_id = player_link_to_player_id[player_link]
        else:
            player_link_to_player_id[player_link] = next_p_id
            player_id = next_p_id
            next_p_id += 1
        if player_link not in player_links:
            player_links.append(player_link)
        temp_away_player_stats = []
        for i, td in enumerate(tr.find_all('td')):
            if i == 0:
                if ':' not in td.get_text():
                    break
                temp_away_player_stats.append(td.get_text().strip())
            elif i in float_ind:
                if td.get_text() == '':
                    temp_away_player_stats.append(float(0))
                else:
                    temp_away_player_stats.append(float(td.get_text()))
            else:
                if td.get_text() == '':
                    temp_away_player_stats.append(int(0))
                else:
                    temp_away_player_stats.append(int(td.get_text()))
        if temp_away_player_stats:
            temp_away_player_stats.insert(0, team_names_and_ids.abbr_to_team_id[away_team])
            temp_away_player_stats.insert(0, player_link)
            temp_away_player_stats.insert(0, player_id)
            if len(temp_away_player_stats) == 22:
                temp_away_player_stats.append(np.nan)
            away_player_stats.append(temp_away_player_stats)

    return home_player_stats + away_player_stats


def scrape_team_stats_per_game(soup, game_id, season):
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    comments = [comment for comment in comments if len(comment) > 500]

    home_team, away_team = get_home_away_teams(soup.h1, season)
    g_date = scrape_game_date(soup.find(class_='scorebox_meta'))
    home_score, away_score = scrape_game_score(BeautifulSoup(comments[1], 'lxml'), home_team)

    g_type = get_game_type(BeautifulSoup(comments[0], 'lxml').encode('utf-8')[:200])

    pace = float(BeautifulSoup(comments[2], 'lxml').find_all('td', {'data-stat': 'pace'})[0].get_text().strip())

    home_team_div_id = "box_" + home_team.lower() + "_basic"
    away_team_div_id = "box_" + away_team.lower() + "_basic"
    home_basic_stats, away_basic_stats = scrape_basic_team_stats(soup.find(id=home_team_div_id).find('tfoot'),
                                                                           soup.find(id=away_team_div_id).find('tfoot'))

    home_team_div_id = "box_" + home_team.lower() + "_advanced"
    away_team_div_id = "box_" + away_team.lower() + "_advanced"
    home_adv_stats, away_adv_stats = scrape_adv_team_stats(soup.find(id=home_team_div_id).find('tfoot'),
                                                                     soup.find(id=away_team_div_id).find('tfoot'))

    return [game_id, int(season), g_date, g_type, 1] + home_score + home_basic_stats + [pace] + home_adv_stats,\
           [game_id, int(season), g_date, g_type, 0] + away_score + away_basic_stats + [pace] + away_adv_stats


def get_game_type(string):
    if 'NBA Scores' in str(string):
        return 1
    else:
        return 2


def scrape_game_date(soup):
    return pd.to_datetime(soup.find('div').get_text())


def scrape_adv_team_stats(home_soup, away_soup):
    home_stats, away_stats = [], []
    not_ind = [0, 12]

    for i, (home_td, away_td) in enumerate(zip(home_soup.find_all('td'), away_soup.find_all('td'))):
        if i in not_ind:
            continue
        else:
            home_stats.append(float(home_td.get_text()))
            away_stats.append(float(away_td.get_text()))

    return home_stats, away_stats


def scrape_basic_team_stats(home_soup, away_soup):
    home_stats, away_stats = [], []
    not_ind = [0, 18, 19]
    float_ind = [3, 6, 9]

    for i, (home_td, away_td) in enumerate(zip(home_soup.find_all('td'), away_soup.find_all('td'))):
        if i in not_ind:
            continue
        elif i in float_ind:
            home_stats.append(float(home_td.get_text()))
            away_stats.append(float(away_td.get_text()))
        else:
            home_stats.append(int(home_td.get_text()))
            away_stats.append(int(away_td.get_text()))

    return home_stats, away_stats


def scrape_game_score(soup, home_team):
    scoring = []

    for td in soup.find_all('td'):
        if td.a:
            scoring.append(td.get_text())
        else:
            scoring.append(int(td.get_text()))

    split = int(len(scoring) / 2)
    team1_score = scoring[:split]
    team2_score = scoring[split:]

    if len(team1_score) == 6:
        team1_score.insert(-1, np.nan)
        team2_score.insert(-1, np.nan)
    elif len(team1_score) == 8:
        team1_score[-3] += team1_score[-2]
        del team1_score[-2]
        team2_score[-3] += team2_score[-2]
        del team2_score[-2]
    elif len(team1_score) == 9:
        team1_score[-4] += team1_score[-2]
        del team1_score[-2]
        team1_score[-3] += team1_score[-2]
        del team1_score[-2]
        team2_score[-4] += team2_score[-2]
        del team2_score[-2]
        team2_score[-3] += team2_score[-2]
        del team2_score[-2]
    elif len(team1_score) == 10:
        team1_score[-5] += team1_score[-2]
        del team1_score[-2]
        team1_score[-4] += team1_score[-2]
        del team1_score[-2]
        team1_score[-3] += team1_score[-2]
        del team1_score[-2]
        team2_score[-5] += team2_score[-2]
        del team2_score[-2]
        team2_score[-4] += team2_score[-2]
        del team2_score[-2]
        team2_score[-3] += team2_score[-2]
        del team2_score[-2]

    team1_score.insert(1, team_names_and_ids.abbr_to_team_id[team1_score[0]])
    team2_score.insert(1, team_names_and_ids.abbr_to_team_id[team2_score[0]])

    if team1_score[0] == home_team:
        return team1_score, team2_score
    else:
        return team2_score, team1_score


def get_home_away_teams(header, year):
    away_team = header.get_text().split(' at ')[0].strip()
    home_team = header.get_text().split(' at ')[1].split('Box')[0].strip()
    home_team_abbr = team_names_and_ids.teams_to_abbr[home_team]
    away_team_abbr = team_names_and_ids.teams_to_abbr[away_team]
    return home_team_abbr, away_team_abbr


def scrape_and_update_game_stats(c):
    base_addr = 'http://www.basketball-reference.com'
    team_game_stats, player_game_stats = [], []
    end_flag = False
    global next_game_id

    year = 2017
    for month in gen_bball_info.months:
        print(month, year)
        addr = base_addr + '/leagues/NBA_' + str(year) + '_games-' + month + '.html'
        try:
            resp = urlopen(addr)
        except HTTPError as e:
            print(e.code)
            continue
        soup = BeautifulSoup(resp.read(), 'lxml')
        tbody = soup.find('tbody')

        for tr in tbody.find_all('tr'):
            game_date = tr.th.get_text().strip()
            game_date = datetime.strptime(game_date, '%a, %b %d, %Y')
            today = datetime.today()
            if game_date < last_date:
                continue
            if game_date > today:
                end_flag = True
                break
            print(game_date)
            for link in tr.find_all('a'):
                if link.get_text().strip() == 'Box Score':
                    box_score_addr = link.get('href')
                    game_addr = base_addr + box_score_addr
                    try:
                        resp = urlopen(game_addr)
                    except HTTPError as e:
                        print(e.code)
                        continue
                    soup = BeautifulSoup(resp.read(), 'lxml')
                    home_team_stats, away_team_stats = scrape_team_stats_per_game(soup, next_game_id, year)
                    team_game_stats.append(home_team_stats)
                    team_game_stats.append(away_team_stats)
                    basic_player_stats = scrape_player_stats_per_game(soup, next_game_id, year)
                    player_game_stats += basic_player_stats
                    next_game_id += 1
        if end_flag:
            break

    player_game_stats_df = pd.DataFrame(data=player_game_stats, columns=table_column_names.player_stats_cols)
    team_game_stats_df = pd.DataFrame(data=team_game_stats, columns=table_column_names.team_game_stats_cols)

    player_game_stats_df.to_sql(name='PlayerGameStatsYTD', con=c, if_exists='append', index=False)
    team_game_stats_df.to_sql(name='TeamGameStatsYTD', con=c, if_exists='append', index=False)


def main():
    # try:
    conn = open_db()
    c = conn.connect()

    populate_playerlink_to_playerid_dict(c)
    get_player_links_from_gen_info(c)
    set_next_p_id(c)
    get_last_date_updated(c)
    set_next_game_id(c)

    scrape_and_update_game_stats(conn)
    scrape_and_update_player_stats(c, conn)
    scrape_and_update_team_stats(c, conn)

    send_confirmation_email()
    # except Exception as e:
    #     send_error_email()
    #     with open('updateServiceErrors.txt', 'w') as f:
    #         f.write('Exception Type: '.format(type(e)))
    #         f.write('Exception Arguments: '.format(e.args))
    #         f.write('Exception: '.format(e))


if __name__ == "__main__":
    main()
