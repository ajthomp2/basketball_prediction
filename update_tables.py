from bs4 import BeautifulSoup
from bs4 import Comment
import pandas as pd
from urllib.request import urlopen
from urllib.error import HTTPError
import sqlite3 as lite
from datetime import datetime
import numpy as np
import os

# ********************************************************#
# ********************************************************#
# NOTES:
#
# ********************************************************#
# ********************************************************#


teams_to_abbr = {'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BRK',
                 'Charlotte Hornets': 'CHH', 'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE',
                 'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN', 'Detroit Pistons': 'DET',
                 'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND',
                 'Los Angeles Clippers': 'LAC', 'Los Angeles Lakers': 'LAL', 'Memphis Grizzlies': 'MEM',
                 'Miami Heat': 'MIA', 'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN',
                 'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK', 'Oklahoma City Thunder': 'OKC',
                 'Orlando Magic': 'ORL', 'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHO',
                 'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS',
                 'Toronto Raptors': 'TOR', 'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS',
                 'New Jersey Nets': 'NJN', 'Charlotte Bobcats': 'CHA', 'Vancouver Grizzlies': 'VAN',
                 'New Orleans/Oklahoma City Hornets': 'NOK', 'Seattle SuperSonics': 'SEA',
                 'New Orleans Hornets': 'NOK'}
abbr_to_teams = {'ATL': 'Atlanta Hawks', 'BOS': 'Boston Celtics', 'BRK': 'Brooklyn Nets',
                 'CHH': 'Charlotte Hornets', 'CHI': 'Chicago Bulls', 'CLE': 'Cleveland Cavaliers',
                 'DAL': 'Dallas Mavericks', 'DEN': 'Denver Nuggets', 'DET': 'Detroit Pistons',
                 'GSW': 'Golden State Warriors', 'HOU': 'Houston Rockets', 'IND': 'Indiana Pacers',
                 'LAC': 'Los Angeles Clippers', 'LAL': 'Los Angeles Lakers', 'MEM': 'Memphis Grizzlies',
                 'MIA': 'Miami Heat', 'MIL': 'Milwaukee Bucks', 'MIN': 'Minnesota Timberwolves',
                 'NOP': 'New Orleans Pelicans', 'NYK': 'New York Knicks', 'OKC': 'Oklahoma City Thunder',
                 'ORL': 'Orlando Magic', 'PHI': 'Philadelphia 76ers', 'PHO': 'Phoenix Suns',
                 'POR': 'Portland Trail Blazers', 'SAC': 'Sacramento Kings', 'SAS': 'San Antonio Spurs',
                 'TOR': 'Toronto Raptors', 'UTA': 'Utah Jazz', 'WAS': 'Washington Wizards',
                 'NJN': 'New Jersey Nets', 'CHA': 'Charlotte Bobcats', 'VAN': 'Vancouver Grizzlies',
                 'NOK': 'New Orleans/Oklahoma City Hornets', 'SEA': 'Seattle SuperSonics',
                 'CHO': 'Charlotte Hornets'}
teams_to_team_id = {'Atlanta Hawks': 1, 'Boston Celtics': 2, 'Brooklyn Nets': 3,
                    'Charlotte Hornets': 4, 'Chicago Bulls': 5, 'Cleveland Cavaliers': 6,
                    'Dallas Mavericks': 7, 'Denver Nuggets': 8, 'Detroit Pistons': 9,
                    'Golden State Warriors': 10, 'Houston Rockets': 11, 'Indiana Pacers': 12,
                    'Los Angeles Clippers': 13, 'Los Angeles Lakers': 14, 'Memphis Grizzlies': 15,
                    'Miami Heat': 16, 'Milwaukee Bucks': 17, 'Minnesota Timberwolves': 18,
                    'New Orleans Pelicans': 19, 'New York Knicks': 20, 'Oklahoma City Thunder': 21,
                    'Orlando Magic': 22, 'Philadelphia 76ers': 23, 'Phoenix Suns': 24,
                    'Portland Trail Blazers': 25, 'Sacramento Kings': 26, 'San Antonio Spurs': 27,
                    'Toronto Raptors': 28, 'Utah Jazz': 29, 'Washington Wizards': 30,
                    'New Jersey Nets': 3, 'Charlotte Bobcats': 4, 'Vancouver Grizzlies': 15,
                    'New Orleans/Oklahoma City Hornets': 19, 'Seattle SuperSonics': 21,
                    'New Orleans Hornets': 19}
abbr_to_team_id = {'ATL': 1, 'BOS': 2, 'BRK': 3, 'CHH': 4, 'CHI': 5, 'CLE': 6,
                   'DAL': 7, 'DEN': 8, 'DET': 9, 'GSW': 10, 'HOU': 11, 'IND': 12,
                   'LAC': 13, 'LAL': 14, 'MEM': 15, 'MIA': 16, 'MIL': 17, 'MIN': 18,
                   'NOP': 19, 'NYK': 20, 'OKC': 21, 'ORL': 22, 'PHI': 23, 'PHO': 24,
                   'POR': 25, 'SAC': 26, 'SAS': 27, 'TOR': 28, 'UTA': 29, 'WAS': 30,
                   'NJN': 3, 'CHA': 4, 'CHO': 4, 'VAN': 15, 'NOK': 19, 'SEA': 21, 'NOH': 19,
                   'WSB': 30, 'KCK': 26}

positions_to_abbr = {'Point Guard': 'PG', 'Shooting Guard': 'SG', 'Small Forward': 'SF',
                     'Power Forward': 'PF', 'Center': 'C', np.nan: np.nan}

division_to_div_id = {'Atlantic': 1, 'Central': 2, 'Southeast': 3,
                      'Northwest': 4, 'Pacific': 5, 'Southwest': 6, 'Midwest': 7}
conference_to_conf_id = {'Eastern': 1, 'Western': 2}

teams = ['ATL', 'BOS', 'BRK', 'CHH', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 'HOU', 'IND',
         'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHO',
         'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS', 'NJN', 'CHA', 'CHO', 'VAN', 'NOK', 'SEA']

months = ['october', 'november', 'december', 'january',
          'february', 'march', 'april', 'may', 'june']

player_links = []
gen_info_player_links = []
player_link_to_player_id = {}
next_p_id = 1
last_date = datetime.today()

# *********     PLAYER STATS COLUMNS     ********* #

general_player_info_cols = ['PlayerID', 'Player', 'PlayerLink', 'BirthDate', 'Position1', 'Position2',
                            'Position3', 'DraftPick', 'Height', 'Weight', 'College']

per_game_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'MP/G', 'FG/G',
                 'FGA/G', 'FG%', '3P/G', '3PA/G', '3P%', '2P/G', '2PA/G', '2P%', 'eFG%',
                 'FT/G', 'FTA/G', 'FT%', 'ORB/G', 'DRB/G', 'TRB/G', 'AST/G', 'STL/G', 'BLK/G',
                 'TOV/G', 'PF/G', 'PTS/G']

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

adv_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'PER', 'TS%', '3PAr',
            'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%', 'OWS', 'DWS',
            'WS', 'WS/48', 'OBRM', 'DBPM', 'BPM', 'VORP']

on_off_cols = ['PlayerID', 'Season', 'GameTypeID', 'TeamID', 'OnCourtTeameFG%', 'OnCourtTeamORB%',
               'OnCourtTeamDRB%', 'OnCourtTeamTRB%', 'OnCourtTeamAST%', 'OnCourtTeamSTL%', 'OnCourtTeamBLK%',
               'OnCourtTeamTOV%', 'OnCourtTeamORtg', 'OnCourtOppeFG%', 'OnCourtOppORB%', 'OnCourtOppDRB%',
               'OnCourtOppTRB%', 'OnCourtOppAST%', 'OnCourtOppSTL%', 'OnCourtOppBLK%', 'OnCourtOppTOV%',
               'OnCourtOppORtg', 'OnCourtDiffeFG%', 'OnCourtDiffORB%', 'OnCourtDiffDRB%', 'OnCourtDiffTRB%',
               'OnCourtDiffAST%', 'OnCourtDiffSTL%', 'OnCourtDiffBLK%', 'OnCourtDiffTOV%', 'OnCourtDiffORtg',
               'OffCourtTeameFG%', 'OffCourtTeamORB%', 'OffCourtTeamDRB%', 'OffCourtTeamTRB%', 'OffCourtTeamAST%',
               'OffCourtTeamSTL%', 'OffCourtTeamBLK%', 'OffCourtTeamTOV%', 'OffCourtTeamORtg', 'OffCourtOppeFG%',
               'OffCourtOppORB%', 'OffCourtOppDRB%', 'OffCourtOppTRB%', 'OffCourtOppAST%', 'OffCourtOppSTL%',
               'OffCourtOppBLK%', 'OffCourtOppTOV%', 'OffCourtOppORtg', 'OffCourtDiffeFG%', 'OffCourtDiffORB%',
               'OffCourtDiffDRB%', 'OffCourtDiffTRB%', 'OffCourtDiffAST%', 'OffCourtDiffSTL%',
               'OffCourtDiffBLK%', 'OffCourtDiffTOV%', 'OffCourtDiffORtg', 'On-OffTeameFG%', 'On-OffTeamORB%',
               'On-OffTeamDRB%', 'On-OffTeamTRB%', 'On-OffTeamAST%', 'On-OffTeamSTL%', 'On-OffTeamBLK%',
               'On-OffTeamTOV%', 'On-OffTeamORtg', 'On-OffOppeFG%', 'On-OffOppORB%', 'On-OffOppDRB%',
               'On-OffOppTRB%', 'On-OffOppAST%', 'On-OffOppSTL%', 'On-OffOppBLK%', 'On-OffOppTOV%',
               'On-OffOppORtg', 'On-OffDiffeFG%', 'On-OffDiffORB%', 'On-OffDiffDRB%', 'On-OffDiffTRB%',
               'On-OffDiffAST%', 'On-OffDiffSTL%', 'On-OffDiffBLK%', 'On-OffDiffTOV%', 'On-OffDiffORtg']

# *********     TEAM STATS COLUMNS     ********* #

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

# *********     GAME STATS COLUMNS     ********* #

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


# **************************************************************************************************************** #
# **************************************************************************************************************** #
#
#                                   UTILITY FUNCTIONS
#
# **************************************************************************************************************** #
# **************************************************************************************************************** #


def open_db(db_file):
    conn = lite.connect(db_file)
    print('Opened database in file {} successfully'.format(db_file))
    return conn


def populate_playerlink_to_playerid_dict(c):
    c.execute('''SELECT PlayerLink, PlayerID
                 FROM GeneralPlayerInfo''')
    result = c.fetchall()

    global player_link_to_player_id
    player_link_to_player_id = dict(result)


def get_player_links_from_gen_info(c):
    c.execute('''SELECT PlayerLink
                 FROM GeneralPlayerInfo''')
    result = c.fetchall()
    global gen_info_player_links
    gen_info_player_links = [row[0] for row in result]


def set_next_p_id(c):
    c.execute('''SELECT MAX(PlayerID)
                 FROM GeneralPlayerInfo''')
    result = c.fetchall()
    global next_p_id
    next_p_id = int(result[0][0]) + 1


def get_last_date_updated(c):
    c.execute('''SELECT Date
                 FROM TeamGameStatsYTD''')
    result = c.fetchall()
    dates = [datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S") for row in result]
    global last_date
    last_date = max(dates)


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
            p1, p2, p3 = positions_to_abbr[p1], positions_to_abbr[p2], positions_to_abbr[p3]

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
                    per_game_stats_season.extend([abbr_to_team_id[team]])
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
                    stats_seasons.extend([abbr_to_team_id[team]])
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
                    team_on_off_stats.extend([abbr_to_team_id[team]])
                elif k == 1 or (k == 0 and j != 0):
                    continue
                else:
                    if td.get_text().strip() != '':
                        team_on_off_stats.append(float(td.get_text().strip()))
                    else:
                        team_on_off_stats.append(float(0))
        on_off_stats.append(team_on_off_stats)

    return on_off_stats


def scrape_and_update_player_stats(c):
    base_addr = 'http://www.basketball-reference.com'
    general_info, per_36_min_stats, per_100_pos_stats, adv_stats = [], [], [], []
    per_game_stats, shooting_stats, pbp_stats, on_off_stats = [], [], [], []

    global player_links
    for link in player_links:
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

    if general_info:
        general_info_df = pd.DataFrame(data=general_info, columns=general_player_info_cols)
        general_info_df.set_index('PlayerID', inplace=True, verify_integrity=True)
        general_info_df.to_sql(name='GeneralPlayerInfo', con=c, if_exists='append')
    per_game_stats_df = pd.DataFrame(data=per_game_stats, columns=per_game_cols)
    per_36_mins_df = pd.DataFrame(data=per_36_min_stats, columns=per_36_min_cols)
    per_100_pos_df = pd.DataFrame(data=per_100_pos_stats, columns=per_100_poss_cols)
    adv_stats_df = pd.DataFrame(data=adv_stats, columns=adv_cols)
    shooting_stats_df = pd.DataFrame(data=shooting_stats, columns=shooting_cols)
    pbp_stats_df = pd.DataFrame(data=pbp_stats, columns=pbp_cols)
    on_off_stats_df = pd.DataFrame(data=on_off_stats, columns=on_off_cols)

    per_game_stats_df.to_sql(name='PlayerSeasonStatsPerGameYTD', con=c, if_exists='append', index=False)
    adv_stats_df.to_sql(name='PlayerSeasonAdvStatsYTD', con=c, if_exists='append', index=False)
    on_off_stats_df.to_sql(name='PlayerSeasonOnOffStatsYTD', con=c, if_exists='append', index=False)
    pbp_stats_df.to_sql(name='PlayerSeasonPBPStatsYTD', con=c, if_exists='append', index=False)
    per_36_mins_df.to_sql(name='PlayerSeasonStatsPer36MinutesYTD', con=c, if_exists='append', index=False)
    per_100_pos_df.to_sql(name='PlayerSeasonStatsPer100PossYTD', con=c, if_exists='append', index=False)
    shooting_stats_df.to_sql(name='PlayerSeasonShootingStatsYTD', con=c, if_exists='append', index=False)


# **************************************************************************************************************** #
# **************************************************************************************************************** #
#
#                                   SCRAPE PLAYER STATS
#
# **************************************************************************************************************** #
# **************************************************************************************************************** #


def scrape_team_statistics(soup, team, season):
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    comments = [comment for comment in comments if len(comment) > 200]

    if BeautifulSoup(comments[0], 'lxml').table.has_attr('id')\
            and BeautifulSoup(comments[0], 'lxml').table['id'] == 'injury':
        team_stats, team_yoy_improvement = \
            scrape_team_stats(BeautifulSoup(comments[2], 'lxml'))

        opp_stats, opp_yoy_improvement = \
            scrape_opp_stats(BeautifulSoup(comments[2], 'lxml'))

        team_adv_stats = scrape_team_adv_stats(BeautifulSoup(comments[3], 'lxml'))
    else:
        team_stats, team_yoy_improvement = \
            scrape_team_stats(BeautifulSoup(comments[1], 'lxml'))

        opp_stats, opp_yoy_improvement = \
            scrape_opp_stats(BeautifulSoup(comments[1], 'lxml'))

        team_adv_stats = scrape_team_adv_stats(BeautifulSoup(comments[2], 'lxml'))

    return [abbr_to_team_id[team], season] + team_stats + team_yoy_improvement,\
           [abbr_to_team_id[team], season] + opp_stats + opp_yoy_improvement,\
           [abbr_to_team_id[team], season] + team_adv_stats


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


def scrape_team_adv_stats(soup):
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
                lineup = [abbr_to_team_id[team], year, 1]
            else:
                lineup = [abbr_to_team_id[team], year, 2]
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


def scrape_and_update_team_stats(c):
    base_addr = 'http://www.basketball-reference.com/teams/'
    general_info, team_stats, opp_stats, team_adv_stats, team_lineups = [], [], [], [], []
    for team in teams:
        year = 2017
        print(year, team)
        addr = base_addr + team + '/' + str(year) + '.html'
        try:
            resp = urlopen(addr)
        except HTTPError as e:
            print(e.code)
            continue
        soup = BeautifulSoup(resp.read(), 'lxml')
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

    # general_info_df = pd.DataFrame(data=general_info, columns=team_gen_info_cols)
    team_stats_df = pd.DataFrame(data=team_stats, columns=team_stats_cols)
    opp_stats_df = pd.DataFrame(data=opp_stats, columns=team_stats_cols)
    team_adv_stats_df = pd.DataFrame(data=team_adv_stats, columns=team_adv_cols)
    team_lineups_df = pd.DataFrame(data=team_lineups, columns=team_lineups_cols)

    # general_info_df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    # general_info_df.to_sql(name='GeneralTeamSeasonInfo', con=c, if_exists='append')

    team_adv_stats_df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    team_adv_stats_df.to_sql(name='TeamSeasonAdvStatsYTD', con=c, if_exists='append')

    team_lineups_df.to_sql(name='TeamSeasonLineupStatsYTD', con=c, if_exists='append', index=False)

    team_stats_df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    team_stats_df.to_sql(name='TeamSeasonStatsYTD', con=c, if_exists='append')

    opp_stats_df.set_index(['TeamID', 'Season'], inplace=True, verify_integrity=True)
    opp_stats_df.to_sql(name='TeamOppSeasonStatsYTD', con=c, if_exists='append')


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
                temp_home_player_stats.append(int(td.get_text().split(':')[0]))
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
            temp_home_player_stats.insert(0, abbr_to_team_id[home_team])
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
                temp_away_player_stats.append(int(td.get_text().split(':')[0]))
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
            temp_away_player_stats.insert(0, abbr_to_team_id[away_team])
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

    home_team_div_id = "box_" + home_team.lower() + "_basic"
    away_team_div_id = "box_" + away_team.lower() + "_basic"
    home_team_basic_stats, away_team_basic_stats = scrape_basic_team_stats(soup.find(id=home_team_div_id).find('tfoot'),
                                                                           soup.find(id=away_team_div_id).find('tfoot'))

    home_team_div_id = "box_" + home_team.lower() + "_advanced"
    away_team_div_id = "box_" + away_team.lower() + "_advanced"
    home_team_adv_stats, away_team_adv_stats = scrape_adv_team_stats(soup.find(id=home_team_div_id).find('tfoot'),
                                                                     soup.find(id=away_team_div_id).find('tfoot'))

    return [game_id] + [int(season)] + [g_date] + [g_type] + home_score + home_team_basic_stats + home_team_adv_stats,\
           [game_id] + [int(season)] + [g_date] + [g_type] + away_score + away_team_basic_stats + away_team_adv_stats


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

    team1_score.insert(1, abbr_to_team_id[team1_score[0]])
    team2_score.insert(1, abbr_to_team_id[team2_score[0]])

    if team1_score[0] == home_team:
        return team1_score, team2_score
    else:
        return team2_score, team1_score


def get_home_away_teams(header, year):
    away_team = header.get_text().split(' at ')[0].strip()
    home_team = header.get_text().split(' at ')[1].split('Box')[0].strip()
    home_team_abbr = teams_to_abbr[home_team]
    away_team_abbr = teams_to_abbr[away_team]
    if home_team_abbr == 'CHH' and year > 2014:
        home_team_abbr = 'CHO'
    elif away_team_abbr == 'CHH' and year > 2014:
        away_team_abbr = 'CHO'
    return home_team_abbr, away_team_abbr


def scrape_and_update_game_stats(c):
    base_addr = 'http://www.basketball-reference.com'
    game_id = 20598
    team_game_stats, player_game_stats = [], []
    end_flag = False

    year = 2017
    for month in months:
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
                    home_team_stats, away_team_stats = scrape_team_stats_per_game(soup, game_id, year)
                    team_game_stats.append(home_team_stats)
                    team_game_stats.append(away_team_stats)
                    basic_player_stats = scrape_player_stats_per_game(soup, game_id, year)
                    player_game_stats += basic_player_stats
                    game_id += 1
        if end_flag:
            break

    player_game_stats_df = pd.DataFrame(data=player_game_stats,
                                        columns=player_stats_cols)
    team_game_stats_df = pd.DataFrame(data=team_game_stats,
                                      columns=team_game_stats_cols)

    # *******    COMMENT OUT BELOW SECTION AFTER TESTING IS DONE

    # global player_links
    # player_links_df = pd.DataFrame(data=player_links)
    #
    # writer = pd.ExcelWriter('player_links_test.xlsx')
    # player_links_df.to_excel(writer, 'Sheet1')
    # writer.save()

    player_game_stats_df.to_sql(name='PlayerGameStatsYTD', con=c, if_exists='append', index=False)
    team_game_stats_df.to_sql(name='TeamGameStatsYTD', con=c, if_exists='append', index=False)


def main():
    conn = open_db(os.path.join('Database', 'test.db'))
    c = conn.cursor()

    populate_playerlink_to_playerid_dict(c)
    get_player_links_from_gen_info(c)
    set_next_p_id(c)
    get_last_date_updated(c)

    scrape_and_update_game_stats(conn)
    scrape_and_update_player_stats(conn)
    scrape_and_update_team_stats(conn)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
