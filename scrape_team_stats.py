from bs4 import BeautifulSoup
from bs4 import Comment
import pandas as pd
import numpy as np
from urllib.request import urlopen
from urllib.error import HTTPError
import requests
import sqlite3 as lite
import sys

#********************************************************#
#********************************************************#
# NOTES:
#   - maybe scrape per game stats instead of total stats for team_stats
#********************************************************#
#********************************************************#


teams_to_abbr = {'Atlanta Hawks' : 'ATL', 'Boston Celtics' : 'BOS', 'Brooklyn Nets' : 'BRK',
                 'Charlotte Hornets' : 'CHH', 'Chicago Bulls' : 'CHI', 'Cleveland Cavaliers' : 'CLE',
                 'Dallas Mavericks' : 'DAL', 'Denver Nuggets' : 'DEN', 'Detroit Pistons' : 'DET',
                 'Golden State Warriors' : 'GSW', 'Houston Rockets' : 'HOU', 'Indiana Pacers' : 'IND',
                 'Los Angeles Clippers' : 'LAC', 'Los Angeles Lakers' : 'LAL', 'Memphis Grizzlies' : 'MEM',
                 'Miami Heat' : 'MIA', 'Milwaukee Bucks' : 'MIL', 'Minnesota Timberwolves' : 'MIN',
                 'New Orleans Pelicans' : 'NOP', 'New York Knicks' : 'NYK', 'Oklahoma City Thunder' : 'OKC',
                 'Orlando Magic' : 'ORL', 'Philadelphia 76ers' : 'PHI', 'Phoenix Suns' : 'PHO',
                 'Portland Trail Blazers' : 'POR', 'Sacramento Kings' : 'SAC', 'San Antonio Spurs' : 'SAS',
                 'Toronto Raptors' : 'TOR', 'Utah Jazz' : 'UTA', 'Washington Wizards' : 'WAS',
                 'New Jersey Nets' : 'NJN', 'Charlotte Bobcats' : 'CHA', 'Vancouver Grizzlies' : 'VAN',
                 'New Orleans/Oklahoma City Hornets' : 'NOK', 'Seattle SuperSonics' : 'SEA',
                 'New Orleans Hornets' : 'NOH'}
abbr_to_teams = {'ATL' : 'Atlanta Hawks', 'BOS' : 'Boston Celtics', 'BRK' : 'Brooklyn Nets',
                 'CHH' : 'Charlotte Hornets', 'CHI' : 'Chicago Bulls', 'CLE' : 'Cleveland Cavaliers',
                 'DAL' : 'Dallas Mavericks', 'DEN' : 'Denver Nuggets', 'DET' : 'Detroit Pistons',
                 'GSW' : 'Golden State Warriors', 'HOU' : 'Houston Rockets', 'IND' : 'Indiana Pacers',
                 'LAC' : 'Los Angeles Clippers', 'LAL' : 'Los Angeles Lakers', 'MEM' : 'Memphis Grizzlies',
                 'MIA' : 'Miami Heat', 'MIL' : 'Milwaukee Bucks', 'MIN' : 'Minnesota Timberwolves',
                 'NOP' : 'New Orleans Pelicans', 'NYK' : 'New York Knicks', 'OKC' : 'Oklahoma City Thunder',
                 'ORL' : 'Orlando Magic', 'PHI' : 'Philadelphia 76ers', 'PHO' : 'Phoenix Suns',
                 'POR' : 'Portland Trail Blazers', 'SAC' : 'Sacramento Kings', 'SAS' : 'San Antonio Spurs',
                 'TOR' : 'Toronto Raptors', 'UTA' : 'Utah Jazz', 'WAS' : 'Washington Wizards',
                 'NJN' : 'New Jersey Nets', 'CHA' : 'Charlotte Bobcats', 'VAN' : 'Vancouver Grizzlies',
                 'NOK' : 'New Orleans/Oklahoma City Hornets', 'SEA' : 'Seattle SuperSonics', 'CHO': 'Charlotte Hornets',
                 'NOH' : 'New Orleans Hornets'}
teams_to_team_id = {'Atlanta Hawks' : 1, 'Boston Celtics' : 2, 'Brooklyn Nets' : 3,
                 'Charlotte Hornets' : 4, 'Chicago Bulls' : 5, 'Cleveland Cavaliers' : 6,
                 'Dallas Mavericks' : 7, 'Denver Nuggets' : 8, 'Detroit Pistons' : 9,
                 'Golden State Warriors' : 10, 'Houston Rockets' : 11, 'Indiana Pacers' : 12,
                 'Los Angeles Clippers' : 13, 'Los Angeles Lakers' : 14, 'Memphis Grizzlies' : 15,
                 'Miami Heat' : 16, 'Milwaukee Bucks' : 17, 'Minnesota Timberwolves' : 18,
                 'New Orleans Pelicans' : 19, 'New York Nnicks' : 20, 'Oklahoma City Thunder' : 21,
                 'Orlando Magic' : 22, 'Philadelphia 76ers' : 23, 'Phoenix Suns' : 24,
                 'Portland Trail Blazers' : 25, 'Sacramento Kings' : 26, 'San Antonio Spurs' : 27,
                 'Toronto Raptors' : 28, 'Utah Jazz' : 29, 'Washington Wizards' : 30,
                 'New Jersey Nets' : 3, 'Charlotte Bobcats' : 4, 'Vancouver Grizzlies' : 15,
                 'New Orleans/Oklahoma City Hornets': 19, 'Seattle SuperSonics' : 21, 'New Orleans Hornets': 19}
abbr_to_team_id = {'ATL': 1, 'BOS': 2, 'BRK': 3, 'CHH': 4, 'CHI': 5, 'CLE': 6,
                 'DAL': 7, 'DEN': 8, 'DET': 9, 'GSW': 10, 'HOU': 11, 'IND': 12,
                 'LAC': 13, 'LAL': 14, 'MEM' : 15, 'MIA': 16, 'MIL': 17, 'MIN': 18,
                 'NOP': 19, 'NYK': 20, 'OKC' : 21, 'ORL': 22, 'PHI': 23, 'PHO': 24,
                 'POR': 25, 'SAC': 26, 'SAS' : 27, 'TOR': 28, 'UTA': 29, 'WAS' : 30,
                 'NJN': 3, 'CHA': 4, 'CHO': 4, 'VAN' : 15, 'NOK': 19, 'SEA': 21, 'NOH': 19}

division_to_div_id = {'Atlantic' : 1, 'Central' : 2, 'Southeast' : 3,
                      'Northwest' : 4, 'Pacific' : 5, 'Southwest' : 6, 'Midwest' : 7}
conference_to_conf_id = {'Eastern' : 1, 'Western' : 2}

teams = ['ATL', 'BOS', 'BRK', 'CHH', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 'HOU', 'IND',
         'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHO',
         'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS', 'NJN', 'CHA', 'CHO', 'VAN', 'NOK', 'SEA']

team_gen_info_cols = ['Team', 'TeamID', 'Season', 'Games', 'Wins', 'Losses', 'Division', 'DivisionID',
                      'DivisionRank', 'Conference', 'ConferenceID', 'ConferenceRank',
                      'HC1', 'HC1Ws', 'HC1Ls', 'HC2', 'HC2Ws', 'HC2Ls', 'R1Ws', 'R1Ls',
                      'R1Opp', 'R1W', 'R2Ws', 'R2Ls', 'R2Opp', 'R2W', 'R3Ws', 'R3Ls',
                      'R3Opp', 'R3W', 'R4Ws', 'R4Ls', 'R4Opp', 'R4W']

team_stats_cols = ['Team', 'TeamID', 'Season', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%', '2P',
                   '2PA', '2P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK',
                   'TOV', 'PF', 'PTS', 'FGYOY', 'FGAYOY', 'FG%YOY', '3PYOY', '3PAYOY', '3P%YOY',
                   '2PYOY', '2PAYOY', '2P%YOY', 'FTYOY', 'FTAYOY', 'FT%YOY', 'ORBYOY', 'DRBYOY',
                   'TRBYOY', 'ASTYOY', 'STLYOY', 'BLKYOY', 'TOVYOY', 'PFYOY', 'PTSYOY']

team_adv_cols = ['Team', 'TeamID', 'Season', 'PW', 'PL', 'MOV', 'SOS', 'SRS', 'ORtg', 'DRtg',
                 'Pace', 'FTr', '3PAr', 'eFG%', 'TOV%', 'ORB%', 'FT/FGA', 'eFG%A', 'TOV%A',
                 'DRB%A', 'FT/FGAA']


def scrape_team_general_info(soup, team, year):
    general_info = [team, abbr_to_team_id[team], year]
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
        conference_id = conference_to_conf_id[conference]
    elif division in ['Northwest', 'Pacific', 'Southwest', 'Midwest']:
        conference = 'Western'
        conference_id = conference_to_conf_id[conference]
    if division in ['Eastern', 'Western']:
        conference = division
        conference_id = conference_to_conf_id[conference]
        conference_rank = division_rank
        division = 'N/A'
        division_id = 'N/A'
        division_rank = 'N/A'
    else:
        division_id = division_to_div_id[division]
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
            finals_opp = teams_to_abbr[playoffs[0].split('versus')[1].strip()]
            won_finals = finals_wins > finals_losses
            conf_finals_wins, conf_finals_losses = map(int, playoffs[1].split('(')[1].split(')')[0].split('-'))
            conf_finals_opp = teams_to_abbr[playoffs[1].split('versus')[1].strip()]
            won_conf_finals = True
            conf_semis_wins, conf_semis_losses = map(int, playoffs[2].split('(')[1].split(')')[0].split('-'))
            conf_semis_opp = teams_to_abbr[playoffs[2].split('versus')[1].strip()]
            won_conf_semis = True
            first_round_wins, first_round_losses = map(int, playoffs[3].split('(')[1].split(')')[0].split('-'))
            first_round_opp = teams_to_abbr[playoffs[3].split('versus')[1].strip()]
            won_first_round = True
        elif len(playoffs) == 3:
            finals_wins, finals_losses = 'N/A', 'N/A'
            finals_opp = 'N/A'
            won_finals = 'N/A'
            conf_finals_wins, conf_finals_losses = map(int, playoffs[0].split('(')[1].split(')')[0].split('-'))
            conf_finals_opp = teams_to_abbr[playoffs[0].split('versus')[1].strip()]
            won_conf_finals = conf_finals_wins > conf_finals_losses
            conf_semis_wins, conf_semis_losses = map(int, playoffs[1].split('(')[1].split(')')[0].split('-'))
            conf_semis_opp = teams_to_abbr[playoffs[1].split('versus')[1].strip()]
            won_conf_semis = True
            first_round_wins, first_round_losses = map(int, playoffs[2].split('(')[1].split(')')[0].split('-'))
            first_round_opp = teams_to_abbr[playoffs[2].split('versus')[1].strip()]
            won_first_round = True
        elif len(playoffs) == 2:
            finals_wins, finals_losses = 'N/A', 'N/A'
            finals_opp = 'N/A'
            won_finals = 'N/A'
            conf_finals_wins, conf_finals_losses = 'N/A', 'N/A'
            conf_finals_opp = 'N/A'
            won_conf_finals = 'N/A'
            conf_semis_wins, conf_semis_losses = map(int, playoffs[0].split('(')[1].split(')')[0].split('-'))
            conf_semis_opp = teams_to_abbr[playoffs[0].split('versus')[1].strip()]
            won_conf_semis = conf_semis_wins > conf_semis_losses
            first_round_wins, first_round_losses = map(int, playoffs[1].split('(')[1].split(')')[0].split('-'))
            first_round_opp = teams_to_abbr[playoffs[1].split('versus')[1].strip()]
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
            first_round_opp = teams_to_abbr[playoffs[0].split('versus')[1].strip()]
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
    with open('team_stats_test.txt', 'wb') as f:
        f.write(soup.prettify('utf-8'))
    comments = soup.find_all(string=lambda text:isinstance(text, Comment))
    comments = [comment for comment in comments if len(comment) > 200]

    team_stats, team_yoy_improvement = \
        scrape_team_stats(BeautifulSoup(comments[1], 'lxml'))

    opp_stats, opp_yoy_improvement = \
        scrape_opp_stats(BeautifulSoup(comments[1], 'lxml'))

    team_adv_stats = scrape_team_adv_stats(BeautifulSoup(comments[2], 'lxml'))

    return [team, abbr_to_team_id[team], season] + team_stats + team_yoy_improvement,\
           [team, abbr_to_team_id[team], season] + opp_stats + opp_yoy_improvement,\
           [team, abbr_to_team_id[team], season] + team_adv_stats

def scrape_team_stats(soup):
    trs = [tr for tr in soup.find_all('tr')]
    team_stats = [float(td.get_text()) if i in [4,7,10,13] else
                      int(td.get_text()) for i, td in enumerate(trs[1].find_all('td'))][2:]
    team_yoy_improvement = [float(td.get_text().split('%')[0]) if td.get_text() != '' else
                            td.get_text() for td in trs[4].find_all('td')][2:]

    return team_stats, team_yoy_improvement

def scrape_opp_stats(soup):
    trs = [tr for tr in soup.find_all('tr')]
    opp_stats = [float(td.get_text()) if i in [4,7,10,13] else
                      int(td.get_text()) for i, td in enumerate(trs[5].find_all('td'))][2:]
    opp_yoy_improvement = [float(td.get_text().split('%')[0]) if td.get_text() != '' else
                            td.get_text() for td in trs[8].find_all('td')][2:]

    return opp_stats, opp_yoy_improvement

def scrape_team_adv_stats(soup):
    return [int(td.get_text()) if i in [0,1] else float(td.get_text())
            for i, td in enumerate(soup.find_all('tr')[2].find_all('td')[:-2])]

def scrape_team_stats_by_year():
    base_addr = 'http://www.basketball-reference.com/teams/'
    general_info, team_stats, opp_stats, team_adv_stats = [], [], [], []
    for team in teams:
        for year in range(2001, 2017):
            print(year, team)
            addr = base_addr + team + '/' + str(year) + '.html'
            try:
                resp = urlopen(addr)
            except HTTPError as e:
                print(e.code)
                continue
            soup = BeautifulSoup(resp.read(), 'lxml')
            team_general_info = scrape_team_general_info(soup.find(id='info'), team, year)
            general_info.append(team_general_info)
            team_stat, opp_stat, adv_stat = scrape_team_statistics(soup, team, year)
            team_stats.append(team_stat)
            opp_stats.append(opp_stat)
            team_adv_stats.append(adv_stat)

    general_info_df = pd.DataFrame(data=general_info, columns=team_gen_info_cols)
    team_stats_df = pd.DataFrame(data=team_stats, columns=team_stats_cols)
    opp_stats_df = pd.DataFrame(data=opp_stats, columns=team_stats_cols)
    team_adv_stats = pd.DataFrame(data=team_adv_stats, columns=team_adv_cols)

    writer1 = pd.ExcelWriter('team_gen_info.xlsx')
    writer2 = pd.ExcelWriter('team_stats.xlsx')
    writer3 = pd.ExcelWriter('team_opp_stats.xlsx')
    writer4 = pd.ExcelWriter('team_adv_stats.xlsx')
    general_info_df.to_excel(writer1, 'Sheet1')
    team_stats_df.to_excel(writer2, 'Sheet1')
    opp_stats_df.to_excel(writer3, 'Sheet1')
    team_adv_stats.to_excel(writer4, 'Sheet1')
    writer1.save()
    writer2.save()
    writer3.save()
    writer4.save()

def main():
    scrape_team_stats_by_year()

if __name__ == "__main__":
    main()