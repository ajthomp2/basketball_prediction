from bs4 import BeautifulSoup
from bs4 import Comment
import pandas as pd
import numpy as np
from urllib.request import urlopen
import requests
import sqlite3 as lite
import sys

#********************************************************#
#********************************************************#
# NOTES:
#
#********************************************************#
#********************************************************#


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
                 'New Orleans/Oklahoma City Hornets': 'NOK', 'Seattle SuperSonics': 'SEA'}
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
                 'NOK': 'New Orleans/Oklahoma City Hornets', 'SEA': 'Seattle SuperSonics', 'CHO': 'Charlotte Hornets'}
teams_to_team_id = {'Atlanta Hawks': 1, 'Boston Celtics': 2, 'Brooklyn Nets': 3,
                    'Charlotte Hornets': 4, 'Chicago Bulls': 5, 'Cleveland Cavaliers': 6,
                    'Dallas Mavericks': 7, 'Denver Nuggets': 8, 'Detroit Pistons': 9,
                    'Golden State Warriors': 10, 'Houston Rockets': 11, 'Indiana Pacers': 12,
                    'Los Angeles Clippers': 13, 'Los Angeles Lakers': 14, 'Memphis Grizzlies': 15,
                    'Miami Heat': 16, 'Milwaukee Bucks': 17, 'Minnesota Timberwolves': 18,
                    'New Orleans Pelicans': 19, 'New York Nnicks': 20, 'Oklahoma City Thunder': 21,
                    'Orlando Magic': 22, 'Philadelphia 76ers': 23, 'Phoenix Suns': 24,
                    'Portland Trail Blazers': 25, 'Sacramento Kings': 26, 'San Antonio Spurs': 27,
                    'Toronto Raptors': 28, 'Utah Jazz': 29, 'Washington Wizards': 30,
                    'New Jersey Nets': 3, 'Charlotte Bobcats': 4, 'Vancouver Grizzlies': 15,
                    'New Orleans/Oklahoma City Hornets': 19, 'Seattle SuperSonics': 21}
abbr_to_team_id = {'ATL': 1, 'BOS': 2, 'BRK': 3, 'CHH': 4, 'CHI': 5, 'CLE': 6,
                   'DAL': 7, 'DEN': 8, 'DET': 9, 'GSW': 10, 'HOU': 11, 'IND': 12,
                   'LAC': 13, 'LAL': 14, 'MEM': 15, 'MIA': 16, 'MIL': 17, 'MIN': 18,
                   'NOP': 19, 'NYK': 20, 'OKC': 21, 'ORL': 22, 'PHI': 23, 'PHO': 24,
                   'POR': 25, 'SAC': 26, 'SAS': 27, 'TOR': 28, 'UTA': 29, 'WAS': 30,
                   'NJN': 3, 'CHA': 4, 'CHO': 4, 'VAN': 15, 'NOK': 19, 'SEA': 21}

team_stats_cols = ['GameID', 'Season', 'Date', 'GameType', 'GameTypeID',
                   'Team', 'TeamID', 'Q1Score', 'Q2Score', 'Q3Score', 'Q4Score',
                   'OTScore', 'FinalScore', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                   'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV',
                   'PF', 'TS%', 'eFG%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%',
                   'STL%', 'BLK%', 'TOV%', 'ORtg', 'DRtg']

player_stats_cols = ['GameID', 'Player', 'PlayerID', 'PlayerLink' 'Team', 'TeamID', 'MP', 'FG',
                     'FGA', 'FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB',
                     'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', '+/-', 'TS%', 'eFG%', '3PAr', 'FTr',
                     'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%', 'ORtg', 'DRtg']

months = ['october', 'november', 'december', 'january',
          'february', 'march', 'april', 'may', 'june']

test_months = ['october']

player_to_player_id = {}
next_p_id = 1


def scrape_player_stats_per_game(addr, game_id):
    print(game_id)
    player_stats = []
    resp = urlopen(addr).read()
    soup = BeautifulSoup(resp, 'lxml')

    home_team, away_team = get_home_away_teams(soup.h1)

    home_team_div_id = "box_" + home_team.lower() + "_basic"
    away_team_div_id = "box_" + away_team.lower() + "_basic"
    basic_player_stats = scrape_basic_player_stats(soup.find(id=home_team_div_id).find('tbody'), \
                                                   soup.find(id=away_team_div_id).find('tbody'),
                                                   home_team, away_team)

    home_team_div_id = "box_" + home_team.lower() + "_advanced"
    away_team_div_id = "box_" + away_team.lower() + "_advanced"
    adv_player_stats = scrape_adv_player_stats(soup.find(id=home_team_div_id).find('tbody'), \
                                               soup.find(id=away_team_div_id).find('tbody'),
                                               home_team, away_team)

    for basic, adv in zip(basic_player_stats, adv_player_stats):
        player_stats.append([game_id] + basic + adv)

    return player_stats

def scrape_adv_player_stats(home_soup, away_soup, home_team, away_team):
    home_player_stats, away_player_stats = [], []
    int_ind = [13, 14]
    global next_p_id
    for tr in home_soup.find_all('tr'):
        if tr.find('th').get_text().strip() == 'Reserves':
            continue
        player_name = tr.find('th').get_text().strip()
        player_link = tr.a.get('href')
        if player_link in player_to_player_id:
            player_id = player_to_player_id[player_link]
        else:
            player_to_player_id[player_link] = next_p_id
            player_id = next_p_id
            next_p_id += 1
        temp_home_player_stats = []
        for i, td in enumerate(tr.find_all('td')):
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
        if temp_home_player_stats != []:
            #temp_home_player_stats.insert(0, abbr_to_team_id[home_team])
            #temp_home_player_stats.insert(0, home_team)
            #temp_home_player_stats.insert(0, player_id)
            #temp_home_player_stats.insert(0, player_name)
            home_player_stats.append(temp_home_player_stats)

    for tr in away_soup.find_all('tr'):
        if tr.find('th').get_text().strip() == 'Reserves':
            continue
        player_name = tr.find('th').get_text().strip()
        player_link = tr.a.get('href')
        if player_link in player_to_player_id:
            player_id = player_to_player_id[player_link]
        else:
            player_to_player_id[player_link] = next_p_id
            player_id = next_p_id
            next_p_id += 1
        temp_away_player_stats = []
        for i, td in enumerate(tr.find_all('td')):
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
        if temp_away_player_stats != []:
            #temp_away_player_stats.insert(0, abbr_to_team_id[away_team])
            #temp_away_player_stats.insert(0, away_team)
            #temp_away_player_stats.insert(0, player_id)
            #temp_away_player_stats.insert(0, player_name)
            away_player_stats.append(temp_away_player_stats)

    #print(home_player_stats + away_player_stats)

    return home_player_stats + away_player_stats

def scrape_basic_player_stats(home_soup, away_soup, home_team, away_team):
    home_player_stats, away_player_stats = [], []
    float_ind = [3,6,9]
    global next_p_id
    for tr in home_soup.find_all('tr'):
        if tr.find('th').get_text().strip() == 'Reserves':
            continue
        player_name = tr.find('th').get_text().strip()
        player_link = tr.a.get('href')
        if player_link in player_to_player_id:
            player_id = player_to_player_id[player_link]
        else:
            player_to_player_id[player_link] = next_p_id
            player_id = next_p_id
            next_p_id += 1
        temp_home_player_stats = []
        for i, td in enumerate(tr.find_all('td')):
            if i == 0:
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
        if temp_home_player_stats != []:
            temp_home_player_stats.insert(0, abbr_to_team_id[home_team])
            temp_home_player_stats.insert(0, home_team)
            temp_home_player_stats.insert(0, player_link)
            temp_home_player_stats.insert(0, player_id)
            temp_home_player_stats.insert(0, player_name)
            home_player_stats.append(temp_home_player_stats)

    for tr in away_soup.find_all('tr'):
        if tr.find('th').get_text().strip() == 'Reserves':
            continue
        player_name = tr.find('th').get_text().strip()
        player_link = tr.a.get('href')
        if player_link in player_to_player_id:
            player_id = player_to_player_id[player_link]
        else:
            player_to_player_id[player_link] = next_p_id
            player_id = next_p_id
            next_p_id += 1
        temp_away_player_stats = []
        for i, td in enumerate(tr.find_all('td')):
            if i == 0:
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
        if temp_away_player_stats != []:
            temp_away_player_stats.insert(0, abbr_to_team_id[away_team])
            temp_away_player_stats.insert(0, away_team)
            temp_away_player_stats.insert(0, player_link)
            temp_away_player_stats.insert(0, player_id)
            temp_away_player_stats.insert(0, player_name)
            away_player_stats.append(temp_away_player_stats)

    #print(home_player_stats + away_player_stats, '\n')

    return home_player_stats + away_player_stats






####____________________________________________________________________________####
####----------------------------------------------------------------------------####
####----------------------------------------------------------------------------####

#   These functions scrape the team statistics per game

def scrape_team_stats_per_game(addr, game_id, season):
    resp = urlopen(addr).read()
    soup = BeautifulSoup(resp, 'lxml')
    comments = soup.find_all(string=lambda text:isinstance(text, Comment))
    comments = [comment for comment in comments if len(comment) > 500]

    home_team, away_team = get_home_away_teams(soup.h1)
    game_date = scrape_game_date(soup.find(class_='scorebox_meta'))
    print(game_date)
    home_score, away_score = scrape_game_score(BeautifulSoup(comments[1], 'lxml'), home_team)

    game_type = get_game_type(BeautifulSoup(comments[0], 'lxml').encode('utf-8')[:200])

    home_team_div_id = "box_" + home_team.lower() + "_basic"
    away_team_div_id = "box_" + away_team.lower() + "_basic"
    home_team_basic_stats, away_team_basic_stats = scrape_basic_team_stats(soup.find(id=home_team_div_id).find('tfoot'), \
                                                                           soup.find(id=away_team_div_id).find('tfoot'))

    home_team_div_id = "box_" + home_team.lower() + "_advanced"
    away_team_div_id = "box_" + away_team.lower() + "_advanced"
    home_team_adv_stats, away_team_adv_stats = scrape_adv_team_stats(soup.find(id=home_team_div_id).find('tfoot'), \
                                                                           soup.find(id=away_team_div_id).find('tfoot'))

    #print([game_id] + [int(season)] + [game_date] + game_type + home_score + home_team_basic_stats + home_team_adv_stats)
    #print([game_id] + [int(season)] + [game_date] + game_type + away_score + away_team_basic_stats + away_team_adv_stats)

    return [game_id] + [int(season)] + [game_date] + game_type + home_score + home_team_basic_stats + home_team_adv_stats, \
           [game_id] + [int(season)] + [game_date] + game_type + away_score + away_team_basic_stats + away_team_adv_stats

def get_game_type(string):
    if 'NBA Scores' in str(string):
        return ['Regular Season', 1]
    else:
        return ['PostSeason', 2]

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
    float_ind = [3,6,9]

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
        if td.a != None:
            scoring.append(td.get_text())
        else:
            scoring.append(int(td.get_text()))

    split = int(len(scoring) / 2)
    team1_score = scoring[:split]
    team2_score = scoring[split:]

    if len(team1_score) == 6:
        team1_score.insert(-1, 'N/A')
        team2_score.insert(-1, 'N/A')
    if len(team1_score) == 8:
        team1_score[-3] += team1_score[-2]
        del team1_score[-2]
        team2_score[-3] += team2_score[-2]
        del team2_score[-2]
    if len(team1_score) == 9:
        team1_score[-4] += team1_score[-2]
        del team1_score[-2]
        team1_score[-3] += team1_score[-2]
        del team1_score[-2]
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

def get_home_away_teams(header):
    away_team = header.get_text().split(' at ')[0].strip()
    home_team = header.get_text().split(' at ')[1].split('Box')[0].strip()
    return teams_to_abbr[home_team], teams_to_abbr[away_team]

####----------------------------------------------------------------------------####
####----------------------------------------------------------------------------####
####____________________________________________________________________________####




def scrape_team_and_player_stats_by_game():
    base_addr = 'http://www.basketball-reference.com'
    game_id = 1
    team_game_stats, player_game_stats = [], []
    year = 2001
    #for year in range(2002, 2005):
    year_months = months
    if year in [2005, 2006, 2012]:
        year_months.remove('october')
    elif year == 2012:
        year_months.remove('november')
    print(year_months)
    for month in year_months:
        print(month, year)
        addr = base_addr + '/leagues/NBA_' + str(year) \
              + '_games-'+ month + '.html'
        resp = urlopen(addr).read()
        soup = BeautifulSoup(resp, 'lxml')
        tbody = soup.find('tbody')

        for link in tbody.find_all('a'):
            if link.get_text() == 'Box Score':
                box_score_addr = link.get('href')
                game_addr = base_addr + box_score_addr
                home_team_stats, away_team_stats = scrape_team_stats_per_game(game_addr, game_id, year)
                team_game_stats.append(home_team_stats)
                team_game_stats.append(away_team_stats)
                basic_player_stats = scrape_player_stats_per_game(game_addr, game_id)
                player_game_stats += basic_player_stats
                game_id += 1

    player_game_stats_df = pd.DataFrame(data=player_game_stats,
                                        columns=player_stats_cols)
    team_game_stats_df = pd.DataFrame(data=team_game_stats,
                                      columns=team_stats_cols)

    writer = pd.ExcelWriter('game_stats_2001.xlsx')
    team_game_stats_df.to_excel(writer, 'Sheet1')
    player_game_stats_df.to_excel(writer, 'Sheet2')
    writer.save()

def main():
    scrape_team_and_player_stats_by_game()

if __name__ == "__main__":
    main()

