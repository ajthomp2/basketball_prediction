# ****************************************** #
# *******     TEAM TABLE COLUMNS     ******* #
# ****************************************** #

team_stats_cols = ['TeamID', 'Season', 'FGPerG', 'FGAPerG', 'FGPercent', '3PPerG', '3PAPerG', '3PPercent', '2PPerG',
                   '2PAPerG', '2PPercent', 'FTPerG', 'FTAPerG', 'FTPercent', 'ORBPerG', 'DRBPerG', 'TRBPerG',
                   'ASTPerG', 'STLPerG', 'BLKPerG', 'TOVPerG', 'PFPerG', 'PTSPerG', 'FGPerGYOYPercentChange',
                   'FGAPerGYOYPercentChange', 'FGPercentYOYPercentChange', '3PPerGYOYPercentChange',
                   '3PAPerGYOYPercentChange', '3PPercentYOYPercentChange', '2PPerGYOYPercentChange',
                   '2PAPerGYOYPercentChange', '2PPercentYOYPercentChange', 'FTPerGYOYPercentChange',
                   'FTAPerGYOYPercentChange', 'FTPercentYOYPercentChange', 'ORBPerGYOYPercentChange',
                   'DRBPerGYOYPercentChange', 'TRBPerGYOYPercentChange', 'ASTPerGYOYPercentChange',
                   'STLPerGYOYPercentChange', 'BLKPerGYOYPercentChange', 'TOVPerGYOYPercentChange',
                   'PFPerGYOYPercentChange', 'PTSPerGYOYPercentChange']

team_adv_cols = ['TeamID', 'Season', 'PythagoreanWins', 'PythagoreanLosses', 'MarginOfVictory', 'StrengthOfSchedule',
                 'SimpleRatingSystem', 'ORtg', 'DRtg', 'Pace', 'FTr', '3PAr', 'eFGPercent', 'TOVPercent', 'ORBPercent',
                 'FTPerFGA', 'eFGPercentAgainst', 'TOVPercentAgainst', 'DRBPercentAgainst', 'FTPerFGAAgainst']

team_lineups_cols = ['TeamID', 'Season', 'GameTypeID', 'NumPlayers', 'Player1ID',
                     'Player2ID', 'Player3ID', 'Player4ID', 'Player5ID', 'MP', 'PlusMinusPTSPer100Poss',
                     'PlusMinusFGPer100Poss', 'PlusMinusFGAPer100Poss', 'PlusMinusFGPercentPer100Poss',
                     'PlusMinus3PPer100Poss', 'PlusMinus3PAPer100Poss', 'PlusMinus3PPercentPer100Poss',
                     'PlusMinuseFGPercentPer100Poss', 'PlusMinusFTPer100Poss', 'PlusMinusFTAPer100Poss',
                     'PlusMinusFTPercentPer100Poss', 'PlusMinusORBPer100Poss', 'PlusMinusORBPercentPer100Poss',
                     'PlusMinusDRBPer100Poss', 'PlusMinusDRBPercentPer100Poss', 'PlusMinusTRBPer100Poss',
                     'PlusMinusTRBPercentPer100Poss', 'PlusMinusASTPer100Poss', 'PlusMinusSTLPer100Poss',
                     'PlusMinusBLKPer100Poss', 'PlusMinusTOVPer100Poss', 'PlusMinusPFPer100Poss']

team_gen_info_cols = ['Team', 'TeamID', 'Season', 'Games', 'Wins', 'Losses', 'Division', 'DivisionID',
                      'DivisionRank', 'Conference', 'ConferenceID', 'ConferenceRank',
                      'HeadCoach1', 'HeadCoach1Wins', 'HeadCoach1Losses', 'HeadCoach2', 'HeadCoach2Wins',
                      'HeadCoach2Losses', 'Round1Wins', 'Round1Losses', 'Round1Opp', 'Round1Won', 'Round2Wins',
                      'Round2Losses', 'Round2Opp', 'Round2Won', 'Round3Wins', 'Round3Losses', 'Round3Opp',
                      'Round3Won', 'Round4Wins', 'Round4Losses', 'Round4Opp', 'Round4Won']


# ******************************************** #
# *******     PLAYER TABLE COLUMNS     ******* #
# ******************************************** #

general_player_info_cols = ['PlayerID', 'Player', 'PlayerLink', 'BirthDate', 'Position1', 'Position2',
                            'Position3', 'DraftPick', 'Height', 'Weight', 'College']

per_game_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'MPPerG', 'FGPerG',
                 'FGAPerG', 'FGPercent', '3PPerG', '3PAPerG', '3PPercent', '2PPerG', '2PAPerG', '2PPercent',
                 'eFGPercent', 'FTPerG', 'FTAPerG', 'FTPercent', 'ORBPerG', 'DRBPerG', 'TRBPerG', 'ASTPerG',
                 'STLPerG', 'BLKPerG', 'TOVPerG', 'PFPerG', 'PTSPerG']

pbp_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'PGPercent', 'SGPercent', 'SFPercent',
            'PFPercent', 'CPercent', 'PlusMinusPer100PossOnCourt', 'PlusMinusPer100PossOnOff', 'BadPassTO',
            'LostBallTO', 'OtherTO', 'ShootingFoulsCommitted', 'BlockingFoulsCommitted', 'OffensiveFoulsCommitted',
            'TakeFoulsCommitted', 'PtsGenByAst', 'ShootingFoulsDrawn', 'And1s', 'BlockedFGA']

per_36_min_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'FGPer36',
                   'FGAPer36', 'FGPercent', '3PPer36', '3PAPer36', '3PPercent', '2PPer36', '2PAPer36', '2PPercent',
                   'FTPer36', 'FTAPer36', 'FTPercent', 'ORBPer36', 'DRBPer36', 'TRBPer36', 'ASTPer36',
                   'STLPer36', 'BLKPer36', 'TOVPer36', 'PFPer36', 'PTSPer36']

per_100_poss_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'FGPer100Poss', 'FGAPer100Poss',
                     'FGPercent', '3PPer100Poss', '3PAPer100Poss', '3PPercent', '2PPer100Poss', '2PAPer100Poss',
                     '2PPercent', 'FTPer100Poss', 'FTAPer100Poss', 'FTPercent', 'ORBPer100Poss', 'DRBPer100Poss',
                     'TRBPer100Poss', 'ASTPer100Poss', 'STLPer100Poss', 'BLKPer100Poss', 'TOVPer100Poss',
                     'PFPer100Poss', 'PTSPer100Poss', 'ORtgPer100Poss', 'DRtgPer100Poss']

shooting_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'FGPercent', 'AvgShotDist',
                 '2PAPercent', 'PercentFGA0to2ft', 'PercentFGA3to9ft', 'PercentFGA10to15ft', 'PercentFGA16Plusftto3',
                 'PercentFGA3P', '2PFGPercent', 'FGPercent0to2ft', 'FGPercent3to9ft', 'FGPercent10to15ft',
                 'FGPercent16Plusftto3', '3PFGPercent', 'Percent2PAAstByOthers', 'Percent3PAAstByOthers',
                 'Percent3PAFromCorner', '3PPercentFromCorner']

adv_cols = ['PlayerID', 'Season', 'GameTypeID', 'Age', 'TeamID', 'PER', 'TSPercent', '3PAr', 'FTr', 'ORBPercent',
            'DRBPercent', 'TRBPercent', 'ASTPercent', 'STLPercent', 'BLKPercent', 'TOVPercent', 'USGPercent', 'OWS',
            'DWS', 'WS', 'WSPer48', 'OBRM', 'DBPM', 'BPM', 'VORP']

on_off_cols = ['PlayerID', 'Season', 'GameTypeID', 'TeamID', 'OnCourtTeameFGPercent', 'OnCourtTeamORBPercent',
               'OnCourtTeamDRBPercent', 'OnCourtTeamTRBPercent', 'OnCourtTeamASTPercent', 'OnCourtTeamSTLPercent',
               'OnCourtTeamBLKPercent', 'OnCourtTeamTOVPercent', 'OnCourtTeamORtg', 'OnCourtOppeFGPercent',
               'OnCourtOppORBPercent', 'OnCourtOppDRBPercent', 'OnCourtOppTRBPercent', 'OnCourtOppASTPercent',
               'OnCourtOppSTLPercent', 'OnCourtOppBLKPercent', 'OnCourtOppTOVPercent', 'OnCourtOppORtg',
               'OnCourtDiffeFGPercent', 'OnCourtDiffORBPercent', 'OnCourtDiffDRBPercent', 'OnCourtDiffTRBPercent',
               'OnCourtDiffASTPercent', 'OnCourtDiffSTLPercent', 'OnCourtDiffBLKPercent', 'OnCourtDiffTOVPercent',
               'OnCourtDiffORtg', 'OffCourtTeameFGPercent', 'OffCourtTeamORBPercent', 'OffCourtTeamDRBPercent',
               'OffCourtTeamTRBPercent', 'OffCourtTeamASTPercent', 'OffCourtTeamSTLPercent', 'OffCourtTeamBLKPercent',
               'OffCourtTeamTOVPercent', 'OffCourtTeamORtg', 'OffCourtOppeFGPercent', 'OffCourtOppORBPercent',
               'OffCourtOppDRBPercent', 'OffCourtOppTRBPercent', 'OffCourtOppASTPercent', 'OffCourtOppSTLPercent',
               'OffCourtOppBLKPercent', 'OffCourtOppTOVPercent', 'OffCourtOppORtg', 'OffCourtDiffeFGPercent',
               'OffCourtDiffORBPercent', 'OffCourtDiffDRBPercent', 'OffCourtDiffTRBPercent', 'OffCourtDiffASTPercent',
               'OffCourtDiffSTLPercent', 'OffCourtDiffBLKPercent', 'OffCourtDiffTOVPercent', 'OffCourtDiffORtg',
               'OnOffTeameFGPercent', 'OnOffTeamORBPercent', 'OnOffTeamDRBPercent', 'OnOffTeamTRBPercent',
               'OnOffTeamASTPercent', 'OnOffTeamSTLPercent', 'OnOffTeamBLKPercent', 'OnOffTeamTOVPercent',
               'OnOffTeamORtg', 'OnOffOppeFGPercent', 'OnOffOppORBPercent', 'OnOffOppDRBPercent', 'OnOffOppTRBPercent',
               'OnOffOppASTPercent', 'OnOffOppSTLPercent', 'OnOffOppBLKPercent', 'OnOffOppTOVPercent', 'OnOffOppORtg',
               'OnOffDiffeFGPercent', 'OnOffDiffORBPercent', 'OnOffDiffDRBPercent', 'OnOffDiffTRBPercent',
               'OnOffDiffASTPercent', 'OnOffDiffSTLPercent', 'OnOffDiffBLKPercent', 'OnOffDiffTOVPercent',
               'OnOffDiffORtg']


# ****************************************** #
# *******     GAME TABLE COLUMNS     ******* #
# ****************************************** #

team_game_stats_cols = ['GameID', 'Season', 'Date', 'GameTypeID', 'Home', 'Team', 'TeamID', 'Q1Score', 'Q2Score',
                        'Q3Score', 'Q4Score', 'OTScore', 'FinalScore', 'FG', 'FGA', 'FGPercent', '3P', '3PA',
                        '3PPercent', 'FT', 'FTA', 'FTPercent', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF',
                        'Pace', 'TSPercent', 'eFGPercent', '3PAr', 'FTr', 'ORBPercent', 'DRBPercent', 'TRBPercent',
                        'ASTPercent', 'STLPercent', 'BLKPercent', 'TOVPercent', 'ORtg', 'DRtg']

player_stats_cols = ['GameID', 'PlayerID', 'PlayerLink', 'TeamID', 'MP', 'FG', 'FGA', 'FGPercent', '3P', '3PA',
                     '3PPercent', 'FT', 'FTA', 'FTPercent', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF',
                     'PTS', 'PlusMinus', 'TSPercent', 'eFGPercent', '3PAr', 'FTr', 'ORBPercent', 'DRBPercent',
                     'TRBPercent', 'ASTPercent', 'STLPercent', 'BLKPercent', 'TOVPercent', 'USGPercent', 'ORtg', 'DRtg']

