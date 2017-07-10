import numpy as np

months = ['october', 'november', 'december', 'january',
          'february', 'march', 'april', 'may', 'june']

positions_to_abbr = {'Point Guard': 'PG', 'Shooting Guard': 'SG', 'Small Forward': 'SF',
                     'Power Forward': 'PF', 'Center': 'C', np.nan: np.nan}

division_to_div_id = {'Atlantic': 1, 'Central': 2, 'Southeast': 3,
                      'Northwest': 4, 'Pacific': 5, 'Southwest': 6, 'Midwest': 7}
conference_to_conf_id = {'Eastern': 1, 'Western': 2}