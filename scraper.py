from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
import utils
from tqdm import tqdm


class Scraper(ABC):
    def __init__(self):
        self.current_game = None

        return

    @abstractmethod
    def get_seasons(self, **kwargs):
        pass

    @abstractmethod
    def get_games(self, season, **kwargs):
        pass

    @abstractmethod
    def get_actions(self):
        pass

    @abstractmethod
    def clean_actions(self, raw_actions):
        pass

    @abstractmethod
    def get_boxes(self, soup: BeautifulSoup):
        pass

    def get_tadd(self, season_id):
        url = 'https://www.legabasket.it/lba/6/calendario/standings'
        params = {'s': season_id}

        soup = utils.get_soup(url, params=params)
        table = soup.find('table', class_='full-standings')
        tbody = table.find('tbody')

        result = []

        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')

            rank = int(tds[0].text.strip())
            result.append({
                'Team': tds[1].text.strip(),
                'team': '',
                'Conference': '',
                'Division': '',
                'Rank': rank,
                'Playoff': 'Y' if rank <= 8 else 'N',
            })

        return result

    def download_data(self, **kwargs):
        dataframes = dict()
        seasons = self.get_seasons(**kwargs)

        for season in seasons:
            dataframes[season] = dict()

            players_df = pd.DataFrame(
                columns=['Team', 'Player', 'MIN', 'PTS', 'P2M', 'P2A', 'P3M', 'P3A', 'FTM', 'FTA', 'OREB', 'DREB',
                         'AST', 'TOV', 'STL', 'BLK', 'PF', 'PM'])
            team_df = pd.DataFrame(
                columns=['Team', 'MIN', 'PTS', 'P2M', 'P2A', 'P3M', 'P3A', 'FTM', 'FTA', 'OREB', 'DREB', 'AST', 'TOV',
                         'STL', 'BLK', 'PF', 'PM'])
            opponent_df = pd.DataFrame(
                columns=['Team', 'MIN', 'PTS', 'P2M', 'P2A', 'P3M', 'P3A', 'FTM', 'FTA', 'OREB', 'DREB', 'AST', 'TOV',
                         'STL', 'BLK', 'PF', 'PM'])
            pbp_df = pd.DataFrame(
                columns=['game_id', 'data_set', 'date', 'a1', 'a2', 'a3', 'a4', 'a5', 'h1', 'h2', 'h3', 'h4', 'h5',
                         'period', 'home_score', 'away_score', 'remaining_time', 'elapsed_time', 'play_length',
                         'play_id', 'team', 'event_type', 'assist', 'away', 'home', 'block', 'entered', 'left', 'num',
                         'opponent', 'outof', 'player', 'points', 'possession', 'reason', 'result', 'steal', 'type',
                         'shot_distance', 'original_x', 'original_y', 'converted_x', 'converted_y', 'description'])

            tadd = self.get_tadd(season_id=season)
            tadd_df = pd.DataFrame(tadd, columns=['Team', 'team', 'Conference', 'Division', 'Rank', 'Playoff'])

            games = self.get_games(seasons[season])

            for game in tqdm(games):

                self.current_game = game
                url = f'https://www.legabasket.it/game/{game["game_id"]}'

                soup = utils.get_soup(url)

                boxes = self.get_boxes(soup)

                if not boxes:
                    continue

                try:

                    for team in boxes:
                        players_df = pd.concat([players_df, pd.DataFrame(boxes[team]['players'])], ignore_index=True)
                        team_df = pd.concat([team_df, pd.DataFrame(boxes[team]['team'])], ignore_index=True)
                        opponent_df = pd.concat([opponent_df, pd.DataFrame(boxes[team]['opponent'])], ignore_index=True)

                except TypeError:
                    continue

                raw_actions = self.get_actions()
                actions = self.clean_actions(raw_actions)

                pbp_df = pd.concat([pbp_df, pd.DataFrame(actions)], ignore_index=True)

            dataframes[season]['Pbox'] = self.summarize_players_df(players_df)
            dataframes[season]['Tbox'] = self.summarize_teams_df(team_df)
            dataframes[season]['Obox'] = self.summarize_teams_df(opponent_df)
            dataframes[season]['PBP'] = pbp_df
            dataframes[season]['Tadd'] = tadd_df

        return dataframes

    def summarize_players_df(self, df):
        ag = df.groupby(['Team', 'Player']).sum()
        ag['GP'] = df[df['MIN'] > 0].groupby(['Team', 'Player'])['MIN'].count()
        ag['GP'] = ag['GP'].fillna(0).astype('int')
        ag['P2p'] = 100 * ag['P2M'] / ag['P2A']
        ag['P3p'] = 100 * ag['P3M'] / ag['P3A']
        ag['FTp'] = 100 * ag['FTM'] / ag['FTA']
        ag = ag[
            ['GP', 'MIN', 'PTS', 'P2M', 'P2A', 'P2p', 'P3M', 'P3A', 'P3p', 'FTM', 'FTA', 'FTp', 'OREB', 'DREB', 'AST',
             'TOV', 'STL', 'BLK', 'PF', 'PM']]

        return ag

    def summarize_teams_df(self, df, opponent=False):
        ag = df.groupby(['Team']).sum()
        ag['GP'] = df[df['MIN'] > 0].groupby(['Team'])['MIN'].count()

        if not opponent:
            ag['W'] = df[df['PM'] > 0].groupby(['Team'])['PM'].count()
            ag['L'] = df[df['PM'] < 0].groupby(['Team'])['PM'].count()
        else:
            ag['W'] = df[df['PM'] < 0].groupby(['Team'])['PM'].count()
            ag['L'] = df[df['PM'] > 0].groupby(['Team'])['PM'].count()
            ag['PM'] = - ag['PM']

        ag['GP'] = ag['GP'].fillna(0).astype('int')
        ag['W'] = ag['W'].fillna(0).astype('int')
        ag['L'] = ag['L'].fillna(0).astype('int')
        ag['P2p'] = 100 * ag['P2M'] / ag['P2A']
        ag['P3p'] = 100 * ag['P3M'] / ag['P3A']
        ag['FTp'] = 100 * ag['FTM'] / ag['FTA']
        ag = ag[['GP', 'MIN', 'PTS', 'W', 'L', 'P2M', 'P2A', 'P2p', 'P3M', 'P3A', 'P3p', 'FTM', 'FTA', 'FTp', 'OREB',
                 'DREB', 'AST', 'TOV', 'STL', 'BLK', 'PF', 'PM']]

        return ag
