from abc import ABC, abstractmethod
from bs4 import BeautifulSoup


class Scraper(ABC):
    def __init__(self):
        self.current_game = None
        self.starters = None

        return

    @abstractmethod
    def get_seasons(self, **kwargs):
        pass

    @abstractmethod
    def get_games(self, season, **kwargs):
        pass

    @abstractmethod
    def get_starters(self, soup):
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

    @abstractmethod
    def get_tadd(self, season_id):
        pass

    @abstractmethod
    def download_data(self, **kwargs):
        pass

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

        ag['GP'] = ag['GP'].fillna(0).astype('int')
        ag['W'] = ag['W'].fillna(0).astype('int')
        ag['L'] = ag['L'].fillna(0).astype('int')
        ag['P2p'] = 100 * ag['P2M'] / ag['P2A']
        ag['P3p'] = 100 * ag['P3M'] / ag['P3A']
        ag['FTp'] = 100 * ag['FTM'] / ag['FTA']
        ag = ag[['GP', 'MIN', 'PTS', 'W', 'L', 'P2M', 'P2A', 'P2p', 'P3M', 'P3A', 'P3p', 'FTM', 'FTA', 'FTp', 'OREB',
                 'DREB', 'AST', 'TOV', 'STL', 'BLK', 'PF', 'PM']]

        return ag
