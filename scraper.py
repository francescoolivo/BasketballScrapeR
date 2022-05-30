from abc import ABC, abstractmethod
from writer import Writer


class InvalidPlayerException(Exception):
    pass


class Scraper(ABC):
    def __init__(self, writer: Writer):
        self.writer = writer
        self.conferences_cache = dict()
        self.franchises_cache = dict()
        self.teams_cache = dict()
        self.players_cache = dict()
        self.managers_cache = dict()
        self.referees_cache = dict()
        self.current_season = None
        self.current_game = None
        return

    @abstractmethod
    def get_games(self, **kwargs):
        pass

    @abstractmethod
    def get_actions(self):
        pass

    @abstractmethod
    def clean_actions(self, raw_actions):
        pass

    @abstractmethod
    def insert_actions(self, actions):
        pass

    @abstractmethod
    def get_boxes(self, game_id):
        pass

    @abstractmethod
    def get_tadd(self):
        pass
