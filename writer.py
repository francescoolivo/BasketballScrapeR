from abc import ABC, abstractmethod


class Writer(ABC):
    def __init__(self):
        return

    @abstractmethod
    def check_and_insert_league(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_season(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_edition(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_conference(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_game(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_team(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_franchise(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_edition_participant(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_person(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_manager(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_player(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_manager_contract(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_player_contract(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_referee(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_game_referee(self, content: dict):
        pass

    @abstractmethod
    def check_and_insert_actions(self, actions):
        pass

    @abstractmethod
    def check_and_insert_actions_players(self, actions_players):
        pass

    @abstractmethod
    def check_and_insert_actions_type(self, actions_type):
        pass

    @abstractmethod
    def check_and_insert_shots_type(self, shots_type):
        pass

    @abstractmethod
    def check_and_insert_fouls_type(self, fouls_type):
        pass

