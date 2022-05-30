import json
import math
import os
from datetime import timedelta

import requests as requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

import utils
from scraper import Scraper
from writer import Writer


class LBAScraper(Scraper):

    def __init__(self, writer: Writer):
        super().__init__(writer)
        self.current_game = dict()

    def get_games(self, **kwargs):
        pass

    def get_actions(self):
        game = self.current_game
        url = f'https://www.legabasket.it/match/{game["game_id"]}/pbp'
        actions = []

        session = requests.Session()
        retry = Retry(connect=10, backoff_factor=2)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        period = 1
        period_url = os.path.join(url, str(period))
        try:
            response = session.get(period_url).json()
        except json.decoder.JSONDecodeError:
            print(period_url)
            exit()
        while response['data']['pbp'] is not None and response['data']['pbp']:
            actions += response['data']['pbp']
            period += 1
            period_url = os.path.join(url, str(period))
            try:
                response = session.get(period_url).json()
            except json.decoder.JSONDecodeError:
                continue

        return actions

    def get_starters(self, home_team: bool):
        url = f'https://www.legabasket.it/game/{self.current_game["game_id"]}/scores'
        soup = utils.get_soup(url)

        while soup is None:
            soup = utils.get_soup(url)

        scores_div = soup.find('div', id='scores')

        if home_team:
            table = scores_div.find('table', id='ht_match_scores').find_next('tbody')

        else:
            table = scores_div.find('table', id='vt_match_scores').find_next('tbody')

        starters = list()

        for tr in table.find_all('tr'):
            if tr.find_all_next('td')[3].find('i'):
                name = tr.find_all_next('td')[0].find('span', {'class': 'scores_player_name'}).text.title()
                surname = tr.find_all_next('td')[0].find('span', {'class': 'scores_player_surname'}).text.title()

                starters.append(' '.join([name, surname]))

        if not starters:
            team = 'home' if home_team else 'away'
            print(f"Could not find starters for {team} team in game {self.current_game['game_id']}")
            print(table)
            exit(1)
        return starters

    def handle_substitutions(self, raw_actions):
        actions = []

        home_team_players = self.get_starters(home_team=True)
        away_team_players = self.get_starters(home_team=False)

        # To handle cases where the same substitution is repeated
        substitutions = set()

        # add a flag to an action so that we can ignore it while iterating
        for raw_action in raw_actions:
            raw_action['checked'] = False

        for raw_action in raw_actions:

            if raw_action['description'] not in ['Ingresso', 'Uscita']:
                raw_action['home_players'] = home_team_players.copy()
                raw_action['away_players'] = away_team_players.copy()
                actions.append(raw_action)

            elif raw_action['description'] in ['Ingresso', 'Uscita'] and not raw_action['checked']:

                type_to_look_for = 'Uscita' if raw_action['description'] == 'Ingresso' else 'Ingresso'

                raw_action_index = raw_actions.index(raw_action)

                # looking for the next out substitution for the team
                for next_raw_action in raw_actions[raw_action_index:]:
                    if next_raw_action['description'] == type_to_look_for and raw_action['team_name'] == \
                            next_raw_action['team_name'] and not next_raw_action['checked']:

                        if type_to_look_for == 'Uscita':
                            player_in = ' '.join(
                                [raw_action['player_name'].title(), raw_action['player_surname'].title()])
                            player_out = ' '.join(
                                [next_raw_action['player_name'].title(), next_raw_action['player_surname'].title()])
                        else:
                            player_in = ' '.join(
                                [next_raw_action['player_name'].title(), next_raw_action['player_surname'].title()])
                            player_out = ' '.join(
                                [raw_action['player_name'].title(), raw_action['player_surname'].title()])

                        sub = f"SUB: {player_in} for {player_out} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"
                        sub_in = f"IN: {player_in} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"
                        sub_out = f"OUT: {player_out} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"

                        players = {
                            1: home_team_players,
                            0: away_team_players
                        }

                        team_descriptions = {
                            1: 'home',
                            0: 'away'
                        }

                        if sub_in in substitutions or sub_out in substitutions:
                            next_raw_action['checked'] = True
                            break
                        elif player_out not in players[raw_action['home_club']]:
                            print(
                                f"Error: {player_out} should be on court for {team_descriptions[raw_action['home_club']]} team but he is not. On court players are {players[raw_action['home_club']]}")
                            print(raw_action)
                            exit(1)
                        elif player_in in players[raw_action['home_club']]:
                            print(
                                f"Error: {player_in} is already on court for {team_descriptions[raw_action['home_club']]} team but he should not. On court players are {players[raw_action['home_club']]}")
                            print(raw_action)
                            exit(1)
                        else:
                            player_out_index = players[raw_action['home_club']].index(player_out)
                            players[raw_action['home_club']][player_out_index] = player_in
                            substitutions.add(sub_out)
                            substitutions.add(sub_in)

                        next_raw_action['checked'] = True

                        raw_action['player_in'] = player_in
                        raw_action['player_out'] = player_out
                        raw_action['home_players'] = home_team_players.copy()
                        raw_action['away_players'] = away_team_players.copy()
                        raw_action['description'] = 'Substitution'

                        actions.append(raw_action)

                        break

        return actions

    def add_ft_count(self, raw_actions):

        player = None
        num = 0

        for raw_action in raw_actions:
            if raw_action['description'].lower() in {'tiro libero sbagliato', 'tiro libero segnato'}:
                player_ra = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title()
                if player is None or player_ra != player:
                    player = player_ra
                    num = 1
                    raw_action['num'] = num
                else:
                    num += 1
                    raw_action['num'] = num
            else:
                player = None
                num = 0

        player = None
        outof = 0

        for raw_action in raw_actions[::-1]:
            if raw_action['description'].lower() in {'tiro libero sbagliato', 'tiro libero segnato'}:
                player_ra = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title()
                if player is None or player_ra != player:
                    player = player_ra
                    outof = raw_action['num']
                    raw_action['outof'] = outof
                else:
                    raw_action['outof'] = outof
            else:
                player = None
                outof = 0

        return raw_actions



    def clean_actions(self, raw_actions):
        actions = []

        home_score = 0
        away_score = 0

        action_start = timedelta(minutes=0)
        period_start = 1

        for raw_action in raw_actions:
            # {
            # "action_id":106,
            # "description":"2 punti segnato",
            # "player_id":6518,
            # "team_id":1516,
            # "home_club":0,
            # "in_area":true,
            # "dunk":0,
            # "seconds":21,
            # "minute":0,
            # "period":1,
            # "order":1000,
            # "side":1,
            # "x":88,
            # "y":48,
            # "score":"0 - 2",
            # "linked_action_id":null,
            # "print_time":"09:39",
            # "action_1_qualifier_code":"2",
            # "action_2_qualifier_code":"500",
            # "action_1_qualifier_description":"Tiro in corsa",
            # "action_2_qualifier_description":"contropiede",
            # "side_area_zone":"A",
            # "side_area_code":"6",
            # "player_name":"Giulio",
            # "player_surname":"Martini",
            # "player_number":null,
            # "team_name":"Happy Casa Brindisi",
            # "id":null}

            action = dict()

            action['game_id'] = self.current_game['game_id']

            action['data_set'] = self.current_game['data_set']
            action['date'] = self.current_game['date']

            # print(raw_action['home_players'])
            for i in range(len(raw_action['home_players'])):
                action[f"a{i + 1}"] = raw_action['home_players'][i]

            # print(raw_action['away_players'])
            for i in range(len(raw_action['away_players'])):
                action[f"h{i + 1}"] = raw_action['away_players'][i]

            period = raw_action['period']
            action['period'] = period

            # score is in the format 22 - 18 (home - away)
            if raw_action['score']:
                home_score = int(raw_action['score'].split('-')[0])
                away_score = int(raw_action['score'].split('-')[1])

            action['home_score'] = home_score
            action['away_score'] = away_score

            elapsed_time = timedelta(minutes=raw_action['minute'], seconds=raw_action['seconds'])

            period_minutes = 10 if raw_action['period'] <= 4 else 5
            time_duration = timedelta(minutes=period_minutes)

            action['remaining_time'] = time_duration - elapsed_time
            action['elapsed_time'] = elapsed_time

            if period == period_start:
                action['play_length'] = elapsed_time - action_start
                action_start = elapsed_time
            else:
                action_start = timedelta(minutes=0)
                action['play_length'] = elapsed_time - action_start

            action['play_id'] = raw_action['action_id']

            action['team'] = raw_action['team_name']

            event_type = self.map_event_type(raw_action['description'])

            if event_type is None and raw_action['description'].lower() == "assist":
                actions[-1]['assist'] = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title().strip()
                continue
            elif event_type is None and raw_action['description'].lower() == "stoppata":
                actions[-1]['block'] = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title().strip()
                continue
            elif event_type is None and raw_action['description'].lower() == "fallo subito":
                actions[-1]['opponent'] = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title().strip()
                continue
            elif event_type is None and raw_action['description'].lower() == "palla recuperata":
                actions[-1]['steal'] = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title().strip()
                continue
            elif event_type is None and raw_action['description'].lower() == "stoppata subita":
                continue

            action['event_type'] = event_type

            action['assist'] = ''
            if event_type == 'jump ball':
                if raw_action['home_club']:
                    action['away'] = ''
                    action['home'] = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title().strip()
                else:
                    action['away'] = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title().strip()
                    action['home'] = ''
            else:
                action['away'] = ''
                action['home'] = ''

            action['block'] = ''

            if event_type == 'sub':
                action['entered'] = raw_action['player_in']
                action['left'] = raw_action['player_out']
            else:
                action['entered'] = ''
                action['left'] = ''

            if 'num' in raw_action:
                action['num'] = raw_action['num']
            else:
                action['num'] = None

            action['opponent'] = ''

            if 'outof' in raw_action:
                action['outof'] = raw_action['outof']
            else:
                action['outof'] = None

            if raw_action['player_name'] and raw_action['player_surname']:
                action['player'] = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title().strip()
            else:
                action['player'] = ''

            points = self.map_points(raw_action['description'])
            action['points'] = points

            action['possession'] = ''

            action['reason'] = self.map_reason(
                [raw_action["action_1_qualifier_description"], raw_action["action_2_qualifier_description"]])

            if points is not None and points > 0:
                action['result'] = 'made'
            elif points is not None and points == 0:
                action['result'] = 'missed'
            else:
                action['result'] = ''

            action['steal'] = ''

            action['type'] = self.map_type(raw_action)

            # original coordinates place the origin in the bottom left corner. The coordinate span is (0, 100) foir both axis, so we shall divide by the number of feet of the size

            if raw_action['x'] and raw_action['y'] and event_type in {'miss', 'shot'}:
                original_x = raw_action['x']
                original_y = raw_action['y']

                converted_x = original_x * .9186
                converted_y = original_y * .4921

                # left side
                if raw_action['side'] == 0: # and raw_action['side_area_zone'] == 'A':
                    x_rim = 5.17
                else:
                    x_rim = 91.86 - 5.17

                y_rim = 49.21 / 2
                shot_distance = math.sqrt((converted_x - x_rim)**2 + (converted_y - y_rim)**2)
            else:
                original_x = None
                original_y = None
                converted_x = None
                converted_y = None
                shot_distance = None

            action['shot_distance'] = shot_distance
            action['original_x'] = original_x
            action['original_y'] = original_y
            action['converted_x'] = converted_x
            action['converted_y'] = converted_y

            action['description'] = raw_action['description']

            actions.append(action)

        return actions

    def insert_actions(self, actions):
        pass

    def get_boxes(self, game_id):
        pass

    def get_tadd(self):
        pass

    def map_event_type(self, description):
        mapping = {
            'substitution': 'sub',
            'falli di squadra': 'foul',
            'fallo commesso': 'foul',
            'palla contesa': 'jump ball',
            'palla persa': 'turnover',
            'palle perse di squadra': 'turnover',
            'rimbalzo difensivo': 'rebound',
            'rimbalzi difensivi di squadra': 'rebound',
            'rimbalzo offensivo': 'rebound',
            'rimbalzi offensivi di squadra': 'rebound',
            'tiro libero sbagliato': 'free throw',
            'tiro libero segnato': 'free throw',
            '2 punti sbagliato': 'miss',
            '2 punti segnato': 'shot',
            '3 punti sbagliato': 'miss',
            '3 punti segnato': 'shot',
            'inizio tempo': 'start of period',
            'fine tempo': 'end of period',
            'time out': 'timeout',
        }

        if description.lower() in mapping:
            return mapping[description.lower()]
        else:
            return None

    def map_points(self, description):
        mapping = {
            'tiro libero sbagliato': 0,
            'tiro libero segnato': 1,
            '2 punti sbagliato': 0,
            '2 punti segnato': 2,
            '3 punti sbagliato': 0,
            '3 punti segnato': 3,

        }

        if description.lower() in mapping:
            return mapping[description.lower()]
        else:
            return None

    def map_reason(self, descriptions):
        mapping = {
            '3 secondi': '3 second violation',
            '5 secondi': '5 second violation',
            '8 secondi': '8 second violation',
            'antisportivo': 'flagrant foul',
            'antisportivo su tiro': 'shooting flagrant foul',
            'doppio': 'double dribble turnover',
            'doppio palleggio': 'discontinue dribble turnover',
            'espulsione': 'ejection',
            'fuori dal campo': 'out of bounds lost ball turnover',
            'infrazione di campo': 'backcourt',
            'offensivo': 'offensive foul',
            'palleggio': 'lost ball',
            'passaggio sbagliato': 'bad pass',
            'passi': 'traveling',
            'personale': 'personal foul',
            'tecnico': 'techincal foul',
            'tecnico allenatore': 'coach technical foul',
            'tiro': 'shooting foul',
            'violazione 24sec': 'shot clock violation',
        }
        for description in descriptions:
            if description and description.lower() in mapping:
                return mapping[description.lower()]
        return ''

    def map_type(self, entry):

        mapping_description = {
            'palla contesa': 'jump ball',
            # 'palla persa': 'turnover',
            # 'palle perse di squadra': 'TOV',
            'rimbalzo difensivo': 'rebound defensive',
            'rimbalzi difensivi di squadra': 'team rebound',
            'rimbalzo offensivo': 'rebound offensive',
            'rimbalzi offensivi di squadra': 'team rebound',

            'inizio tempo': 'start of period',
            'fine tempo': 'end of period',
            'time out': 'timeout: regular',
        }

        mapping_flags = {
            '3 secondi': '3 second violation',
            '5 secondi': '5 second violation',
            '8 secondi': '8 second violation',
            'antisportivo': 'flagrant foul',
            'antisportivo su tiro': 'shooting flagrant foul',
            'doppio': 'double dribble turnover',
            'doppio palleggio': 'discontinue dribble turnover',
            'espulsione': 'ejection',
            'fuori dal campo': 'out of bounds lost ball turnover',
            'infrazione di campo': 'backcourt',
            'offensivo': 'offensive foul',
            'palleggio': 'lost ball',
            'passaggio sbagliato': 'bad pass',
            'passi': 'traveling',
            'personale': 'personal foul',
            'tecnico': 'techincal foul',
            'tecnico allenatore': 'coach technical foul',
            'tiro': 'shooting foul',
            'violazione 24sec': 'shot clock violation',
            # 'alley-oop': 'ALLEY-OOP',
            'altro': None,
            'appoggio a canestro': 'Layup',
            'arresto e tiro': 'Pullup',
            'da penetrazione': 'Driving',
            'gancio': 'Hook Shot',
            'giro e tiro': 'Turnaround shot',
            'schiacciata': 'Dunk',
            'stoppata': None,
            'tiro in corsa': 'Floating Jump Shot',
            'tiro in fadeaway': 'Fadeaway Jumper',
            'tiro in sospensione': 'Jump Shot',
            'tiro in step back': 'Step Back Jump Shot',
        }

        if entry['description'].lower() in mapping_description:
            return mapping_description[entry['description'].lower()]

        if entry['description'].lower() in ['tiro libero segnato', 'tiro libero sbagliato']:
            return f'Free Throw {entry["num"]} of {entry["outof"]}'

        for el in [entry['action_1_qualifier_description'], entry['action_2_qualifier_description']]:
            if el and el.lower() == 'alley-oop':
                if entry['dunk']:
                    return 'Alley Oop Dunk'
                else:
                    return 'Alley Oop Layup'

        for description in [entry['action_1_qualifier_description'], entry['action_2_qualifier_description']]:
            if description and description.lower() in mapping_flags:
                return mapping_flags[description.lower()]

        return ''