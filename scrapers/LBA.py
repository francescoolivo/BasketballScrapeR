from datetime import datetime
import json
import math
import re
from datetime import timedelta
import pandas as pd
from bs4 import BeautifulSoup
import requests as requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3 import Retry
import utils
from scraper import Scraper


class SubstitutionError(Exception):
    pass

IN_SUB_STRING = "Ingresso"
OUT_SUB_STRING = "Uscita"

class LBAScraper(Scraper):

    def __init__(self):
        super().__init__()

    def get_seasons(self, **kwargs):
        def find_code(year, seasons):
            for season in seasons:
                if year == season['year']:
                    return season['id']

        seasons = dict()

        base_url = 'https://www.legabasket.it/championship/'
        rs_url = f'{base_url}/429'
        po_url = f'{base_url}/222'

        analyzed_years = set()
        rs_seasons = json.loads(requests.get(rs_url).content)
        rs_seasons = sorted(rs_seasons['data']['years'], key=lambda d: (d['year'], d['id']))

        for season in rs_seasons:
            if season['year'] in analyzed_years:
                continue
            analyzed_years.add(season['year'])

            season_code = f'{season["year"]}-{season["year"] + 1}'

            if 'seasons' in kwargs and kwargs['seasons'] and season_code not in kwargs['seasons']:
                continue

            seasons[season['year']] = dict()

            seasons[season['year']]['year'] = season['year']
            seasons[season['year']]['code'] = season_code

            seasons[season['year']]['RS'] = find_code(season['year'], rs_seasons)

            po_seasons = json.loads(requests.get(po_url).content)['data']['years']
            seasons[season['year']]['PO'] = find_code(season['year'], po_seasons)

        return seasons

    def get_games(self, season, **kwargs):

        games = []

        base_url = 'https://www.legabasket.it/championship/'
        league_url = 'https://www.legabasket.it/phase/'

        params = dict()

        params['s'] = season['year']
        params['c'] = season['RS']

        url = f'{base_url}{season["RS"]}'
        season_full = json.loads(requests.get(url).content)['data']
        rs_phases = sorted(season_full['phases'], key=lambda d: d['id'])
        for phase in rs_phases:
            phase_name = self.map_phase(phase["code"])
            params['p'] = phase['id']
            rounds = json.loads(
                requests.get(f'{league_url}{phase["id"]}/{season["RS"]}').content.decode(
                    'utf8'))['data']['days']

            for r in rounds:
                params['d'] = r['code']
                game_url = 'https://www.legabasket.it/lba/6/calendario/calendar?'
                soup = utils.get_soup(game_url, params=params)

                while soup is None:
                    soup = utils.get_soup(url)

                if soup.find('tbody') is None:
                    continue

                for tr in soup.find('tbody').find_all('tr'):
                    url_id = tr.find(class_='result').find('a').attrs['href']
                    game_id = re.findall(r'/game/([0-9]*)/*', url_id)[0]

                    game_result = tr.find(class_='result').text.strip()
                    status = 'played' if game_result != '0 - 0' else 'scheduled'

                    if status != 'played':
                        continue

                    try:
                        date = datetime.strptime(':'.join(tr.find_all('td')[5].text.strip().split()),
                                                 '%d/%m/%Y:%H:%M')
                    except ValueError:
                        print("Error fetching date")
                        continue

                    dataset = f'{season["code"]} {phase_name}'

                    game = {
                        'game_id': game_id,
                        'data_set': dataset,
                        'date': date
                    }

                    games.append(game)

        if season['PO'] is None:
            return games
        params['c'] = season['PO']

        url = f'{base_url}{season["PO"]}'
        data = json.loads(requests.get(url).content)['data']

        po_phases = sorted(data['phases'], key=lambda d: d['id'])
        for phase in po_phases:
            phase_name = self.map_phase(phase["code"])
            params['p'] = phase['id']
            rounds = json.loads(
                requests.get(f'{league_url}{phase["id"]}/{season["PO"]}').content.decode(
                    'utf8'))['data']['days']

            for r in rounds:
                params['d'] = r['code']
                game_url = 'https://www.legabasket.it/lba/6/calendario/calendar?'
                soup = utils.get_soup(game_url, params=params)

                while soup is None:
                    soup = utils.get_soup(url)

                if soup.find('tbody') is None:
                    continue

                for tr in soup.find('tbody').find_all('tr'):
                    url_id = tr.find(class_='result').find('a').attrs['href']
                    game_id = re.findall(r'/game/([0-9]*)/*', url_id)[0]

                    game_result = tr.find(class_='result').text.strip()
                    status = 'played' if game_result != '0 - 0' else 'scheduled'

                    if status != 'played':
                        continue

                    try:
                        date = datetime.strptime(':'.join(tr.find_all('td')[5].text.strip().split()),
                                                 '%d/%m/%Y:%H:%M')
                    except ValueError:
                        continue

                    dataset = f'{season["code"]} {phase_name}'

                    game = {
                        'game_id': game_id,
                        'data_set': dataset,
                        'date': date
                    }

                    games.append(game)

        return games

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
        period_url = f'{url}/{period}'
        try:
            response = session.get(period_url).json()
        except json.decoder.JSONDecodeError:
            print(f"Actions not found for game {game} @ {period_url}")
            return actions
        while response['data']['pbp'] is not None and response['data']['pbp']:
            actions += response['data']['pbp']
            period += 1
            period_url = f'{url}/{period}'
            try:
                response = session.get(period_url).json()
            except json.decoder.JSONDecodeError:
                continue

        return actions

    def get_boxes(self, soup: BeautifulSoup):

        boxes = dict()

        scores = soup.find('div', id='scores')

        if scores is None:
            return dict

        for h5 in scores.find_all('h5'):
            team = h5.text

            table = h5.find_next('tbody')

            boxes[team] = dict()
            boxes[team]['players'] = []
            boxes[team]['team'] = []

            player_re = re.compile(r'^tr_player_(\d+)$', re.IGNORECASE)
            total_re = re.compile(r'^tr_totals_(\d+)$', re.IGNORECASE)
            for row in table.find_all('tr'):
                if player_re.match(row.attrs['id']):

                    mapping = self.get_stats_mapping()
                    tds = row.find_all('td')
                    stats = dict()
                    stats['Team'] = team

                    name = tds[0].find('span', {'class': 'scores_player_name'}).text
                    surname = tds[0].find('span', {'class': 'scores_player_surname'}).text

                    stats['Player'] = ' '.join([name, surname]).title().strip()
                    for key in mapping:
                        stats[key] = int(tds[mapping[key]].text)

                    boxes[team]['players'].append(stats.copy())

                elif total_re.match(row.attrs['id']):
                    mapping = self.get_stats_mapping(team=True)
                    tds = row.find_all('td')
                    stats = dict()
                    stats['Team'] = team

                    for key in mapping:
                        stats[key] = int(tds[mapping[key]].text)

                    boxes[team]['team'].append(stats.copy())

        for t1 in boxes:
            for t2 in boxes:
                if t1 != t2:
                    boxes[t1]['opponent'] = [boxes[t2]['team'][0].copy()]
                    boxes[t1]['opponent'][0]['Team'] = t1

        for t in boxes:
            boxes[t]['team'][0]['PM'] = boxes[t]['team'][0]['PTS'] - boxes[t]['opponent'][0]['PTS']
            boxes[t]['opponent'][0]['PM'] = boxes[t]['opponent'][0]['PTS'] - boxes[t]['team'][0]['PTS']

        return boxes

    def get_stats_mapping(self, team=False):
        mapping = {
            'MIN': 2,
            'PTS': 1,
            'P2M': 6,
            'P2A': 7,
            'P3M': 10,
            'P3A': 11,
            'FTM': 13,
            'FTA': 14,
            'OREB': 16,
            'DREB': 17,
            'AST': 23,
            'TOV': 21,
            'STL': 22,
            'BLK': 19,
            'PF': 4,
            'PM': 26,
        }

        if team:
            mapping.pop('PM')

        return mapping

    def get_starters(self, soup):
        # url = f'https://www.legabasket.it/game/{self.current_game["game_id"]}/scores'
        # soup = utils.get_soup(url)

        starters = dict()
        starters['home'] = []
        starters['away'] = []

        scores_div = soup.find('div', id='scores')

        if scores_div is None:
            return starters

        table = scores_div.find('table', id='ht_match_scores').find_next('tbody')
        for tr in table.find_all('tr'):
            if tr.find_all_next('td')[3].find('i'):
                name = tr.find_all_next('td')[0].find('span', {'class': 'scores_player_name'}).text.title()
                surname = tr.find_all_next('td')[0].find('span', {'class': 'scores_player_surname'}).text.title()

                starters['home'].append(' '.join([name, surname]))

        table = scores_div.find('table', id='vt_match_scores').find_next('tbody')
        for tr in table.find_all('tr'):
            if tr.find_all_next('td')[3].find('i'):
                name = tr.find_all_next('td')[0].find('span', {'class': 'scores_player_name'}).text.title()
                surname = tr.find_all_next('td')[0].find('span', {'class': 'scores_player_surname'}).text.title()

                starters['away'].append(' '.join([name, surname]))

        if not starters:
            print(f"Could not find starter in game {self.current_game['game_id']}")
            print(table)
            exit(1)
        return starters

    def handle_substitutions(self, raw_actions):
        actions = []

        starters = self.starters
        home_team_players = set(starters['home'])
        away_team_players = set(starters['away'])

        players = {
            1: home_team_players,
            0: away_team_players
        }

        team_descriptions = {
            1: 'home',
            0: 'away'
        }

        # To handle cases where the same substitution is repeated
        substitutions = set()

        faulted_games = {'23482', '23029', '23098', '23118', '23156', '23210'}

        # unfortunately, some substitutions must be fixed manually
        faulted_actions = get_faulted_actions()
        action_to_edit = get_actions_to_edit()
        actions_to_add = get_actions_to_add()

        game_website_id = self.current_game['game_id']

        actions_to_reorder = get_actions_to_sort()
        if game_website_id in actions_to_reorder:
            for couple in actions_to_reorder[game_website_id]:
                to_remove = raw_actions[couple[0]]
                raw_actions.remove(to_remove)
                raw_actions.insert(couple[1], to_remove)

        if game_website_id in actions_to_add:
            for couple in actions_to_add[game_website_id]:
                raw_actions.insert(couple[0], couple[1])

        # add a flag to an action so that we can ignore it while iterating
        subs = set()
        for raw_action in raw_actions:
            raw_action['checked'] = False
            raw_action['to_ignore'] = False

        for raw_action in raw_actions:
            raw_action['checked'] = False
            raw_action['to_ignore'] = False
            edited = False

            if game_website_id in action_to_edit and raw_action['action_id'] in action_to_edit[game_website_id]:
                for key in action_to_edit[game_website_id][raw_action['action_id']]:
                    raw_action[key] = action_to_edit[game_website_id][raw_action['action_id']][key]

                edited = True

            if game_website_id in faulted_actions and raw_action['action_id'] in faulted_actions[game_website_id]:
                raw_action['to_ignore'] = True
                edited = True

            if raw_action['description'] in [IN_SUB_STRING, OUT_SUB_STRING] and not edited:
                sub_str = f'{raw_action["description"]}|{raw_action["period"]}|{raw_action["print_time"]}|{raw_action["player_name"].title()} {raw_action["player_surname"].title()}'
                if raw_action['print_time'] == '00:00':

                    index = raw_actions.index(raw_action)
                    time_error = False

                    for next_action in raw_actions[index:]:
                        if raw_action['period'] != next_action['period']:
                            break
                        elif next_action['print_time'] == '00:00' and next_action['description'] not in ['Ingresso',
                                                                                                         'Uscita',
                                                                                                         'Timeout',
                                                                                                         'Fine Tempo'] and not \
                        next_action['to_ignore']:
                            time_error = True
                            break
                    if time_error:
                        # print(f'Game {game_website_id} has some subs with missing timestamp in period {raw_action["period"]}. Ignoring repeated substitutions')
                        continue
                if sub_str in subs:
                    raw_action['to_ignore'] = True
                    # print(f'Sub {sub_str} in game {game_website_id} is replicated, thus ignored')
                subs.add(sub_str)

        # count the number of in and out substitutions for each team, to check that they are the same
        sub_count = dict()
        for i in [0, 1]:
            sub_count[i] = dict()
            sub_count[i][IN_SUB_STRING] = 0
            sub_count[i][OUT_SUB_STRING] = 0

        for raw_action in raw_actions:
            if raw_action['description'] in [IN_SUB_STRING, OUT_SUB_STRING] and not raw_action['to_ignore']:
                sub_count[raw_action['home_club']][raw_action['description']] += 1

        # check that the number of in and out subs is the same
        for team in [1, 0]:
            if sub_count[team][IN_SUB_STRING] != sub_count[team][OUT_SUB_STRING]:
                error = f'{game_website_id}, {team}, IN: {sub_count[team][IN_SUB_STRING]}, OUT: {sub_count[team][OUT_SUB_STRING]}'
                raise Exception(error)

        pending_subs = {
            0: {
                IN_SUB_STRING: set(),
                OUT_SUB_STRING: set()
            },
            1: {
                IN_SUB_STRING: set(),
                OUT_SUB_STRING: set()
            }
        }

        for raw_action in raw_actions:

            if raw_action['description'] not in [IN_SUB_STRING, OUT_SUB_STRING]:
                raw_action['home_players'] = home_team_players.copy()
                raw_action['away_players'] = away_team_players.copy()
                actions.append(raw_action)

            elif raw_action['description'] in [IN_SUB_STRING, OUT_SUB_STRING] and not raw_action['to_ignore'] and not raw_action['checked'] and \
                    raw_action['player_name'] and raw_action['player_surname']:

                player = f'{raw_action["player_name"]} {raw_action["player_surname"]}'.title()
                # 1. se il tipo dell'azione è uscita controllare che non ci siano ingressi in attesa. Se ci sono sostituire, altrimenti inserire in lista
                # 2. se il tipo dell'azione è ingresso controllare che non ci siano uscite in attesa. Se ci sono sostituire, altrimenti inserire in lista

                type_to_look_for = OUT_SUB_STRING if raw_action['description'] == IN_SUB_STRING else IN_SUB_STRING

                # if there is a player waiting to conclude the substitution, we shall replace him
                # if the pending player is an out sub, we shall remove him from the list and add the entering player
                # if the pending player is an in sub, we shall remove the current player from the list and add the entering player
                if pending_subs[raw_action['home_club']][type_to_look_for]:

                    if type_to_look_for == OUT_SUB_STRING:  # is not empty
                        player_to_be_removed = pending_subs[raw_action["home_club"]][type_to_look_for].pop()
                        player_to_be_inserted = player
                        # player_to_be_removed = self.players_cache[player_to_be_removed_str]

                    else:
                        player_to_be_removed = player
                        player_to_be_inserted = pending_subs[raw_action["home_club"]][type_to_look_for].pop()
                        # player_to_be_inserted = self.players_cache[player_to_be_inserted_str]

                    try:
                        players[raw_action['home_club']].remove(player_to_be_removed)
                        players[raw_action['home_club']].add(player_to_be_inserted)


                    except KeyError:
                        error = f'Key Error: could not make substitution {player_to_be_inserted} for {player_to_be_removed}. Game is {self.current_game["game_id"]}, action is {raw_action["action_id"]} and type is {raw_action["description"]}\nPlayers on court are {players[raw_action["home_club"]]}'
                        raise Exception(error)

                    if len(players[raw_action['home_club']]) != 5:
                        error = f'Error with team {raw_action["team"]} in action {raw_action["action_id"]} during game {game_website_id}:\nthere are not 5 players on court!\nSub: {player_to_be_inserted} for {player_to_be_removed}\nOn court: {players[raw_action["home_club"]]}\nPending: {pending_subs[raw_action["home_club"]]}'
                        raise Exception(error)

                    raw_action['player_in'] = player_to_be_inserted
                    raw_action['player_out'] = player_to_be_removed
                    raw_action['home_players'] = home_team_players.copy()
                    raw_action['away_players'] = away_team_players.copy()
                    raw_action['description'] = 'Substitution'

                    actions.append(raw_action)

                else:
                    pending_subs[raw_action['home_club']][raw_action['description']].add(player)

                # raw_action['home_players'] = home_team_players.copy()
                # raw_action['away_players'] = away_team_players.copy()
                #
                # actions.append(raw_action)

            else:
                continue

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

        raw_actions = self.handle_substitutions(raw_actions)
        raw_actions = self.add_ft_count(raw_actions)

        actions = []

        home_score = 0
        away_score = 0

        action_start = timedelta(minutes=0)
        period_start = 1

        for raw_action in raw_actions:

            action = dict()

            action['game_id'] = self.current_game['game_id']

            action['data_set'] = self.current_game['data_set']
            action['date'] = self.current_game['date']

            # print(raw_action['home_players'])
            ap_l = list(raw_action['away_players'])
            for i in range(len(raw_action['away_players'])):
                action[f"a{i + 1}"] = ap_l[i]

            # print(raw_action['away_players'])
            hp_l = list(raw_action['home_players'])
            for i in range(len(raw_action['home_players'])):
                action[f"h{i + 1}"] = hp_l[i]

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

            # if raw_action['player_name'] and raw_action['player_name'] and event_type != "sub":
            #     player = ' '.join(
            #         [raw_action['player_name'], raw_action['player_surname']]).title().strip()
            #
            #     on_court = player and (player in raw_action['home_players'] or player in raw_action['away_players'])
            #
            #     if player and not on_court and event_type:
            #         players = raw_action['home_players'] if raw_action['home_club'] else raw_action['away_players']
            #         print(f"ERROR: {player} is not among players on court {players}, event type is {event_type} [{self.current_game}]")

            if event_type is None and raw_action['description'].lower() == "assist":
                actions[-1]['assist'] = ' '.join(
                    [raw_action['player_name'], raw_action['player_surname']]).title().strip()
                continue
            elif event_type is None and raw_action['description'].lower() == "stoppata":
                actions[-1]['block'] = ' '.join(
                    [raw_action['player_name'], raw_action['player_surname']]).title().strip()
                continue
            elif event_type is None and raw_action['description'].lower() == "fallo subito":
                actions[-1]['opponent'] = ' '.join(
                    [raw_action['player_name'], raw_action['player_surname']]).title().strip()
                continue
            elif event_type is None and raw_action['description'].lower() == "palla recuperata":
                actions[-1]['steal'] = ' '.join(
                    [raw_action['player_name'], raw_action['player_surname']]).title().strip()
                continue
            elif event_type is None and raw_action['description'].lower() == "stoppata subita":
                continue

            action['event_type'] = event_type

            action['assist'] = ''

            if event_type == 'jump ball' and raw_action['home_club'] and raw_action['player_name'] and raw_action[
                'player_surname']:
                action['away'] = ''
                action['home'] = ' '.join([raw_action['player_name'], raw_action['player_surname']]).title().strip()
            elif event_type == 'jump ball' and not raw_action['home_club'] and raw_action['player_name'] and raw_action[
                'player_surname']:
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

            # original coordinates place the origin in the bottom left corner. The coordinate span is (0, 100) for both axis, so we shall divide by the number of feet of the size
            if raw_action['x'] and raw_action['y']:
                original_x = raw_action['x']
                original_y = raw_action['y']

                converted_y = original_x * .9186
                converted_x = original_y * .4921

                # left side
                if raw_action['side'] == 0:  # and raw_action['side_area_zone'] == 'A':
                    x_rim = 5.17
                else:
                    x_rim = 91.86 - 5.17

                y_rim = 49.21 / 2
                shot_distance = math.sqrt((converted_x - x_rim) ** 2 + (converted_y - y_rim) ** 2)
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

    def get_tadd(self, season_id):
        url = 'https://www.legabasket.it/lba/6/calendario/standings'
        params = {'s': season_id}

        soup = utils.get_soup(url, params=params)
        table = soup.find('table', class_='full-standings')
        tbody = table.find('tbody')

        df = pd.DataFrame(columns=['Team', 'team', 'Conference', 'Division', 'Rank', 'Playoff'])

        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')

            rank = int(tds[0].text.strip())

            df = pd.concat([df, pd.DataFrame([{
                'Team': tds[1].text.strip(),
                'team': '',
                'Conference': '',
                'Division': '',
                'Rank': rank,
                'Playoff': 'Y' if rank <= 8 else 'N',
            }])], ignore_index=True)

        return df.sort_values(by=['Team'])

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

            tadd_df = self.get_tadd(season_id=season)
            # tadd_df = pd.DataFrame(tadd, columns=['Team', 'team', 'Conference', 'Division', 'Rank', 'Playoff'])

            games = self.get_games(seasons[season])

            for game in tqdm(games):

                self.current_game = game
                url = f'https://www.legabasket.it/game/{game["game_id"]}'

                soup = utils.get_soup(url)

                self.starters = self.get_starters(soup)

                boxes = self.get_boxes(soup)

                if not boxes:
                    continue

                if type(boxes) == type:
                    print(self.current_game['game_id'])

                for team in boxes:
                    players_df = pd.concat([players_df, pd.DataFrame(boxes[team]['players'])], ignore_index=True)
                    team_df = pd.concat([team_df, pd.DataFrame(boxes[team]['team'])], ignore_index=True)
                    opponent_df = pd.concat([opponent_df, pd.DataFrame(boxes[team]['opponent'])], ignore_index=True)

                if kwargs['ignore_pbp']:
                    continue

                raw_actions = self.get_actions()
                if not raw_actions:
                    print(f"Missing play-by-play logs for game {game}")
                    continue
                elif game['game_id'] in {'23096'}:
                    print(f"Game play-by-play is faulted, ignoring. {game}")
                    continue
                actions = self.clean_actions(raw_actions)

                pbp_df = pd.concat([pbp_df, pd.DataFrame(actions)], ignore_index=True)

            dataframes[season]['Pbox'] = self.summarize_players_df(players_df)
            dataframes[season]['Tbox'] = self.summarize_teams_df(team_df)
            dataframes[season]['Obox'] = self.summarize_teams_df(opponent_df, opponent=True)
            dataframes[season]['PBP'] = pbp_df
            dataframes[season]['Tadd'] = tadd_df

        return dataframes

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

    def map_phase(self, code):
        mapping = {
            'andata': 'Regular Season',
            'ritorno': 'Regular Season',
            'seconda fase': 'Clock Round',
            'ottavi': 'Playoffs',
            'quarti': 'Playoffs',
            'quarti di finale': 'Playoffs',
            'semifinali': 'Playoffs',
            'finale': 'Playoffs',
            'finali': 'Playoffs',
            'girone a': 'Regular Season',
            'girone b': 'Regular Season',
            'girone c': 'Regular Season',
            'girone d': 'Regular Season',
            'Finale 3°/4° Posto': 'Playoffs',
        }

        if code.lower() in mapping:
            return mapping[code.lower()]
        else:
            print(f"{code} not recognized in allowed values")
            return None

def get_faulted_actions():
    return {
        '23482': [652, 653, 654, 655, 656, 659, 660, 661, 647, 648],
        '23371': [503, 505],
        '23379': [466, 467, 468, 469],
        '23406': [225],
        '23569': [580, 581],
        '23579': [370]
    }


def get_actions_to_edit():
    return {
        '23371': {
            535: {
                'seconds': 13
            },
        },
        '23406': {
            193: {
                'player_name': 'Giovanni',
                'player_surname': 'De Nicolao'
            }
        }
    }


def get_actions_to_sort():
    return {
        '23371': [(418, 402), ]
    }


def get_actions_to_add():
    return {
        '23393': [(266, {'action_id': None, 'description': 'Ingresso', 'player_id': 2697, 'team_id': 1462, 'home_club': 1, 'in_area': False, 'dunk': 0, 'seconds': 0, 'minute': 0, 'period': 3, 'order': 42200, 'side': None, 'x': None, 'y': None, 'score': None, 'linked_action_id': 382, 'print_time': '10:00', 'action_1_qualifier_code': None, 'action_2_qualifier_code': None, 'action_1_qualifier_description': None, 'action_2_qualifier_description': None, 'side_area_zone': None, 'side_area_code': None, 'player_name': 'Kyle', 'player_surname': 'Hines', 'player_number': '42', 'team_name': 'A|X Armani Exchange Milano', 'id': None})],
        '23480': [(351, {'action_id': None, 'description': 'Uscita', 'player_id': 3951, 'team_id': 1477, 'home_club': 0, 'in_area': False, 'dunk': 0, 'seconds': 27, 'minute': 6, 'period': 3, 'order': 62800, 'side': None, 'x': None, 'y': None, 'score': None, 'linked_action_id': 479, 'print_time': '03:33', 'action_1_qualifier_code': None, 'action_2_qualifier_code': None, 'action_1_qualifier_description': None, 'action_2_qualifier_description': None, 'side_area_zone': None, 'side_area_code': None, 'player_name': 'Valerio', 'player_surname': 'Mazzola', 'player_number': '22', 'team_name': 'Umana Reyer Venezia', 'id': None}),
                  (352, {'action_id': 483, 'description': 'Ingresso', 'player_id': 5834, 'team_id': 1477, 'home_club': 0, 'in_area': False, 'dunk': 0, 'seconds': 27, 'minute': 6, 'period': 3, 'order': 62900, 'side': None, 'x': None, 'y': None, 'score': None, 'linked_action_id': 479, 'print_time': '03:33', 'action_1_qualifier_code': None, 'action_2_qualifier_code': None, 'action_1_qualifier_description': None, 'action_2_qualifier_description': None, 'side_area_zone': None, 'side_area_code': None, 'player_name': 'Michele', 'player_surname': 'Vitali', 'player_number': '31', 'team_name': 'Umana Reyer Venezia', 'id': None})]
    }