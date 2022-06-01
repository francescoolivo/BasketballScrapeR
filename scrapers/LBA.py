from datetime import datetime
import json
import math
import os
import re
from datetime import timedelta
from bs4 import BeautifulSoup
import requests as requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import utils
from scraper import Scraper


class SubstitutionError(Exception):
    pass


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
        period_url = os.path.join(url, str(period))
        try:
            response = session.get(period_url).json()
        except json.decoder.JSONDecodeError:
            print(f"Actions not found for game {game} @ {period_url}")
            return actions
        while response['data']['pbp'] is not None and response['data']['pbp']:
            actions += response['data']['pbp']
            period += 1
            period_url = os.path.join(url, str(period))
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
                    boxes[t1]['opponent'] = boxes[t2]['team']

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

    def get_starters(self):
        url = f'https://www.legabasket.it/game/{self.current_game["game_id"]}/scores'
        soup = utils.get_soup(url)

        starters = dict()
        starters['home'] = []
        starters['away'] = []

        while soup is None:
            soup = utils.get_soup(url)

        scores_div = soup.find('div', id='scores')

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

        starters = self.get_starters()
        home_team_players = starters['home']
        away_team_players = starters['away']

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

        faulted_games = {'23482', '23029'}

        # add a flag to an action so that we can ignore it while iterating
        for raw_action in raw_actions:
            raw_action['checked'] = False
            raw_action['to_ignore'] = False

        sub_count = dict()


        for i in [0, 1]:
            sub_count[i] = dict()
            sub_count[i]['Ingresso'] = 0
            sub_count[i]['Uscita'] = 0

        for raw_action in raw_actions:
            if raw_action['description'] in ['Ingresso', 'Uscita']:
                sub_count[raw_action['home_club']][raw_action['description']] += 1

        for team in [1, 0]:
            if sub_count[team]['Ingresso'] == 0 and  sub_count[team]['Uscita'] == 0:
                print(f"ERROR: 0 subs in game {self.current_game}")
                exit(1)
            if sub_count[team]['Ingresso'] != sub_count[team]['Uscita']:
                # print(f"IN: {sub_count[team]['Ingresso']}, OUT: {sub_count[team]['Uscita']}")
                # case 1: one IN more than OUT, likely it's repeated
                if sub_count[team]['Ingresso'] - sub_count[team]['Uscita'] == 1:
                    # print(f"Sub error type 1, team {team}")

                    substitution_check = set()
                    for raw_action in raw_actions:
                        if raw_action['description'] in ['Ingresso', 'Uscita'] and raw_action['home_club'] == team:

                            player = ' '.join([raw_action['player_name'].title(), raw_action['player_surname'].title()])

                            sub = f"{raw_action['description']}: {player} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"
                            if sub in substitution_check:
                                raw_action['checked'] = True
                            else:
                                substitution_check.add(sub)

                # case 2: one OUT more than IN, we shall check when a player which is on court is removed
                elif sub_count[team]['Uscita'] - sub_count[team]['Ingresso'] == 1:
                    # print(f"Sub error type 2, team {team}")

                    players_temp = set(players[team].copy())

                    for raw_action in raw_actions:
                        if raw_action['description'] in ['Ingresso', 'Uscita'] and raw_action['home_club'] == team:
                            player = ' '.join([raw_action['player_name'].title(), raw_action['player_surname'].title()])

                            if raw_action['description'] == 'Ingresso':
                                players_temp.add(player)
                            elif raw_action['description'] == 'Uscita':
                                if player in players_temp:
                                    players_temp.remove(player)
                                else:
                                    # found
                                    raw_action['checked'] = True
                                    break
                # case 3: more errors, likely the same error has been repeated more than once
                elif abs(sub_count[team]['Uscita'] - sub_count[team]['Ingresso']) > 1:
                    # print(f"Sub error type 3, team {team}")

                    for raw_action in raw_actions:
                        if raw_action['description'] in ['Ingresso', 'Uscita'] and raw_action['home_club'] == team and not raw_action['to_ignore']:
                            found = False
                            type_to_look_for = 'Uscita' if raw_action['description'] == 'Ingresso' else 'Ingresso'

                            raw_action_index = raw_actions.index(raw_action)

                            # looking for the next out substitution for the team
                            for next_raw_action in raw_actions[raw_action_index:]:
                                if next_raw_action['description'] == type_to_look_for and raw_action['team_name'] == \
                                        next_raw_action['team_name'] and next_raw_action['home_club'] == team and (not next_raw_action['checked']) and (not next_raw_action['to_ignore']) and raw_action['minute'] == next_raw_action['minute'] and raw_action['seconds'] == next_raw_action['seconds']:

                                    found = True
                                    raw_action['to_ignore'] = True
                                    next_raw_action['to_ignore'] = True
                                    break
                            if not found:
                                raw_action['checked'] = True

        for raw_action in raw_actions:

            if raw_action['description'] not in ['Ingresso', 'Uscita']:
                raw_action['home_players'] = home_team_players.copy()
                raw_action['away_players'] = away_team_players.copy()
                actions.append(raw_action)

            elif raw_action['description'] in ['Ingresso', 'Uscita'] and not raw_action['checked'] and raw_action['player_name'] and raw_action['player_surname']:

                type_to_look_for = 'Uscita' if raw_action['description'] == 'Ingresso' else 'Ingresso'

                raw_action_index = raw_actions.index(raw_action)

                # looking for the next out substitution for the team
                for next_raw_action in raw_actions[raw_action_index+1:]:
                    if next_raw_action['description'] == type_to_look_for and raw_action['team_name'] == \
                            next_raw_action['team_name'] and not next_raw_action['checked'] and next_raw_action and next_raw_action['player_name'] and next_raw_action['player_surname']:  # and raw_action['minute'] == next_raw_action['minute'] and raw_action['seconds'] == next_raw_action['seconds']:
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

                        if player_in == player_out:
                            continue

                        sub = f"SUB: {player_in} for {player_out} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"
                        sub_in = f"IN: {player_in} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"
                        sub_out = f"OUT: {player_out} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"

                        if player_out in players[raw_action['home_club']] and player_in in players[raw_action['home_club']]:
                            raw_action_index = raw_actions.index(raw_action)
                            found = False
                            for next_raw_action_2 in raw_actions[raw_action_index + 1:]:
                                if next_raw_action_2['home_club'] == raw_action['home_club'] and next_raw_action_2['player_surname'] and next_raw_action_2['player_name']:
                                    player = ' '.join([next_raw_action_2['player_name'].title(),
                                                       next_raw_action_2['player_surname'].title()])
                                    if next_raw_action_2['description'] != 'Ingresso' and player not in players[raw_action['home_club']]:
                                        found = True

                                        player_in = player
                                        sub = f"SUB: {player_in} for {player_out} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"
                                        sub_in = f"IN: {player_in} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"

                                        break

                                    elif next_raw_action['description'] == 'Uscita' and player == player_in:
                                        break

                            if not found:
                                next_raw_action['checked'] = True

                                break
                                raise SubstitutionError(f"Could not find a player to replace {player_out}, {sub}")

                        # switch players
                        elif player_out not in players[raw_action['home_club']] and player_in in players[raw_action['home_club']]:
                            #
                            # player_in, player_out = player_out, player_in
                            # sub = f"SUB: {player_in} for {player_out} [{raw_action['period']} - {raw_action['minute']:02d}:{raw_action['seconds']:02d}]"
                            #
                            # print(f'switched players | {sub}')
                            next_raw_action['checked'] = True

                            # print(f"Ignored {sub}")
                            break

                        elif player_out not in players[raw_action['home_club']] and self.current_game['game_id'] in faulted_games:
                            next_raw_action['checked'] = True

                            # print(f"Faulted game, ignored {sub}")
                            break




                        if sub_in in substitutions or sub_out in substitutions:
                            next_raw_action['checked'] = True
                            break

                        elif player_out not in players[raw_action['home_club']]:
                            #return actions
                            raise SubstitutionError(
                                f"{player_out} should be on court for {team_descriptions[raw_action['home_club']]} team but he is not. On court players are {players[raw_action['home_club']]}\nSub is {sub}\n{self.current_game}")

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
            for i in range(len(raw_action['away_players'])):
                action[f"a{i + 1}"] = raw_action['away_players'][i]

            # print(raw_action['away_players'])
            for i in range(len(raw_action['home_players'])):
                action[f"h{i + 1}"] = raw_action['home_players'][i]

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

            if event_type == 'jump ball' and raw_action['home_club'] and raw_action['player_name'] and raw_action['player_surname']:
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

            # original coordinates place the origin in the bottom left corner. The coordinate span is (0, 100) foir both axis, so we shall divide by the number of feet of the size

            if raw_action['x'] and raw_action['y'] and event_type in {'miss', 'shot'}:
                original_x = raw_action['x']
                original_y = raw_action['y']

                converted_x = original_x * .9186
                converted_y = original_y * .4921

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
