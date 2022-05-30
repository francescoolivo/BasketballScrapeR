import csv
from datetime import datetime

from LBA_scraper import LBAScraper

if __name__ == '__main__':

    scraper = LBAScraper(None)

    scraper.current_game = {
        'game_id': 23684,
        'data_set': '2021-2022 Playoffs',
        'date': datetime(2022, 5, 29)
    }

    raw_actions = scraper.get_actions()
    raw_actions = scraper.handle_substitutions(raw_actions)
    raw_actions = scraper.add_ft_count(raw_actions)

    actions = scraper.clean_actions(raw_actions)

    keys = actions[0].keys()

    file = open("output.csv", "w")
    dict_writer = csv.DictWriter(file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(actions)
    file.close()


