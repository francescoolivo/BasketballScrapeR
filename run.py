import argparse
import os
from datetime import datetime
from scrapers.LBA import LBAScraper


def get_scraper(name):
    name = name.upper()
    if name == "LBA":
        scraper = LBAScraper()
    else:
        scraper = None

    return scraper

parser = argparse.ArgumentParser()

league_help = 'the leagues to save; allowed value as of today are: LBA (default)'
parser.add_argument('-l', '--leagues', nargs='+', required=True, help=league_help, default=['LBA'])

seasons_help = 'the seasons to save in the format starting_year-ending_year (ex: 2020-2021 2021-2022)'
parser.add_argument('-s', '--seasons', nargs='+', help=seasons_help, default=[])

output_help = 'the output dir where to save csvs. Default is "csvs"'
parser.add_argument('-o', '--output', type=str, help=output_help, default='csvs')

args = parser.parse_args()


kwargs = {
    'seasons': args.seasons,
}

for league in args.leagues:
    scraper = get_scraper(name=league)

    if scraper is None:
        print(f'League {league} is not within allowed vales [LBA]. Ignoring')
        continue

    dfs = scraper.download_data(**kwargs)

    for year in dfs:
        year_code = f'{year%1000}{(year+1)%1000}'

        dir_path = os.path.join(args.output, league, year_code)

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        for entry in ['Pbox', 'Tbox', 'Obox', 'PBP', 'Tadd']:
            filename = os.path.join(dir_path, entry + '.csv')
            dfs[year][entry].to_csv(filename, float_format='%.5f', index=False)

exit(0)
