import argparse
import os
from scrapers.LBA import LBAScraper


def get_scraper(name):
    name = name.upper()
    if name == "LBA":
        scraper = LBAScraper()
    else:
        scraper = None

    return scraper


parser = argparse.ArgumentParser()

league_help = 'The leagues to save; allowed value as of today are: LBA (default)'
parser.add_argument('-l', '--leagues', nargs='+', required=True, help=league_help, default=['LBA'])

seasons_help = 'The seasons to save in the format starting_year-ending_year (ex: 2020-2021 2021-2022)'
parser.add_argument('-s', '--seasons', nargs='+', help=seasons_help, default=[])

output_help = 'The output dir where to save csvs. Default is "csvs"'
parser.add_argument('-o', '--output', type=str, help=output_help, default='csvs')

ignore_help = 'Whether to ignore the play-by-play logs in order to only download the box-scores'
parser.add_argument('--ignore_pbp', action='store_true', default=False, help=ignore_help)

decimal_separator_help = 'The separator of decimal numbers, default is "."'
parser.add_argument('--decimal_separator', type=str, default='.', help=decimal_separator_help)

csv_separator_help = 'The separator of csv files, default is ","'
parser.add_argument('--csv_separator', type=str, default=',', help=csv_separator_help)

args = parser.parse_args()

if args.csv_separator == args.decimal_separator:
    print("Error: csv separator and decimal separator must be different!")
    exit(1)

kwargs = {
    'seasons': args.seasons,
    'ignore_pbp': args.ignore_pbp,
}

for league in args.leagues:
    scraper = get_scraper(name=league)

    if scraper is None:
        print(f'League {league} is not within allowed vales [LBA]. Ignoring')
        continue

    dfs = scraper.download_data(**kwargs)

    for year in dfs:
        year_code = f'{year % 1000}{(year + 1) % 1000}'

        dir_path = os.path.join(args.output, league, year_code)
        # print(dir_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        for entry in [('Pbox', True), ('Tbox', True), ('Obox', True), ('PBP', False), ('Tadd', False)]:
            filename = os.path.join(dir_path, entry[0] + '.csv')
            df = dfs[year][entry[0]].convert_dtypes()

            s = df.select_dtypes(include='Float64').columns
            df[s] = df[s].astype("float")

            df.to_csv(filename, float_format='%.3f', index=entry[1], sep=args.csv_separator, decimal=args.decimal_separator)

exit(0)
