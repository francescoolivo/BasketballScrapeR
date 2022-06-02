# BasketballScrapeR

BasketballScrapeR is a python web scraper that downloads data from European basketball leagues in the BigDataBall format.

At the moment the only supported league is the Italian National League (LBA).

## Download and set up

You can download BasketballScrapeR from the command line:
```shell
git clone https://github.com/francescoolivo/BasketballScrapeR.git
cd BasketballScrapeR
```

From there, you can set up the Conda environment:
```shell
conda env create -f environment.yml
conda activate BasketballScrapeR
```

Then you are ready to go!

## Usage

You can download the data in csv format by typing:
```shell
python3 run.py -l LBA -s '2021-2022'
```
This downloads the last LBA season, and saves it by default in the ```csvs``` directory inside the repository.

In case you want to save the files in a different directory you can simply add the output parameter:
```shell
python3 run.py -l LBA -s '2021-2022' -o directory
```

In both the previous cases you only download data from the last season. If you don't pass the ```-s``` argument, by default the script will download all possible data in the history of the Italian League.
You are free to do it, but know in advance that the code has been tested only for the last two seasons, so you will likely find terminating errors due to play-by-play errors.

For this reason, in case you are interested only in the box-scores, you can run the script ignoring play-by-play:
```shell
python3 run.py -l LBA -s '2018-2019' --ignore_pbp
```

You should know that due to server-side errors play-by-play logs for the 2020-2021 and previous seasons are incomplete and sometimes faulted.

## Future developments
In the next months I will try to create a scraper also for Euroleague and for other leagues.

Also, I will translate the package in R.


