import datetime
import gzip
import os
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import typer
from loguru import logger

# const
WORKING_DIR = f"{Path.home()}/.tamagoyaki"

logger.remove()
logger.add(f"{WORKING_DIR}/log/app.log", level="INFO", format="{time} {level} {message}")
app = typer.Typer()


@app.callback(help="🍳 A CLI tool for managing the crypto candlestick data.")
def callback() -> None:
    """ callback
    initialize the working directory.
    """

    os.makedirs(WORKING_DIR, exist_ok=True)


@app.command(help="update the database.")
def update(
    symbol: str = typer.Argument(help="The symbol to download"),
    begin: str = typer.Argument(help="The begin date (YYYYMMDD)"),
    end: str = typer.Argument(help="The end date (YYYYMMDD)"),
) -> None:
    """ update

    update data directory of 1-second candlestick data.

    """
    
    # validate the date format
    try:
        bdt = datetime.datetime.strptime(begin, "%Y%m%d")
        edt = datetime.datetime.strptime(end, "%Y%m%d")
    except ValueError:
        err = "Invalid date format. Please use YYYYMMDD."
        raise typer.BadParameter(err)
    
    # main process
    date_range = [bdt + datetime.timedelta(days=i) for i in range((edt - bdt).days + 1)]

    for date in date_range:

        # check if the data already exists
        target = f"{WORKING_DIR}/candles/{symbol}/{date.strftime('%Y-%m-%d')}.csv.gz"
        if os.path.exists(target):
            logger.info(f"{target} already exists.")
            continue

        # make url
        base_url = "https://public.bybit.com/trading/"
        filename = f"{symbol}{date.strftime('%Y-%m-%d')}.csv.gz"
        url = os.path.join(base_url, symbol, filename)

        # download
        resp = requests.get(url)
        if resp.status_code != 200:
            logger.error(f"Failed to download {filename}")
            continue
        
        # data processing
        with gzip.open(BytesIO(resp.content), "rt") as f:

            df = pd.read_csv(f)

            # setting
            df = df[["timestamp", "side", "size", "price"]]
            df.loc[:, ["datetime"]] = pd.to_datetime(df["timestamp"], unit="s")
            df.loc[:, ["buySize"]] = np.where(df["side"] == "Buy", df["size"], 0)
            df.loc[:, ["sellSize"]] = np.where(df["side"] == "Sell", df["size"], 0)
            df.loc[:, ["datetime"]] = df["datetime"].dt.floor("1s")

            # groupby 
            df = df.groupby("datetime").agg(
                {
                    "price": ["first", "max", "min", "last"],
                    "size": "sum",
                    "buySize": "sum",
                    "sellSize": "sum",
                }
            )

            # multiindex to single index
            df.columns = ["_".join(col) for col in df.columns]
            df = df.rename(
                columns={
                    "price_first": "open",
                    "price_max": "high",
                    "price_min": "low",
                    "price_last": "close",
                    "size_sum": "volume",
                    "buySize_sum": "buyVolume",
                    "sellSize_sum": "sellVolume",
                }
            )

            # save to data dir
            os.makedirs(f"{WORKING_DIR}/candles/{symbol}", exist_ok=True)
            df.to_csv(
                f"{WORKING_DIR}/candles/{symbol}/{date.strftime('%Y-%m-%d')}.csv.gz",
                compression="gzip",
            )


@app.command()
def generate(
    symbol: str = typer.Argument(help="The symbol to download"),
    begin: str = typer.Argument(help="The begin date (YYYYMMDD)"),
    end: str = typer.Argument(help="The end date (YYYYMMDD)"),
    interval: int = typer.Argument(help="The interval in seconds"),
    output_dir: str = typer.Option("./", help="The output directory"),
) -> None:
    """ generate

    generate the database of 1-second candlestick data.

    """

    # validate the date format
    try:
        bdt = datetime.datetime.strptime(begin, "%Y%m%d")
        edt = datetime.datetime.strptime(end, "%Y%m%d")
    except ValueError:
        err = "Invalid date format. Please use YYYYMMDD."
        raise typer.BadParameter(err)
    

    date_range = [bdt + datetime.timedelta(days=i) for i in range((edt - bdt).days + 1)]

    # main process
    dfs = []
    for date in date_range:
        
        # check if the data already exists
        target = f'{WORKING_DIR}/candles/{symbol}/{date.strftime("%Y-%m-%d")}.csv.gz'
        if not os.path.exists(target):
            logger.error(f"{target} does not exist.")
            continue

        df = pd.read_csv(target)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)
        df["buyVolume"] = df["buyVolume"].astype(float)
        df["sellVolume"] = df["sellVolume"].astype(float)

        df = df.set_index("datetime")
        df = df.resample(f"{interval}s").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "buyVolume": "sum",
                "sellVolume": "sum",
            }
        )

        df = df.dropna()

        # append
        dfs.append(df)

    if len(dfs) == 0:
        logger.error("No data found.")
        return
    ans = pd.concat(dfs)
    ans.to_csv(f"{output_dir}/{symbol}.csv.gz", compression="gzip")


@app.command()
def inventory() -> None:
    """ inventory

    show the available symbols and dates.

    """

    # main process
    symbols = os.listdir(f"{WORKING_DIR}/candles")
    symbols = [x for x in symbols if not x.startswith(".")] # remove hidden files
    for symbol in symbols:
        dates = os.listdir(f"{WORKING_DIR}/candles/{symbol}")
        dates = os.listdir(f"{WORKING_DIR}/candles/{symbol}")

        dates = [x.split(".")[0] for x in dates] # remove extension(.csv.gz)
        print(f'{symbol}: from {min(dates)} to {max(dates)}')


if __name__ == "__main__":
    app()