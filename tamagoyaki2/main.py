import datetime
import os
from pathlib import Path

import numpy as np
import pandas as pd
import typer
from loguru import logger

# Working directory
WORKING_DIR = f"{Path.home()}/.tamagoyaki"

logger.remove()
logger.add(f"{WORKING_DIR}/log/app.log", level="INFO", format="{time} {level} {message}")
app = typer.Typer()


def make_1s_candle(df: pd.DataFrame):
    """
    convert trading data to ohlcv data.
    required columns of df: ['datetime', 'side', 'size', 'price']

    df:
    - datetime(pd.datetime64[ns]): timestamp of the trade
    - side(str): 'Buy' or 'Sell'
    - size(float): size of the trade
    - price(float): price of the trade
    """

    df = df[["datetime", "side", "size", "price"]]

    df.loc[:, ["buySize"]] = np.where(df["side"] == "Buy", df["size"], 0)
    df.loc[:, ["sellSize"]] = np.where(df["side"] == "Sell", df["size"], 0)
    df.loc[:, ["datetime"]] = df["datetime"].dt.floor("1s")

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

    return df


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

    # make the directory
    os.makedirs(f"{WORKING_DIR}/candles/{symbol}", exist_ok=True)
    
    # main process
    date_range = pd.date_range(bdt, edt, freq="D")

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
        logger.info(f"Downloading {filename}")
        try:
            df = pd.read_csv(url, compression="gzip")
        except Exception as e:
            logger.error(f"Failed to download {filename}")
            logger.error(e)
            continue
        
        # data processing
        logger.info(f"Processing {filename}")
        df.loc[:, ["datetime"]] = pd.to_datetime(df["timestamp"], unit="s")
        df = make_1s_candle(df)

        # save to data dir
        logger.info(f"Saving {filename}")
        df.to_csv(target, compression="gzip")


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

    # main process
    date_range = pd.date_range(bdt, edt, freq="D")
    dfs: list[pd.DataFrame] = [] # https://github.com/microsoft/pylance-release/issues/5630

    for date in date_range:
        
        # check if the data already exists
        target = f'{WORKING_DIR}/candles/{symbol}/{date.strftime("%Y-%m-%d")}.csv.gz'
        if not os.path.exists(target):
            logger.error(f"{target} does not exist.")
            continue
        
        # read the data and convert its type
        df = pd.read_csv(target)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)
        df["buyVolume"] = df["buyVolume"].astype(float)
        df["sellVolume"] = df["sellVolume"].astype(float)

        # re-structure the data
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
        dfs.append(df)

    # save
    if len(dfs) == 0:
        logger.error("No data found.")
        print(f"No data found.")
        return
    
    ans = pd.concat(dfs)
    file_name = f"{output_dir}/{symbol}_{begin}_{end}_{interval}.csv.gz"
    output_path = os.path.join(output_dir, file_name)
    ans.to_csv(output_path, compression="gzip")


@app.command()
def remove(
    symbol: str = typer.Argument(help="The symbol to remove"),
) -> None:
    """ remove

    Delete data stored by tamagoyaki

    """

    # confirm
    y_or_n = input(f"Are you sure you want to delete {symbol}? [y/n]: ")
    if y_or_n != "y":
        logger.info("Canceled.")
        return

    # main process
    target = f"{WORKING_DIR}/candles/{symbol}"
    if os.path.exists(target):
        os.system(f"rm -rf {target}")
        logger.info(f"{target} has been removed.")
    else:
        logger.error(f"{target} does not exist.")


@app.command()
def tidy(
    symbol: str = typer.Argument(help="The symbol to tidy"),
    begin: str = typer.Argument(help="The begin date (YYYYMMDD)"),
    end: str = typer.Argument(help="The end date (YYYYMMDD)"),
) -> None:
    """ tidy

    Remove the data outside the specified date range.

    """

    logger.info('called tidy')

    # validate the date format
    try:
        bdt = datetime.datetime.strptime(begin, "%Y%m%d")
        edt = datetime.datetime.strptime(end, "%Y%m%d")
    except ValueError:
        err = "Invalid date format. Please use YYYYMMDD."
        raise typer.BadParameter(err)

    # main process
    date_range = pd.date_range(bdt, edt, freq="D")

    for date in date_range:

        # check if the data already exists
        target = f"{WORKING_DIR}/candles/{symbol}/{date.strftime('%Y-%m-%d')}.csv.gz"
        if not os.path.exists(target):
            logger.error(f"{target} does not exist.")
            continue

        # remove the data
        os.remove(target)
        logger.info(f"{target} has been removed.")


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
        dates = [x.split(".")[0] for x in dates] # remove extension(.csv.gz)
        dates = [datetime.datetime.strptime(x, "%Y-%m-%d") for x in dates]

        mindt = min(dates)
        maxdt = max(dates)
        missings = [x for x in pd.date_range(mindt, maxdt, freq="D") if x not in dates]

        message = "{}: from {} to {}, {} missing dates".format(
            symbol, mindt.strftime("%Y-%m-%d"), maxdt.strftime("%Y-%m-%d"), len(missings)
        )
        print(message)


if __name__ == "__main__":
    app()