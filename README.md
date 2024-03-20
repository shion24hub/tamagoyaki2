# Simple Example

To add 60-second candlestick data to the BTCUSDT database for January 1-3, 2024
```
$ python3 tamagoyaki/main.py update BTCUSDT 20240101 20240103
```

Then save to csv file in the current directory
```
$ python3 tamagoyaki/main.py generate BTCUSDT 20240101 20240103 60
``` 

To check the symbols already stored
```
$ python3 tamagoyaki/main.py inventory
```

To check the log, monitor `$HOME/.tamagoyaki/log/app.log`
```
$ tail -f ~/.tamagoyaki/log/app.log
```