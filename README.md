# Simple Example

To add BTCUSDT data from January 1 to 3, 2024 to the data store
```
$ python3 tamagoyaki/main.py update BTCUSDT 20240101 20240103
```

To save to csv file in the current directory
```
$ python3 tamagoyaki/main.py generate BTCUSDT 20240101 20240103 60
``` 

To delete symbol data that is no longer needed, specifying a date
```
$ python3 tamagoyaki/main.py tidy BTCUSDT 20240101 20240102
```

To delete symbol data that is no longer needed
```
$ python3 tamagoyaki/main.py remove BTCUSDT
```

To check the symbols already stored
```
$ python3 tamagoyaki/main.py inventory
```

To check the log, monitor `$HOME/.tamagoyaki/log/app.log`
```
$ tail -f ~/.tamagoyaki/log/app.log
```

# How to delete

If for some reason you want to delete all data related to tamagoyaki, delete the `$HOME/.tamagoyaki` directory completely. (`$HOME/.tamagoyaki` is the only working directory used by tamagoyaki.)

**Be careful not to accidentally delete another directory.**

