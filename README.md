# iex-stocks
These Racket programs will download data from the [IEX Stocks API](https://iextrading.com/developer/docs/#stocks) and insert this data into a PostgreSQL database. The intended usage is :

```bash
$ racket ohlc-extract.rkt
$ racket ohlc-transform-load.rkt
```

```bash
$ racket volume-extract.rkt
$ racket volume-transform-load.rkt
```

```bash
$ racket splits-extract.rkt
$ racket splits-transform-load.rkt
```

```bash
$ racket dividends-extract.rkt
$ racket dividends-transform-load.rkt
```


The provided schema.sql file shows the expected schema within the target PostgreSQL instance. This process assumes that you can write to a /var/tmp/iex folder. This process also assumes that you have loaded your database with NASDAQ symbol file information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.
