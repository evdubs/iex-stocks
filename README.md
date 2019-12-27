# iex-stocks
These Racket programs will download data from the [IEX Stocks API](https://iextrading.com/developer/docs/#stocks) and insert this data into a PostgreSQL database. The intended usage is :

```bash
$ racket chart-extract.rkt
$ racket chart-transform-load.rkt
```

```bash
$ racket splits-extract.rkt
$ racket splits-transform-load.rkt
```

```bash
$ racket dividends-extract.rkt
$ racket dividends-transform-load.rkt
```

```bash
$ racket company-extract.rkt
$ racket company-transform-load.rkt
```

The provided schema.sql file shows the expected schema within the target PostgreSQL instance. This process assumes that you can write to a /var/tmp/iex folder. This process also assumes that you have loaded your database with NASDAQ symbol file information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.

Unfortunately, around June 2019, the `ohlc` and `volume` endpoints have been made unavailable to users of the free tier and, for NASDAQ data, a special NASDAQ permission must be received after July 2019. Data similar to those endpoints is now accessed through `chart-extract` and `chart-transform-load`. Data is only available for the previous day and prior.

### Dependencies

It is recommended that you start with the standard Racket distribution. With that, you will need to install the following packages:

```bash
$ raco pkg install --skip-installed gregor tasks threading
```
