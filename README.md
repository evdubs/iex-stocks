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

Many of the above programs require a database password. The available parameters are:

```bash
$ racket chart-extract.rkt -h
racket chart-extract.rkt [ <option> ... ]
 where <option> is one of
  -d <date>, --date <date> : Exact date to query. Enabled only when querying for --history-range date
  -f <first>, --first-symbol <first> : First symbol to query. Defaults to nothing
  -l <last>, --last-symbol <last> : Last symbol to query. Defaults to nothing
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -t <token>, --api-token <token> : IEX Cloud API Token
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  -r <r>, --history-range <r> : Amount of history to request. Defaults to date, with date paired with a specified date using --date (-d)
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket chart-transform-load.rkt -h
racket chart-transform-load.rkt [ <option> ... ]
 where <option> is one of
  -b <folder>, --base-folder <folder> : IEX Stocks Chart base folder. Defaults to /var/tmp/iex/chart
  -d <date>, --folder-date <date> : IEX Stocks Chart folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket splits-extract.rkt -h
racket splits-extract.rkt [ <option> ... ]
 where <option> is one of
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -t <token>, --api-token <token> : IEX Cloud API Token
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  -r <r>, --history-range <r> : Amount of history to request. Defaults to 1m (one month)
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket splits-transform-load.rkt -h
racket splits-transform-load.rkt [ <option> ... ]
 where <option> is one of
  -b <folder>, --base-folder <folder> : IEX Stocks splits base folder. Defaults to /var/tmp/iex/splits
  -d <date>, --folder-date <date> : IEX Stocks splits folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket company-extract.rkt -h
racket company-extract.rkt [ <option> ... ]
 where <option> is one of
  -t <token>, --api-token <token> : IEX Cloud API Token
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket company-transform-load.rkt -h
racket company-transform-load.rkt [ <option> ... ]
 where <option> is one of
  -b <folder>, --base-folder <folder> : IEX Stocks company base folder. Defaults to /var/tmp/iex/company
  -d <date>, --folder-date <date> : IEX Stocks company folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'
```

The provided `schema.sql` file shows the expected schema within the target PostgreSQL instance. This process assumes that you can write to a `/var/tmp/iex` folder. This process also assumes that you have loaded your database with NASDAQ symbol file information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.

Unfortunately, around June 2019, the `ohlc` and `volume` endpoints have been made unavailable to users of the free tier and, for NASDAQ data, a special NASDAQ permission must be received after July 2019. Data similar to those endpoints is now accessed through `chart-extract` and `chart-transform-load`. Data is only available for the previous day and prior.

### Dependencies

It is recommended that you start with the standard Racket distribution. With that, you will need to install the following packages:

```bash
$ raco pkg install --skip-installed gregor http-easy tasks threading
```
