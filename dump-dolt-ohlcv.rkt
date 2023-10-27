#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/tmp/dolt/stocks"))

(define start-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define end-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dump-dolt-ohlcv.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Base dolt folder. Defaults to /var/tmp/dolt/stocks"
                         (base-folder folder)]
 [("-e" "--end-date") end
                      "Final date for history retrieval. Defaults to today"
                      (end-date end)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-s" "--start-date") start
                        "Earliest date for history retrieval. Defaults to today"
                        (start-date start)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(for-each (λ (date)
            (define ohlcv-file (string-append (base-folder) "/ohlcv-" date ".csv"))
            (call-with-output-file* ohlcv-file
              (λ (out)
                (displayln "date,act_symbol,open,high,low,close,volume" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  date::text,
  act_symbol::text,
  coalesce(open::text, ''),
  coalesce(high::text, ''),
  coalesce(low::text, ''),
  coalesce(close::text, ''),
  coalesce(volume::text, '')
from
  iex.chart
where
  date = $1::text::date
order by
  act_symbol;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u --continue ohlcv ohlcv-" date ".csv")))
          (query-list dbc "
select distinct
  date::text
from
  iex.chart
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))
(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add ohlcv; "
                       "/usr/local/bin/dolt commit -m 'ohlcv " (end-date) " update'; /usr/local/bin/dolt push --silent"))
