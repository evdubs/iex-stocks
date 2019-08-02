#lang racket/base

(require db
         json
         racket/cmdline
         racket/list
         racket/port
         racket/sequence
         racket/string
         srfi/19 ; Time Data Types and Procedures
         threading)

(define base-folder (make-parameter "/var/tmp/iex/chart"))

(define folder-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket chart-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "IEX Stocks Chart base folder. Defaults to /var/tmp/iex/chart"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "IEX Stocks Chart folder date. Defaults to today"
                         (folder-date (string->date date "~Y-~m-~d"))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(parameterize ([current-directory (string-append (base-folder) "/" (date->string (folder-date) "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".json")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-range (string-replace (path->string p) ".json" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-range
                                                                      " for date "
                                                                      (date->string (folder-date) "~1")))
                                       (displayln ((error-value->string-handler) e 1000))
                                       (rollback-transaction dbc))])
            (start-transaction dbc)
            (~> (port->string in)
                (string->jsexpr _)
                (hash-for-each _ (λ (symbol chart-hash)
                                   (cond [(and (not (equal? 'null (hash-ref chart-hash 'chart)))
                                               (not (empty? (hash-ref chart-hash 'chart))))
                                          (let ([date (hash-ref (first (hash-ref chart-hash 'chart)) 'date)]
                                                [open (hash-ref (first (hash-ref chart-hash 'chart)) 'uOpen)]
                                                [high (hash-ref (first (hash-ref chart-hash 'chart)) 'uHigh)]
                                                [low (hash-ref (first (hash-ref chart-hash 'chart)) 'uLow)]
                                                [close (hash-ref (first (hash-ref chart-hash 'chart)) 'uClose)]
                                                [volume (hash-ref (first (hash-ref chart-hash 'chart)) 'uVolume)])
                                            (query-exec dbc "
insert into iex.chart (
  act_symbol,
  date,
  open,
  high,
  low,
  close,
  volume
) values (
  $1,
  $2::text::date,
  $3::text::numeric,
  $4::text::numeric,
  $5::text::numeric,
  $6::text::numeric,
  $7::text::numeric
) on conflict (act_symbol, date) do update set
  open = $3::text::numeric,
  high = $4::text::numeric,
  low = $5::text::numeric,
  close = $6::text::numeric,
  volume = $7::text::numeric;
"
                                                        (symbol->string symbol)
                                                        date
                                                        (real->decimal-string open 4)
                                                        (if (equal? high 0) (real->decimal-string (max open close) 4)
                                                            (real->decimal-string high 4))
                                                        (if (equal? low 0) (real->decimal-string (min open close) 4)
                                                            (real->decimal-string low 4))
                                                        (real->decimal-string close 4)
                                                        (number->string volume)))]))))
            (commit-transaction dbc)))))))

(disconnect dbc)
