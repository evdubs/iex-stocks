#lang racket/base

(require db
         gregor
         gregor/period
         json
         racket/cmdline
         racket/port
         racket/sequence
         racket/string
         threading)

(define base-folder (make-parameter "/var/tmp/iex/ohlc"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket ohlc-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "IEX Stocks OHLC base folder. Defaults to /var/tmp/iex/ohlc"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "IEX Stocks OHLC folder date. Defaults to today"
                         (folder-date (iso8601->date date))]
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

(parameterize ([current-directory (string-append (base-folder) "/" (date->iso8601 (folder-date)) "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".json")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->iso8601 (folder-date)) "/" (path->string p))]
          [ticker-range (string-replace (path->string p) ".json" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-range
                                                                      " for date "
                                                                      (date->iso8601 (folder-date))))
                                       (displayln ((error-value->string-handler) e 1000))
                                       (rollback-transaction dbc))])
            (start-transaction dbc)
            (~> (port->string in)
                (string->jsexpr _)
                (hash-for-each _ (λ (symbol ohlc-hash)
                                   (let* ([open (hash-ref (hash-ref ohlc-hash 'ohlc) 'open)]
                                          [high (hash-ref (hash-ref ohlc-hash 'ohlc) 'high)]
                                          [low (hash-ref (hash-ref ohlc-hash 'ohlc) 'low)]
                                          [close (hash-ref (hash-ref ohlc-hash 'ohlc) 'close)])
                                     (cond [(and (not (hash-empty? close))
                                                 (date=? (folder-date) (->date (+period (datetime 1970) (period [milliseconds (hash-ref close 'time)])))))
                                            (query-exec dbc "
insert into iex.chart (
  act_symbol,
  date,
  open,
  high,
  low,
  close
) values (
  $1,
  $2::text::date,
  case $3
    when '' then NULL
    else $3::text::numeric
  end,
  case $4
    when '' then NULL
    else $4::text::numeric
  end,
  case $5
    when '' then NULL
    else $5::text::numeric
  end,
  case $6
    when '' then NULL
    else $6::text::numeric
  end
) on conflict (act_symbol, date) do nothing;
"
                                                        (symbol->string symbol)
                                                        (date->iso8601 (->date (+period (datetime 1970) (period [milliseconds (hash-ref close 'time)]))))
                                                        (if (hash-empty? open) "" (real->decimal-string (hash-ref open 'price) 4))
                                                        (if (equal? high 'null) "" (real->decimal-string high 4))
                                                        (if (equal? low 'null) "" (real->decimal-string low 4))
                                                        (if (hash-empty? close) "" (real->decimal-string (hash-ref close 'price) 4)))])))))
            (commit-transaction dbc)))))))

(disconnect dbc)
