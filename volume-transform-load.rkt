#lang racket/base

(require db
         json
         racket/cmdline
         racket/port
         racket/sequence
         racket/string
         srfi/19 ; Time Data Types and Procedures
         threading)

(struct volume-by-venue (venue volume date))

(define base-folder (make-parameter "/var/tmp/iex/volume"))

(define folder-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket volume-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "IEX Stocks volume base folder. Defaults to /var/tmp/iex/volume"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "IEX Stocks volume folder date. Defaults to today"
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
                (hash-for-each _ (λ (symbol volume-by-venue-hash)
                                   (for-each (λ (vbv) (query-exec dbc "
insert into iex.volume (
  act_symbol,
  date,
  venue,
  volume
) values (
  $1,
  $2::text::date,
  $3::text::iex.venue,
  $4
) on conflict (act_symbol, date, venue) do nothing;
"
                                                                  (symbol->string symbol)
                                                                  (volume-by-venue-date vbv)
                                                                  (volume-by-venue-venue vbv)
                                                                  (volume-by-venue-volume vbv)))
                                             (filter (λ (vbv) (not (equal? 'null (volume-by-venue-date vbv))))
                                                     (map (λ (e) (apply volume-by-venue
                                                                        (list (hash-ref e 'venue)
                                                                              (hash-ref e 'volume)
                                                                              (hash-ref e 'date))))
                                                          (hash-ref volume-by-venue-hash 'volume-by-venue)))))))
            (commit-transaction dbc)))))))

(disconnect dbc)
