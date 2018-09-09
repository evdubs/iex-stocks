#lang racket

(require db)
(require json)
(require racket/cmdline)
(require srfi/19) ; Time Data Types and Procedures
(require threading)

(struct split (ex-date declared-date record-date payment-date to-factor for-factor))

(define base-folder (make-parameter "/var/tmp/iex/splits"))

(define folder-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket splits-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "IEX Stocks splits base folder. Defaults to /var/tmp/iex/splits"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "IEX Stocks splits folder date. Defaults to today"
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
                (hash-for-each _ (λ (symbol split-hash)
                                   (for-each (λ (s) (query-exec dbc "
insert into iex.split (
  act_symbol,
  ex_date,
  payment_date,
  record_date,
  declared_date,
  to_factor,
  for_factor
) values (
  $1,
  $2::text::date,
  case $3
    when '' then NULL
    else $3::text::date
  end,
  case $4
    when '' then NULL
    else $4::text::date
  end,
  case $5
    when '' then NULL
    else $5::text::date
  end,
  $6::text::numeric,
  $7::text::numeric
) on conflict (act_symbol, ex_date) do nothing;
"
                                                                (symbol->string symbol)
                                                                (split-ex-date s)
                                                                (split-payment-date s)
                                                                (split-record-date s)
                                                                (split-declared-date s)
                                                                (real->decimal-string (split-to-factor s) 6)
                                                                (real->decimal-string (split-for-factor s) 6)))
                                             (map (λ (e) (apply split
                                                                (list (hash-ref e 'exDate)
                                                                      (hash-ref e 'declaredDate)
                                                                      (hash-ref e 'recordDate)
                                                                      (hash-ref e 'paymentDate)
                                                                      (hash-ref e 'toFactor)
                                                                      (hash-ref e 'forFactor))))
                                                  (hash-ref split-hash 'splits))))))
            (commit-transaction dbc)))))))

(disconnect dbc)
