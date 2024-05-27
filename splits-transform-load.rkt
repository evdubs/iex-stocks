#lang racket/base

(require db
         gregor
         json
         racket/cmdline
         racket/port
         racket/sequence
         racket/string
         threading)

(struct split (ex-date declared-date to-factor from-factor)
  #:transparent)

(define base-folder (make-parameter "/var/tmp/iex/splits"))

(define folder-date (make-parameter (today)))

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

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".json")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [ticker-range (string-replace (string-replace file-name (path->string (current-directory)) "") ".json" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-range
                                                                      " for date "
                                                                      (~t (folder-date) "yyyy-MM-dd")))
                                       (displayln e)
                                       (rollback-transaction dbc))])
            (start-transaction dbc)
            (~> (port->string in)
                (string->jsexpr _)
                (hash-for-each _ (λ (symbol split-hash)
                                   (for-each (λ (s) (query-exec dbc "
insert into iex.split (
  act_symbol,
  ex_date,
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
  $4::text::numeric,
  $5::text::numeric
) on conflict (act_symbol, ex_date) do nothing;
"
                                                                (symbol->string symbol)
                                                                (split-ex-date s)
                                                                (if (equal? 'null (split-declared-date s)) "" (split-declared-date s))
                                                                (real->decimal-string (split-to-factor s) 6)
                                                                (real->decimal-string (split-from-factor s) 6)))
                                             (filter (λ (s) (and (not (= 0 (split-to-factor s)))
                                                                 (not (= 0 (split-from-factor s)))))
                                                     (map (λ (e) (apply split
                                                                        (list (hash-ref e 'exDate)
                                                                              (hash-ref e 'declaredDate)
                                                                              (hash-ref e 'toFactor)
                                                                              (hash-ref e 'fromFactor))))
                                                          (hash-ref split-hash 'splits)))))))
            (commit-transaction dbc)))))))

(disconnect dbc)
