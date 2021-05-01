#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/tmp/dolt/stocks"))

(define start-date (make-parameter (~t (-months (today) 1) "yyyy-MM-dd")))

(define end-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dump-dolt-splits.rkt"
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

(define split-file (string-append (base-folder) "/split-" (end-date) ".csv"))

(call-with-output-file split-file
  (λ (out)
    (displayln "act_symbol,ex_date,to_factor,for_factor" out)
    (for-each (λ (row)
                (displayln (string-join (vector->list row) ",") out))
              (query-rows dbc "
select
  act_symbol::text,
  ex_date::text,
  trunc(to_factor, 5)::text,
  trunc(for_factor, 5)::text
from
  iex.split
where
  ex_date >= $1::text::date and
  ex_date <= $2::text::date
"
                          (start-date)
                          (end-date))))
  #:exists 'replace)

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u --continue split split-" (end-date) ".csv"))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add split; "
                       "/usr/local/bin/dolt commit -m 'split " (end-date) " update'; /usr/local/bin/dolt push"))
