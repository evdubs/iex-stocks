#lang racket/base

(require db
         net/url
         racket/cmdline
         racket/file
         racket/list
         racket/port
         racket/string
         srfi/19 ; Time Data Types and Procedures
         tasks
         threading
         "list-partition.rkt")

(define (download-chart symbols)
  (make-directory* (string-append "/var/tmp/iex/chart/" (date->string (exact-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/iex/chart/" (date->string (exact-date) "~1") "/"
                                        (first symbols) "-" (last symbols) ".json")
    (λ (out)
      (~> (string-append "https://cloud.iexapis.com/stable/stock/market/batch?symbols=" (string-join symbols ",")
                         "&types=chart&range="
                         (cond [(equal? "date" (history-range))
                                (string-append (history-range) "&exactDate=" (date->string (exact-date) "~Y~m~d")
                                               "&chartByDay=true")]
                               [else (history-range)])
                         "&token=" (api-token))
          (string->url _)
          (get-pure-port _)
          (copy-port _ out)))
    #:exists 'replace))

(define api-token (make-parameter ""))

(define exact-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(define history-range (make-parameter "date"))

(command-line
 #:program "racket chart-extract.rkt"
 #:once-each
 [("-d" "--date") date
                  "Exact date to query. Enabled only when querying for --history-range date"
                  (exact-date (string->date date "~Y-~m-~d"))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-t" "--api-token") token
                     "IEX Cloud API Token"
                     (api-token token)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)]
 [("-r" "--history-range") r
                   "Amount of history to request. Defaults to date, with date paired with a specified date using --date (-d)"
                   (history-range r)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define symbols (query-list dbc "
select
  act_symbol
from
  nasdaq.symbol
where
  is_test_issue = false and
  is_next_shares = false and
  nasdaq_symbol !~ '[-\\$\\+\\*#!@%\\^=~]' and
  case when nasdaq_symbol ~ '[A-Z]{4}[L-Z]'
    then security_name !~ '(Note|Preferred|Right|Unit|Warrant)'
    else true
  end and
  last_seen = (select max(last_seen) from nasdaq.symbol)
order by
  act_symbol;
"))

(disconnect dbc)

(define grouped-symbols (list-partition symbols 100 100))

(define delay-interval 5)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length grouped-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-chart (first l)))
                                                          (second l)))
                            (map list grouped-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
