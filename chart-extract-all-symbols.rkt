#lang racket/base

(require db
         gregor
         net/http-easy
         racket/cmdline
         racket/file
         racket/list
         racket/port
         racket/string
         tasks
         threading
         "list-partition.rkt")

(define (download-chart symbols)
  (make-directory* (string-append "/var/tmp/iex/chart/" (~t (exact-date) "yyyy-MM-dd")))
  (call-with-output-file* (string-append "/var/tmp/iex/chart/" (~t (exact-date) "yyyy-MM-dd") "/"
                                         (first symbols) "-" (last symbols) ".json")
    (λ (out)
      (with-handlers ([exn:fail?
                       (λ (error)
                         (displayln (string-append "Encountered error for " (first symbols) "-" (last symbols) " for date " (date->iso8601 (exact-date))))
                         (displayln error))])
        (~> (string-append "https://cloud.iexapis.com/stable/stock/market/batch?symbols=" (string-join symbols ",")
                           "&types=chart&range="
                           (cond [(equal? "date" (history-range))
                                  (string-append (history-range) "&exactDate=" (~t (exact-date) "yyyyMMdd")
                                                 "&chartByDay=true")]
                                 [else (history-range)])
                           "&token=" (api-token))
            (get _)
            (response-body _)
            (write-bytes _ out))))
    #:exists 'replace))

(define api-token (make-parameter ""))

(define exact-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(define history-range (make-parameter "date"))

(define first-symbol (make-parameter ""))

(define last-symbol (make-parameter ""))

(command-line
 #:program "racket chart-extract-all-symbols.rkt"
 #:once-each
 [("-d" "--date") date
                  "Exact date to query. Enabled only when querying for --history-range date"
                  (exact-date (iso8601->date date))]
 [("-f" "--first-symbol") first
                          "First symbol to query. Defaults to nothing"
                          (first-symbol first)]
 [("-l" "--last-symbol") last
                         "Last symbol to query. Defaults to nothing"
                         (last-symbol last)]
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

(cond [(and (equal? "date" (history-range))
            (or (= 0 (->wday (exact-date)))
                (= 6 (->wday (exact-date)))))
       (displayln (string-append "Requested date " (date->iso8601 (exact-date)) " falls on a weekend. Terminating."))
       (exit)])

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
  case when $1 != ''
    then act_symbol >= $1
    else true
  end and
  case when $2 != ''
    then act_symbol <= $2
    else true
  end
order by
  act_symbol;
"
                            (first-symbol)
                            (last-symbol)))

(disconnect dbc)

(define grouped-symbols (list-partition symbols 100 100))

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length grouped-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (thread (λ () (download-chart (first l)))))
                                                          (second l)))
                            (map list grouped-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
