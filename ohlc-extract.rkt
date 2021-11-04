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

(define (download-ohlc symbols)
  (make-directory* (string-append "/var/tmp/iex/ohlc/" (date->iso8601 (today))))
  (call-with-output-file* (string-append "/var/tmp/iex/ohlc/" (date->iso8601 (today)) "/"
                                         (first symbols) "-" (last symbols) ".json")
    (λ (out)
      (with-handlers ([exn:fail?
                       (λ (error)
                         (displayln (string-append "Encountered error for " (first symbols) "-" (last symbols)))
                         (displayln ((error-value->string-handler) error 1000)))])
        (~> (string-append "https://cloud.iexapis.com/stable/stock/market/batch?symbols=" (string-join symbols ",")
                           "&types=ohlc&token=" (api-token))
            (get _)
            (response-body _)
            (write-bytes _ out))))
    #:exists 'replace))

(define api-token (make-parameter ""))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket ohlc-extract.rkt"
 #:once-each
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
                     (db-user user)])

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

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length grouped-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (thread (λ () (download-ohlc (first l)))))
                                                          (second l)))
                            (map list grouped-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
