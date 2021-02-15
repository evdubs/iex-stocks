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

(define (download-splits symbols)
  (make-directory* (string-append "/var/tmp/iex/splits/" (~t (today) "yyyy-MM-dd")))
  (call-with-output-file (string-append "/var/tmp/iex/splits/" (~t (today) "yyyy-MM-dd") "/"
                                        (first symbols) "-" (last symbols) ".json")
    (λ (out)
      (~> (string-append "https://cloud.iexapis.com/stable/stock/market/batch?symbols=" (string-join symbols ",")
                         "&types=splits&range=" (history-range) "&token=" (api-token))
          (get _ #:stream? #t)
          (response-output _)
          (copy-port _ out)))
    #:exists 'replace))

(define api-token (make-parameter ""))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(define history-range (make-parameter "1m"))

(command-line
 #:program "racket splits-extract.rkt"
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
                     (db-user user)]
 [("-r" "--history-range") r
                   "Amount of history to request. Defaults to 1m (one month)"
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

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length grouped-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-splits (first l)))
                                                          (second l)))
                            (map list grouped-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
