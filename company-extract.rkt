#lang racket/base

(require json
         net/url
         racket/file
         racket/list
         racket/port
         racket/string
         srfi/19 ; Time Data Types and Procedures
         tasks
         threading
         "list-partition.rkt")

(define issue-types '("ad" "re" "ce" "si" "lp" "cs" "et"))

(define (download-symbols)
  (~> (string->url "https://api.iextrading.com/1.0/ref-data/symbols")
      (get-pure-port _)
      (port->string _)
      (string->jsexpr _)
      (filter (λ (h) (member (hash-ref h 'type) issue-types)) _)
      (map (λ (h) (hash-ref h 'symbol)) _)))

(define (download-company symbols)
  (make-directory* (string-append "/var/tmp/iex/company/" (date->string (current-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/iex/company/" (date->string (current-date) "~1") "/"
                                        (first symbols) "-" (last symbols) ".json")
    (λ (out)
      (~> (string-append "https://api.iextrading.com/1.0/stock/market/batch?symbols=" (string-join symbols ",")
                         "&types=company")
          (string->url _)
          (get-pure-port _)
          (copy-port _ out)))))

(define grouped-symbols (list-partition (download-symbols) 100 100))

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length grouped-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-company (first l)))
                                                          (second l)))
                            (map list grouped-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
