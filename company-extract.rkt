#lang racket/base

(require json
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

(define issue-types '("ad" "re" "ce" "si" "lp" "cs" "et"))

(define (download-symbols)
  (~> (string-append "https://cloud.iexapis.com/stable/ref-data/symbols?token=" (api-token))
      (get _)
      (response-body _)
      (bytes->string/utf-8 _)
      (string->jsexpr _)
      (filter (λ (h) (member (hash-ref h 'type) issue-types)) _)
      (map (λ (h) (hash-ref h 'symbol)) _)))

(define (download-company symbols)
  (make-directory* (string-append "/var/tmp/iex/company/" (~t (today) "yyyy-MM-dd")))
  (call-with-output-file (string-append "/var/tmp/iex/company/" (~t (today) "yyyy-MM-dd") "/"
                                        (first symbols) "-" (last symbols) ".json")
    (λ (out)
      (with-handlers ([exn:fail?
                       (λ (error)
                         (displayln (string-append "Encountered error for " (first symbols) "-" (last symbols)))
                         (displayln ((error-value->string-handler) error 1000)))])
        (~> (string-append "https://cloud.iexapis.com/stable/stock/market/batch?symbols=" (string-join symbols ",")
                           "&types=company&token=" (api-token))
            (get _)
            (response-body _)
            (write-bytes _ out))))
    #:exists 'replace))

(define api-token (make-parameter ""))

(command-line
 #:program "racket company-extract.rkt"
 #:once-each
 [("-t" "--api-token") token
                     "IEX Cloud API Token"
                     (api-token token)])

(define grouped-symbols (list-partition (download-symbols) 100 100))

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length grouped-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-company (first l)))
                                                          (second l)))
                            (map list grouped-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
