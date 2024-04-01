#lang racket/base

(require json
         gregor
         net/http-easy
         net/uri-codec
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
  (with-handlers ([exn:fail?
                   (λ (error)
                     (displayln (string-append "Encountered error while downloading symbols."))
                     (displayln error))])
    (~> (string-append "https://cloud.iexapis.com/stable/ref-data/symbols?token=" (api-token))
        (get _)
        (response-body _)
        (bytes->string/utf-8 _)
        (string->jsexpr _)
        (filter (λ (h) (member (hash-ref h 'type) issue-types)) _)
        (map (λ (h) (hash-ref h 'symbol)) _)
        (filter (λ (s) (if (equal? "" (first-symbol)) #t (string>=? s (first-symbol)))) _)
        (filter (λ (s) (if (equal? "" (last-symbol)) #t (string<=? s (last-symbol)))) _))))

(define (download-company symbols)
  (make-directory* (string-append "/var/tmp/iex/company/" (~t (today) "yyyy-MM-dd")))
  (call-with-output-file* (string-append "/var/tmp/iex/company/" (~t (today) "yyyy-MM-dd") "/"
                                         (first symbols) "-" (last symbols) ".json")
    (λ (out)
      (with-handlers ([exn:fail?
                       (λ (error)
                         (displayln (string-append "Encountered error for " (first symbols) "-" (last symbols)))
                         (displayln error))])
        (~> (string-append "https://cloud.iexapis.com/stable/stock/market/batch?symbols="
                           (uri-encode (string-join symbols ","))
                           "&types=company&token=" (api-token))
            (get _)
            (response-body _)
            (write-bytes _ out))))
    #:exists 'replace))

(define api-token (make-parameter ""))

(define first-symbol (make-parameter ""))

(define last-symbol (make-parameter ""))

(command-line
 #:program "racket company-extract.rkt"
 #:once-each
 [("-f" "--first-symbol") first
                          "First symbol to query. Defaults to nothing"
                          (first-symbol first)]
 [("-l" "--last-symbol") last
                         "Last symbol to query. Defaults to nothing"
                         (last-symbol last)]
 [("-t" "--api-token") token
                     "IEX Cloud API Token"
                     (api-token token)])

(define grouped-symbols (list-partition (download-symbols) 100 100))

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length grouped-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (thread (λ () (download-company (first l)))))
                                                          (second l)))
                            (map list grouped-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
