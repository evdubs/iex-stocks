#lang racket/base

(require db
         gregor
         json
         racket/cmdline
         racket/list
         racket/port
         racket/sequence
         racket/string
         threading)

(define base-folder (make-parameter "/var/tmp/iex/company"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket company-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "IEX Stocks company base folder. Defaults to /var/tmp/iex/company"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "IEX Stocks company folder date. Defaults to today"
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
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".json")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/" (path->string p))]
          [ticker-range (string-replace (path->string p) ".json" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-range
                                                                      " for date "
                                                                      (~t (folder-date) "yyyy-MM-dd")))
                                       (displayln ((error-value->string-handler) e 1000))
                                       (rollback-transaction dbc))])
            (start-transaction dbc)
            (~> (port->string in)
                (string->jsexpr _)
                (hash-for-each _ (λ (symbol company-hash)
                                   (cond [(not (equal? 'null (hash-ref (hash-ref company-hash 'company) 'companyName)))
                                          (query-exec dbc "
with it as (
  select case $9
    when 'ad' then 'American depositary receipt'::iex.issue_type
    when 're' then 'Real estate investment trust'::iex.issue_type
    when 'ce' then 'Closed end fund'::iex.issue_type
    when 'si' then 'Secondary issue'::iex.issue_type
    when 'lp' then 'Limited partnership'::iex.issue_type
    when 'cs' then 'Common stock'::iex.issue_type
    when 'et' then 'Exchange traded fund'::iex.issue_type
    else NULL
  end as issue_type
)
insert into iex.company (
  act_symbol,
  company_name,
  exchange,
  industry,
  sub_industry,
  website,
  description,
  ceo,
  issue_type,
  sector,
  last_seen
) values (
  $1,
  $2,
  $3,
  case $4
    when '' then NULL
    else $4
  end,
  case $5
    when '' then NULL
    else $5
  end,
  case $6
    when '' then NULL
    else $6
  end,
  $7,
  case $8
    when '' then NULL
    else $8
  end,
  (select issue_type from it),
  case $10
    when '' then NULL
    else $10
  end,
  $11::text::date
) on conflict (act_symbol) do update set
  last_seen = $11::text::date;
"
                                                      (hash-ref (hash-ref company-hash 'company) 'symbol)
                                                      (hash-ref (hash-ref company-hash 'company) 'companyName)
                                                      (hash-ref (hash-ref company-hash 'company) 'exchange)
                                                      (hash-ref (hash-ref company-hash 'company) 'industry)
                                                      (~> (hash-ref (hash-ref company-hash 'company) 'tags)
                                                          (remove (hash-ref (hash-ref company-hash 'company) 'sector) _)
                                                          (remove (hash-ref (hash-ref company-hash 'company) 'industry) _)
                                                          (append _ (list ""))
                                                          (first _))
                                                      (hash-ref (hash-ref company-hash 'company) 'website)
                                                      (hash-ref (hash-ref company-hash 'company) 'description)
                                                      (hash-ref (hash-ref company-hash 'company) 'CEO)
                                                      (hash-ref (hash-ref company-hash 'company) 'issueType)
                                                      (hash-ref (hash-ref company-hash 'company) 'sector)
                                                      (~t (folder-date) "yyyy-MM-dd"))]))))
            (commit-transaction dbc)))))))

(disconnect dbc)
