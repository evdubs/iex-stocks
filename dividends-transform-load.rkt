#lang racket/base

(require db
         json
         racket/cmdline
         racket/port
         racket/sequence
         racket/string
         srfi/19 ; Time Data Types and Procedures
         threading)

(struct dividend (ex-date declared-date record-date payment-date amount flag type qualified)
  #:transparent)

(define base-folder (make-parameter "/var/tmp/iex/dividends"))

(define folder-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dividends-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "IEX Stocks dividends base folder. Defaults to /var/tmp/iex/dividends"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "IEX Stocks dividends folder date. Defaults to today"
                         (folder-date (string->date date "~Y-~m-~d"))]
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

(parameterize ([current-directory (string-append (base-folder) "/" (date->string (folder-date) "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".json")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-range (string-replace (path->string p) ".json" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-range
                                                                      " for date "
                                                                      (date->string (folder-date) "~1")))
                                       (displayln ((error-value->string-handler) e 1000))
                                       (rollback-transaction dbc))])
            (start-transaction dbc)
            (~> (port->string in)
                (string->jsexpr _)
                (hash-for-each _ (λ (symbol dividend-hash)
                                   (for-each (λ (s) (query-exec dbc "
insert into iex.dividend (
  act_symbol,
  ex_date,
  payment_date,
  record_date,
  declared_date,
  amount,
  flag,
  type,
  qualified
) values (
  $1,
  $2::text::date,
  case $3
    when '' then NULL
    else $3::text::date
  end,
  case $4
    when '' then NULL
    else $4::text::date
  end,
  case $5
    when '' then NULL
    else $5::text::date
  end,
  case $6
    when '' then NULL
    else $6::text::numeric
  end,
  case $7
    when 'FI' then 'Final dividend'::iex.dividend_flag
    when 'LI' then 'Liquidation'::iex.dividend_flag
    when 'PR' then 'Proceeds of a sale of rights or shares'::iex.dividend_flag
    when 'RE' then 'Redemption of rights'::iex.dividend_flag
    when 'AC' then 'Accrued dividend'::iex.dividend_flag
    when 'AR' then 'Payment in arrears'::iex.dividend_flag
    when 'AD' then 'Additional payment'::iex.dividend_flag
    when 'EX' then 'Extra payment'::iex.dividend_flag
    when 'SP' then 'Special dividend'::iex.dividend_flag
    when 'YE' then 'Year end'::iex.dividend_flag
    when 'UR' then 'Unknown rate'::iex.dividend_flag
    when 'SU' then 'Regular dividend is suspended'::iex.dividend_flag
    else NULL
  end,
  case $8
    when 'Unspecified term captial gain' then 'Unspecified term capital gain'::iex.dividend_type
    else $8::text::iex.dividend_type
  end,
  case $9
    when 'P' then 'Partially qualified income'::iex.dividend_qualified
    when 'Q' then 'Qualified income'::iex.dividend_qualified
    when 'N' then 'Unqualified income'::iex.dividend_qualified
    else NULL
  end
) on conflict (act_symbol, ex_date) do nothing;
"
                                                                (symbol->string symbol)
                                                                (dividend-ex-date s)
                                                                (dividend-payment-date s)
                                                                (dividend-record-date s)
                                                                (dividend-declared-date s)
                                                                (if (not (equal? "" (dividend-amount s)))
                                                                    (real->decimal-string (dividend-amount s) 6)
                                                                    (dividend-amount s))
                                                                (dividend-flag s)
                                                                (dividend-type s)
                                                                (dividend-qualified s)))
                                             (map (λ (e) (apply dividend
                                                                (list (hash-ref e 'exDate)
                                                                      (hash-ref e 'declaredDate)
                                                                      (hash-ref e 'recordDate)
                                                                      (hash-ref e 'paymentDate)
                                                                      (hash-ref e 'amount)
                                                                      (hash-ref e 'flag)
                                                                      (hash-ref e 'type)
                                                                      (hash-ref e 'qualified))))
                                                  (hash-ref dividend-hash 'dividends))))))
            (commit-transaction dbc)))))))

(disconnect dbc)
