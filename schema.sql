CREATE SCHEMA iex;

CREATE TYPE iex.dividend_flag AS ENUM
   ('Final dividend',
    'Liquidation',
    'Proceeds of a sale of rights or shares',
    'Redemption of rights',
    'Accrued dividend',
    'Payment in arrears',
    'Additional payment',
    'Extra payment',
    'Special dividend',
    'Year end',
    'Unknown rate',
    'Regular dividend is suspended');

CREATE TYPE iex.dividend_qualified AS ENUM
   ('Partially qualified income',
    'Qualified income',
    'Unqualified income');

CREATE TYPE iex.dividend_type AS ENUM
   ('Dividend income',
    'Interest income',
    'Stock dividend',
    'Short term capital gain',
    'Medium term capital gain',
    'Long term capital gain',
    'Unspecified term capital gain');

CREATE TYPE iex.issue_type AS ENUM
   ('American depositary receipt',
    'Real estate investment trust',
    'Closed end fund',
    'Secondary issue',
    'Limited partnership',
    'Common stock',
    'Exchange traded fund');

CREATE TYPE iex.venue AS ENUM
   ('ARCX',
    'BATS',
    'BATY',
    'EDGA',
    'EDGX',
    'IEXG',
    'TRF',
    'XASE',
    'XBOS',
    'XCHI',
    'XCIS',
    'XNGS',
    'XNYS',
    'XPHL');

CREATE TABLE iex.chart
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint,
    CONSTRAINT chart_pkey PRIMARY KEY (date, act_symbol),
    CONSTRAINT chart_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE iex.company
(
    act_symbol text NOT NULL,
    company_name text,
    exchange text,
    industry text,
    sub_industry text,
    website text,
    description text,
    ceo text,
    issue_type iex.issue_type,
    sector text,
    last_seen date,
    CONSTRAINT company_pkey PRIMARY KEY (act_symbol)
);

CREATE TABLE iex.dividend
(
    act_symbol text NOT NULL,
    ex_date date NOT NULL,
    payment_date date NOT NULL,
    record_date date NOT NULL,
    declared_date date NOT NULL,
    amount numeric NOT NULL,
    flag iex.dividend_flag,
    type iex.dividend_type,
    qualified iex.dividend_qualified,
    CONSTRAINT dividend_pkey PRIMARY KEY (act_symbol, ex_date),
    CONSTRAINT dividend_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE iex.split
(
    act_symbol text NOT NULL,
    ex_date date NOT NULL,
    payment_date date,
    record_date date,
    declared_date date,
    to_factor numeric NOT NULL,
    for_factor numeric NOT NULL,
    CONSTRAINT split_pkey PRIMARY KEY (act_symbol, ex_date),
    CONSTRAINT split_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE OR REPLACE FUNCTION iex.split_adjusted_chart(arg_act_symbol text, arg_start_date date, arg_end_date date)
 RETURNS TABLE(act_symbol text, date date, open numeric, high numeric, low numeric, close numeric, volume numeric)
 LANGUAGE sql
AS $function$
select
  act_symbol,
  date,
  trunc(open / mul(split_ratio), 4) as open,
  trunc(high / mul(split_ratio), 4) as high,
  trunc(low / mul(split_ratio), 4) as low,
  trunc(close / mul(split_ratio), 4) as close,
  trunc(volume * mul(split_ratio), 4) as volume
from
  (select
    c.act_symbol,
    c.date,
    c.open,
    case 
      when c.high is null and c.open >= c.close then c.open
      when c.high is null and c.open < c.close then c.close
      else c.high
    end as high,
    case 
      when c.low is null and c.open >= c.close then c.close
      when c.low is null and c.open < c.close then c.open
      else c.low
    end as low,
    c.close,
    case
      when c.volume is null then 0
      else c.volume
    end as volume,
    s.split_ratio
  from
    iex.chart c
  left join
    (select
      act_symbol,
      ex_date as date,
      to_factor / for_factor as split_ratio
    from
      iex.split
    where
      act_symbol = arg_act_symbol and
      ex_date >= arg_start_date) s
  on
    c.act_symbol = s.act_symbol and
    c.date < s.date
  where
    c.act_symbol = arg_act_symbol and
    c.date >= arg_start_date and
    c.date <= arg_end_date) as adjusted_chart
group by
  act_symbol, date, open, high, low, close, volume
order by
  act_symbol, date;
$function$
;
