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

CREATE TABLE iex.company
(
    act_symbol text COLLATE pg_catalog."default" NOT NULL,
    company_name text,
    exchange text,
    industry text,
    website text,
    description text,
    ceo text,
    issue_type iex.issue_type,
    sector text,
    CONSTRAINT company_pkey PRIMARY KEY (act_symbol),
    CONSTRAINT company_act_symbol_pkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE iex.dividend
(
    act_symbol text COLLATE pg_catalog."default" NOT NULL,
    ex_date date NOT NULL,
    payment_date date NOT NULL,
    record_date date NOT NULL,
    declared_date date NOT NULL,
    amount numeric NOT NULL,
    flag iex.dividend_flag,
    type iex.dividend_type,
    qualified iex.dividend_qualified,
    CONSTRAINT dividend_pkey PRIMARY KEY (act_symbol, ex_date),
    CONSTRAINT dividend_act_symbol_pkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE iex.ohlc
(
    act_symbol text COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    CONSTRAINT ohlc_pkey PRIMARY KEY (act_symbol, date),
    CONSTRAINT ohlc_act_symbol_pkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE iex.split
(
    act_symbol text COLLATE pg_catalog."default" NOT NULL,
    ex_date date NOT NULL,
    payment_date date,
    record_date date,
    declared_date date,
    to_factor numeric NOT NULL,
    for_factor numeric NOT NULL,
    CONSTRAINT split_pkey PRIMARY KEY (act_symbol, ex_date),
    CONSTRAINT split_act_symbol_pkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE iex.volume
(
    act_symbol text COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    venue iex.venue NOT NULL,
    volume bigint NOT NULL,
    CONSTRAINT volume_pkey PRIMARY KEY (act_symbol, date, venue),
    CONSTRAINT volume_act_symbol_pkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
