CREATE TABLE iex.ohlc
(
    act_symbol text COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    CONSTRAINT cta_summary_pkey PRIMARY KEY (act_symbol, date),
    CONSTRAINT cta_summary_act_symbol_pkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
