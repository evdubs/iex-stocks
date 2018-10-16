#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%F")
current_year=$(date "+%Y")

racket ${dir}/ohlc-extract.rkt -p "$1"
racket ${dir}/ohlc-transform-load.rkt -p "$1"

7zr a /var/tmp/iex/ohlc/${current_year}.7z /var/tmp/iex/ohlc/${today}
