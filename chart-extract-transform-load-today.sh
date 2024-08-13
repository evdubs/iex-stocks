#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%F")
today_year=$(date "+%Y")

racket -y ${dir}/chart-extract.rkt -d ${today} -p "$1" -t "$2"
racket -y ${dir}/chart-transform-load.rkt -d ${today} -p "$1"

7zr a /var/tmp/iex/chart/${today_year}.7z /var/tmp/iex/chart/${today}

# racket -y ${dir}/dump-dolt-ohlcv.rkt -p "$1"
