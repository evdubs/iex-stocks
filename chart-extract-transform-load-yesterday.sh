#!/usr/bin/env bash

dir=$(dirname "$0")
yesterday=$(date -d "-1 day" "+%F")
yesterday_year=$(date -d "-1 day" "+%Y")

racket ${dir}/chart-extract.rkt -d ${yesterday} -p "$1" -t "$2"
racket ${dir}/chart-transform-load.rkt -d ${yesterday} -p "$1"

7zr a /var/tmp/iex/chart/${yesterday_year}.7z /var/tmp/iex/chart/${yesterday}
