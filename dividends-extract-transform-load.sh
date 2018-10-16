#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%F")
current_year=$(date "+%Y")

racket ${dir}/dividends-extract.rkt -p "$1"
racket ${dir}/dividends-transform-load.rkt -p "$1"

7zr a /var/tmp/iex/dividends/${current_year}.7z /var/tmp/iex/dividends/${today}
