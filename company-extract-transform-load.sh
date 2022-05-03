#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%F")
current_year=$(date "+%Y")

racket -y ${dir}/company-extract.rkt -t "$2"
racket -y ${dir}/company-transform-load.rkt -p "$1"

7zr a /var/tmp/iex/company/${current_year}.7z /var/tmp/iex/company/${today}
