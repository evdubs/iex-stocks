#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%F")
current_year=$(date "+%Y")

racket -y ${dir}/volume-extract.rkt -p "$1" -t "$2"
racket -y ${dir}/volume-transform-load.rkt -p "$1"

7zr a /var/tmp/iex/volume/${current_year}.7z /var/tmp/iex/volume/${today}
