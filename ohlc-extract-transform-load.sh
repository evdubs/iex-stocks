#!/usr/bin/env bash

dir=$(dirname "$0")

racket ${dir}/ohlc-extract.rkt -p "$1"
racket ${dir}/ohlc-transform-load.rkt -p "$1"
