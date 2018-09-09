#!/usr/bin/env bash

dir=$(dirname "$0")

racket ${dir}/dividends-extract.rkt -p "$1"
racket ${dir}/dividends-transform-load.rkt -p "$1"
