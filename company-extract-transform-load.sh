#!/usr/bin/env bash

dir=$(dirname "$0")

racket ${dir}/company-extract.rkt -p "$1"
racket ${dir}/company-transform-load.rkt -p "$1"
