#!/usr/bin/env bash

dir=$(dirname "$0")

racket ${dir}/volume-extract.rkt -p "$1"
racket ${dir}/volume-transform-load.rkt -p "$1"
