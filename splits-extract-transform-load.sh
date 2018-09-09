#!/usr/bin/env bash

dir=$(dirname "$0")

racket ${dir}/splits-extract.rkt -p "$1"
racket ${dir}/splits-transform-load.rkt -p "$1"
