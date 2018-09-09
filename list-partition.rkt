#lang racket

(provide list-partition)

(define (list-partition l period step)
  (if (> period (length l)) (list l)
      (append (list (take l period))
              (list-partition (drop l step) period step))))
