(ns bitfinex-clj.utils)

;; Utils and predicates
(defn field-checker
  [prop]
  (fn [value]
   (fn [msg] (= value (prop msg)))))
