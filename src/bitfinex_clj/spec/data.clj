(ns bitfinex-clj.spec.data
  (:require [clojure.spec.alpha :as s]))

(s/def ::chan-id integer?)
(s/def ::candle (s/cat
                 :ts integer?
                 :open float?
                 :close float?
                 :high float?
                 :low float?
                 :volume float?))
(s/def ::candle-list (s/coll-of ::candle))
(s/def ::candle-snapshot (s/cat :chan-id (s/spec ::chan-id) :candle-list (s/spec ::candle-list)))
(s/def ::candle-update (s/cat :chan-id (s/spec ::chan-id) :candle (s/spec ::candle)))
(s/def ::tick (s/cat
                 :bid float?
                 :bid-size float?
                 :ask float?
                 :ask-size float?
                 :daily-change float?
                 :daily-change-perc float?
                 :last-price float?
                 :volume float?
                 :high float?
                 :low float?))
