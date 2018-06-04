(ns bitfinex-clj.utils.subscriptions
  (:require [clojure.string :as str]))

;;;;; Symbols
(defn is-funding?
  [s]
  (= (count s) 3))

;;;;;; Subscriptions
(defn subscribe-msg
  "Constructs the subscribe message"
  [channel opts]
  (merge {:event "subscribe" :channel channel} opts))

(defn unsubscribe-msg
  [chan-id]
  {:event "unsubscribe" :chanId chan-id})

;; Transforms from the user defined subscription format to bitfinex websocket one
(defmulti user-sub->ws-sub :channel)

(defmethod user-sub->ws-sub :candles
  [{:keys [symbol timeframe]}]
  (subscribe-msg
   "candles"
   {:key (str "trade:" (name timeframe) ":t" symbol)}))

(defmethod user-sub->ws-sub :ticker
  [{:keys [symbol]}]
  (let [prefix (if (is-funding? symbol) "f" "t")]
    (subscribe-msg
     "ticker"
     {:symbol (str prefix symbol)})))

(defmethod user-sub->ws-sub :trades
  [{:keys [symbol]}]
  (let [prefix (if (is-funding? symbol) "f" "t")]
    (subscribe-msg
     "trades"
     {:symbol (str prefix symbol)})))

(defmethod user-sub->ws-sub :books
  [{:keys [symbol] :as opts}]
  (let [prefix (if (is-funding? symbol) "f" "t")
        opts (merge opts {:symbol (str prefix symbol)})]
    (subscribe-msg
     "book"
     opts)))

;; Transforms from the subscribed message to the user-defined subscription
(defmulti ws-sub->user-sub :channel)
(defmethod ws-sub->user-sub "ticker"
  [{:keys [pair currency]}]
  {:channel :ticker
   :symbol (or pair currency)})
(defmethod ws-sub->user-sub "candles"
  [{:keys [key]}]
  (let [[_ tf s] (str/split key #":")
        symbol (subs s 1)
        timeframe (keyword tf)]
    {:channel :candles
     :symbol symbol
     :timeframe timeframe}))

(defmethod ws-sub->user-sub "trades"
  [{:keys [pair currency]}]
  {:channel :candles
   :symbol (or pair currency)})

(defmethod ws-sub->user-sub "book"
  [{:keys [symbol prec freq len]}]
  (let [opts {:symbol (subs symbol 1) :prec prec :len len}]
    (merge {:channel :books}
           (if freq (merge opts {:freq freq}) opts))))

;; Multi to get default options for each channnel. Used specifically in books
(defn is-raw-book [prec] (= prec "R0"))

(def BOOKS_DEFAULT_PREC "P0")
(def BOOKS_DEFAULT_FREQ "F0")
(def BOOKS_DEFAULT_LEN "25")

(defmulti add-defaults :channel)
(defmethod add-defaults :books
  [opts]
  (let [defaults {:prec BOOKS_DEFAULT_PREC
                  :len BOOKS_DEFAULT_LEN}]
    (if (is-raw-book (:prec opts))
      (merge defaults opts)
      (merge defaults {:freq BOOKS_DEFAULT_FREQ} opts))))

(defmethod add-defaults :default [opts] opts)
