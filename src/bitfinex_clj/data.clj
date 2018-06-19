(ns bitfinex-clj.data
  "Handles data messages"
  (:require [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [bitfinex-clj.subscriptions :as subs]))

(defmacro kat [& ks]
  "Version of spec/cat that uses the same keyword as key and predicate of returned map"
  (let [ks' (mapcat #(repeat 2 %) ks)]
    `(spec/cat ~@ks')))

(defmacro defentity
  "Given an entity symbol and a vector of namespaced keys,
  it defines a tuple spec with expanded keys and name ::[entity],
  and defines a fn with name ->[entity] to transform data in the spec
  format to a map with the same namespaced keys and values in data"
  [entity ks]
  `(do
     (spec/def ~(keyword (str *ns*) (name entity)) (spec/tuple ~@ks))
     (defn ~(symbol (str "->" (name entity)))
       [data#]
       (zipmap ~ks data#))))

;;;; Top level trading properties
(spec/def ::chan-id integer?)
(spec/def ::id integer?) ; Generic ID
(spec/def ::timestamp integer?)

;; Candle properties
(spec/def ::open number?)
(spec/def ::close number?)
(spec/def ::high number?)
(spec/def ::low number?)
(spec/def ::volume number?)

;; Ticker properties
(spec/def ::bid number?)
(spec/def ::bid-period integer?)
(spec/def ::bid-size number?)
(spec/def ::ask number?)
(spec/def ::ask-period integer?)
(spec/def ::ask-size number?)
(spec/def ::daily-change number?)
(spec/def ::daily-change-perc number?)
(spec/def ::last-price number?)
(spec/def ::daily-volume number?)
(spec/def ::daily-high number?)
(spec/def ::daily-low number?)
(spec/def ::flash-return-rate number?)

;; Trades properties
(spec/def ::amount number?)
(spec/def ::price number?)
(spec/def ::rate number?)
(spec/def ::period integer?)

;; Books properties
(spec/def ::order-id integer?)
(spec/def ::offer-id integer?)
(spec/def ::price number?)
(spec/def ::rate number?)
(spec/def ::period number?)
(spec/def ::count integer?)
(spec/def ::amount number?)

;;;; Utility functions to deal with data transformations
(defn ->snapshot
  [->update data]
  (mapv ->update data))

(defn ->out-message
  [sub type data]
  {::subscription sub
   ::message-type type
   ::data data})

(defn channel-from-message
  [{:keys [ws-subscriptions]} msg]
  (let [sub (subs/subscription-from-chan-id ws-subscriptions (first msg))]
    (:channel sub)))

;; data types out? :snapshot :update, :error?, :network?
(defmulti process-data
  "Given [state actions], and new message in bitfinex ws format,
  process the message adding the necessary actions to be taken,
  and returning [state actions]"
  (fn [[state actions] msg]
    (or (channel-from-message state msg)
        :unknown)))

;; Candles
(defentity candle
  [::timestamp ::open ::close ::high ::low ::volume])

(spec/def ::candle-list (spec/coll-of ::candle))
(spec/def ::candle-snapshot (kat ::chan-id ::candle-list))
(spec/def ::candle-update (kat ::chan-id ::candle))

(spec/def ::candle-message (spec/or :snapshot ::candle-snapshot
                                    :update ::candle-update)))

(defmethod process-data :candles
  [[{:keys [ws-subscriptions] :as state} actions] msg]
  (let [sub (subs/subscription-from-chan-id ws-subscriptions (first msg))
        conformed (spec/conform ::candle-message msg)
        [type ws-data] conformed
        data (case type
              :snapshot (->snapshot ->candle (::candle-list ws-data))
              :update (->candle (::candle ws-data)))
        out (->out-message sub type data)]

    [state (conj actions (acts/create-action :out out))]))

;; Trades
(defentity trade-trading [::id ::timestamp ::amount ::price])
(defentity trade-funding [::id ::timestamp ::amount ::rate ::period])

(def trade-trading-message-types #{"te" "tu"})
(def trade-funding-message-types #{"fte" "ftu"})

(spec/def ::trade-message-type (set/union trade-trading-message-types trade-funding-message-types))

(spec/def ::trade (spec/or :trading ::trade-trading
                           :funding ::trade-funding))

(spec/def ::trade-list (spec/coll-of ::trade))
(spec/def ::trade-snapshot (kat ::chan-id ::trade-list))
(spec/def ::trade-update (kat ::chan-id ::trade-message-type ::trade))

(spec/def ::trade-message (spec/or :snapshot ::trade-snapshot
                                    :update ::trade-update))

(def new-trade-fns {"fte" ->trade-funding
                    "ftu" ->trade-funding
                    "te" ->trade-trading
                    "tu" ->trade-trading})

(defmethod process-data :trades
  [[state acitons] msg]
  (let [sub (subs/subscription-from-chan-id ws-subscriptions (first msg))
        conformed (spec/conform ::trade-message msg)
        [snap-or-update ws-data] conformed
        new-fn ((::trade-message-type ws-data) new-trade-fns)
        data (case type
              :snapshot (->snapshot new-fn (::trade-list ws-data))
              :update (new-fn (::trade ws-data)))
        out (->out-message sub snap-or-update data)]

    [state (conj actions (acts/create-action :out out))]))

;; Tickers
(defentity ticker-trading
  [::bid ::bid-size ::ask ::ask-size ::daily-change ::daily-change-perc ::last-price
   ::daily-volume ::daily-high ::daily-low])

(defentity ticker-funding
  [::flash-return-rate ::bid ::bid-period ::bid-size ::ask ::ask-period ::ask-size
   ::daily-change ::daily-change-perc ::last-price ::volume ::high ::low])

(spec/def ::ticker (spec/or :ticker-trading ::ticker-trading
                            :ticker-funding ::ticker-funding))

(spec/def ::ticker-message (kat ::chan-in ::ticker))

(def new-ticker-fns {:ticker-trading ->ticker-trading
                     :ticker-funding ->ticker-funding})

(defmethod process-data :tickers
  [[state actions] msg]
  (let [sub (subs/subscription-from-chan-id ws-subscriptions (first msg))
        conformed (spec/conform ::ticker-message msg)
        [_ [type ws-data]] conformed
        new-fn (type new-ticker-fns)
        data (new-fn ws-data)
        out (->out-message sub data)]

    [state (conj actions (acts/create-action :out out))]))

;; Books
(defentity book-trading [::price ::count ::amount])
(defentity book-funding [::rate ::period ::count ::amount])
(defentity raw-book-trading [::order-id ::price ::amount])
(defentity raw-book-funding [::offer-id ::period ::rate ::amount])

(spec/def ::any-book (spec/or :trading ::book-trading
                              :funding ::book-funding
                              :raw-trading ::raw-book-trading
                              :raw-funding ::raw-book-funding))

(spec/def ::book-list (spec/coll-of ::any-book))

(spec/def ::book-update (spec/tuple ::chan-id ::any-book))
(spec/def ::book-snapshot (spec/tuple ::chan-in ::book-list))

(spec/def ::book-message (spec/or :snapshot ::book-snapshot
                                  :update ::book-update))

(defmethod process-data :books
  [[state actions] msg]
  (let [sub (subs/subscription-from-chan-id ws-subscriptions (first msg))])
        

  (let [sub (subs/subscription-from-chan-id ws-subscriptions (first msg))
        conformed (spec/conform ::ticker-message msg)
        [_ [type ws-data]] conformed
        new-fn (type new-ticker-fns)
        data (new-fn ws-data)
        out (->out-message sub data)]

    [state (conj actions (acts/create-action :out out))]))

(defmethod process-data :unknown
  [[state actions] msg]
  [state actions])

(defmethod process-data :default
  [[state actions] msg]
  [state actions])
