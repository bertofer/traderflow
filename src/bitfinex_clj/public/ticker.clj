(ns bitfinex-clj.public.ticker
  (:require [clojure.spec.alpha :as spec]
            [clojure.core.async :refer [go <! >! <!! >!! pipe chan sub alts! sliding-buffer]]
            [bitfinex-clj.core :as core]
            [bitfinex-clj.client :as client]))

(spec/def ::options (spec/keys :req-un [::core/symbol]))

;; Data spec
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

(core/defentity ticker-trading
  [::bid ::bid-size ::ask ::ask-size ::daily-change ::daily-change-perc ::last-price
   ::daily-volume ::daily-high ::daily-low])

(core/defentity ticker-funding
  [::flash-return-rate ::bid ::bid-period ::bid-size ::ask ::ask-period ::ask-size
   ::daily-change ::daily-change-perc ::last-price ::volume ::high ::low])

(spec/def ::ticker (spec/or :ticker-trading ::ticker-trading
                            :ticker-funding ::ticker-funding))

(spec/def ::ticker-message (spec/cat :chan-id ::core/chan-in :ticker ::ticker))

(def new-ticker-fns {:ticker-trading ->ticker-trading
                     :ticker-funding ->ticker-funding})

(defn get->ticker
  [symbol]
  (if (core/is-trading? symbol)
    ->ticker-trading
    ->ticker-funding))

;; Subscribe API
(defn ->ticker-subscription
  [{:keys [symbol]}]
  (core/->subscription "ticker" {:symbol symbol}))

(defn subscribe!
  [{:keys [queue]} opts]
  {:pre [(spec/assert ::options opts)]}
  (go (>! queue [:subscribe (->ticker-subscription opts)])))

(defn unsubscribe!
  [{:keys [queue]} opts]
  {:pre [(spec/assert ::options opts)]}
  (go (>! queue [:unsubscribe (->ticker-subscription opts)])))


;; Subscriptions
(defn sub-ticker
  ([client opts ch] (sub-ticker client opts ch true))
  ([client opts ch close?]
   (let [t (->ticker-subscription opts)
         ->ticker (get->ticker (:symbol opts))
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (map core/public-msg->data)
                       (map ->ticker)))]

     (core/sub-pipe (:publication client) t c ch close?))))


;test
; (def options {:symbol "tBTCUSD"})
;
; (def ticker-ch (chan))
;
; (go (loop []
;       (let [v (<! ticker-ch)]
;         (prn v)
;         (recur))))
;
; (def c (client/client))
;
; (def ticker-sub (sub-ticker c options ticker-ch false))
;
; (client/connect c)
; (client/close c)
;
; (subscribe! c options)
; (unsubscribe! c options)
