(ns bitfinex-clj.public.trades
  (:require [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async :as async :refer [go go-loop >! <! pipe chan sub alts! sliding-buffer]]
            [bitfinex-clj.core :as core]
            [bitfinex-clj.client :as client]))

(spec/def ::options (spec/keys :req-un [::core/symbol]))

; Data spec
(spec/def ::id any?)
(spec/def ::timestamp integer?)
(spec/def ::amount number?)
(spec/def ::price number?)
(spec/def ::rate number?)
(spec/def ::period integer?)

;; Trades
(core/defentity trade-trading [::id ::timestamp ::amount ::price])
(core/defentity trade-funding [::id ::timestamp ::amount ::rate ::period])

(def trade-trading-message-types #{"te" "tu"})
(def trade-funding-message-types #{"fte" "ftu"})

(spec/def ::trade-message-types (set/union trade-trading-message-types trade-funding-message-types))

(spec/def ::trade (spec/or :trading ::trade-trading
                           :funding ::trade-funding))

(spec/def ::trade-snapshot (spec/cat :chan-id ::core/chan-id :trade-list (spec/coll-of ::trade)))
(spec/def ::trade-update (spec/cat :chan-id ::core/chan-id
                                   :trade-message-type ::trade-message-types
                                   :trade ::trade))
(spec/def ::trade-message (spec/or :snapshot ::trade-snapshot
                                   :update ::trade-update))

(defn get->trade
  [symbol]
  (if (core/is-trading? symbol)
    ->trade-trading
    ->trade-funding))

;; Subscribe API
(defn ->trade-subscription
  [{:keys [symbol]}]
  (core/->subscription "trades" {:symbol symbol}))

(defn subscribe!
  [{:keys [queue]} opts]
  {:pre [(spec/assert ::options opts)]}
  (go (>! queue [:subscribe (->trade-subscription opts)])))

(defn unsubscribe!
  [{:keys [queue]} opts]
  {:pre [(spec/assert ::options opts)]}
  (go (>! queue [:unsubscribe (->trade-subscription opts)])))

;; Subscriptions
(defn sub-update
  ([client opts ch] (sub-update client opts ch true))
  ([client opts ch close?]
   (let [t (->trade-subscription opts)
         ->trade (get->trade (:symbol opts))
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (core/filter-spec ::trade-update)
                       (map core/public-msg->data)
                       (map ->trade)))]

     (core/sub-pipe (:publication client) t c ch close?))))

(defn sub-snapshot
  ([client opts ch] (sub-snapshot client opts ch true))
  ([client opts ch close?]
   (let [t (->trade-subscription opts)
         ->trade (get->trade (:symbol opts))
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (core/filter-spec ::trade-snapshot)
                       (map core/public-msg->data)
                       (map (partial core/->entity-snapshot ->trade))))]

     (core/sub-pipe (:publication client) t c ch close?))))


(def DEFAULT_MAX_TRADES 200)

(defn aggregated-snapshot-xf
  ([->entity] (aggregated-snapshot-xf ->entity DEFAULT_MAX_TRADES))
  ([->entity max]
   (fn [xf]
     (let [aggr (volatile! [])]
       (fn ([] (xf))
           ([result] (xf result))
           ([result input]
            (cond
              (core/is-valid? ::trade-snapshot input)
              (let [v (->> input
                       (core/public-msg->data)
                       (take max)
                       (map ->entity))]
                (vreset! aggr v)
                (xf result v))

              (core/is-valid? ::trade-update input)
              (let [entity (->> input
                                (core/public-msg->data)
                                (->entity))]

                (when-not (some #(when (= (:id entity) (:id %)) %) @aggr)
                  (let [v (as-> @aggr a
                                (apply list a)
                                (conj a entity)
                                (take max a))]

                    (vreset! aggr v)
                    (xf result v)))))))))))


(defn sub-aggregated-snapshot
  ([client opts ch] (sub-aggregated-snapshot client opts ch true))
  ([client opts ch close?]
   (let [t (->trade-subscription opts)
         ->trade (get->trade (:symbol opts))
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (aggregated-snapshot-xf
                         ->trade
                         (or (:max opts) DEFAULT_MAX_TRADES))))]

     (core/sub-pipe (:publication client) t c ch close?))))


; test
; (def options {:symbol "tBTCUSD"})
;
; (def update-ch (chan))
; (def snapshot-ch (chan))
; (def agg-snapshot-ch (chan))
;
; (def gloop (go-loop []
;             (let [v (<! agg-snapshot-ch)]
;               (prn "agg-snapshot" v)
;               (recur))))
;
; (async/close! gloop)
;
; (def c (client/client))
;
; (def update-sub (sub-update c options update-ch false))
; (def snapshot-sub (sub-snapshot c options snapshot-ch false))
; (def agg-snapshot-sub (sub-aggregated-snapshot c options agg-snapshot-ch false))
;
; (client/connect c)
; (client/close c)
;
; (subscribe! c options)
; (unsubscribe! c options)
