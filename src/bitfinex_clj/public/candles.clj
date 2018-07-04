(ns bitfinex-clj.public.candles
  (:require [clojure.spec.alpha :as spec]
            [clojure.core.async :as async :refer [go >! pipe chan sub alts! sliding-buffer]]
            [bitfinex-clj.core :as core]
            [bitfinex-clj.client :as client]))

;; options spec
(spec/def ::timeframe #{:1m :5m :15m :30m :1h :3h :6h :12h :1D :7D :14D :1M})
(spec/def ::options (spec/keys :req-un [::core/symbol ::timeframe]))

;; Data spec
(spec/def ::timestamp integer?)
(spec/def ::open number?)
(spec/def ::close number?)
(spec/def ::high number?)
(spec/def ::low number?)
(spec/def ::volume number?)

(core/defentity candle
  [::timestamp ::open ::close ::high ::low ::volume])

(spec/def ::candle-snapshot (spec/cat :chan-id ::core/chan-id :candle (spec/coll-of ::candle)))
(spec/def ::candle-update (spec/cat :chan-id ::core/chan-id :candle (spec/spec ::candle)))

;; Subscribe API
(defn ->candle-subscription
  [{:keys [symbol timeframe]}]
  (core/->subscription "candles" {:key (str "trade:" (name timeframe) ":" symbol)}))

(defn subscribe!
  [{:keys [queue]} opts]
  {:pre [(spec/assert ::options opts)]}
  (go (>! queue [:subscribe (->candle-subscription opts)])))

(defn unsubscribe!
  [{:keys [queue]} opts]
  {:pre [(spec/assert ::options opts)]}
  (go (>! queue [:unsubscribe (->candle-subscription opts)])))

;; Subscriptions
(defn sub-update
  ([client opts ch] (sub-update client opts ch true))
  ([client opts ch close?]
   (let [t (->candle-subscription opts)
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (core/filter-spec ::candle-update)
                       (map core/public-msg->data)
                       (map ->candle)))]

     (core/sub-pipe (:publication client) t c ch close?))))

(defn sub-snapshot
  ([client opts ch] (sub-snapshot client opts ch true))
  ([client opts ch close?]
   (let [t (->candle-subscription opts)
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (core/filter-spec ::candle-snapshot)
                       (map core/public-msg->data)
                       (map (partial core/->entity-snapshot ->candle))))]

     (core/sub-pipe (:publication client) t c ch close?))))

;; aggregator
(def DEFAULT_MAX_CANDLES 240)

; Uses sorted map to order by timestamp
(defn ->candles-smap
  [& candles]
  (->> candles
      (map #(identity [(:timestamp %) %]))
      (into (sorted-map))))

(defn aggregated-snapshot-xf
  ([] (aggregated-snapshot-xf DEFAULT_MAX_CANDLES))
  ([max]
   (fn [xf]
     (let [aggr (volatile! [])]
       (fn ([] (xf))
           ([result] (xf result))
           ([result input]
            (let [v (cond-> input
                      (core/is-valid? ::candle-snapshot input)
                      (->> (core/public-msg->data)
                           (map ->candle)
                           (take max))

                      (core/is-valid? ::candle-update input)
                      (->> (core/public-msg->data)
                           (->candle)
                           (->candles-smap)
                           (merge (apply ->candles-smap @aggr))
                           (map #(val %))
                           (reverse)
                           (take 240)))]

              (vreset! aggr v)
              (xf result v))))))))

(defn sub-aggregated-snapshot
  ([client opts ch] (sub-update client opts ch true))
  ([client opts ch close?]
   (let [t (->candle-subscription opts)
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (aggregated-snapshot-xf (or (:max opts) DEFAULT_MAX_CANDLES))))]

      (core/sub-pipe (:publication client) t c ch close?))))



;; test
; (def options {:symbol "tBTCUSD" :timeframe :1m})
;
; (def snapshot (chan 1 (map #(last %))))
; (def snapshot-aggr (chan 1 (map #(identity [(last %) (count %)]))))
; (def update-ch (chan))
;
; (async/go-loop []
;   (let [[v p] (alts! [snapshot update-ch snapshot-aggr])]
;     (cond
;       (= p snapshot) (prn "snapshot" v)
;       (= p update-ch) (prn "update" v)
;       (= p snapshot-aggr) (prn "snapshot-aggr" v)
;       :default (prn v p))
;     (recur)))
;
; (def c (client/client))
;
; (def update-sub (sub-update c options update-ch false))
; (def snapshot-sub (sub-snapshot c options snapshot false))
; (def aggr-sub (sub-aggregated-snapshot c options snapshot-aggr false))
;
; (client/connect c)
; (client/close c)
;
; (subscribe! c options)
; (unsubscribe! c options)
