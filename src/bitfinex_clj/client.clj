(ns bitfinex-clj.client
  (:require [cheshire.core :as json]
            [clojure.spec.alpha :as spec]
            [clojure.core.async :as async :refer [go >!! >! <! <!! sub pipe chan]]
            [bitfinex-clj.core :as core]
            [bitfinex-clj.websocket :as ws]
            [bitfinex-clj.connection]
            [bitfinex-clj.effects]))

(defn connect [{:keys [queue]}]
  (go (>! queue [:connect])))

(defn close [{:keys [queue out]}]
  (go (>! queue [:close])))
      ; (async/close! queue)
      ; (async/close! out)))

(defn client
  ([]
   (client {}))
  ([config]
   (let [queue (async/chan 100)

         out (async/chan)

         publication (async/pub out (fn [x] (first x)))

         wsclient (ws/new-websocket-client queue)

         db (atom {})

         cofx (fn [] @db)

         fx-deps {:ws wsclient
                  :out out
                  :queue queue
                  :db db}

         process-loop (core/process-loop {:queue queue
                                          :fx-deps fx-deps
                                          :cofx-fn cofx})]

     {:queue queue
      :publication publication
      :process-loop process-loop})))

(defn sub-messages
  ([p t ch] (sub-messages p t ch true))
  ([p t ch close?]
   (let [c (chan 1 (map second))] ; unwraps message from [topic msg]
     (core/sub-pipe p t c ch close?))))
