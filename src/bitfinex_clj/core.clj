(ns bitfinex-clj.core
  (:require [cheshire.core :as json]
            [clojure.spec.alpha :as spec]
            [clojure.core.async :as async]
            [bitfinex-clj.spec.core :as s]
            [bitfinex-clj.sources :as sources]
            [bitfinex-clj.ws :as ws]
            [bitfinex-clj.utils.subscriptions :as subs]))

; ; Enable assertions for options
(spec/check-asserts true)

;;;; Public API
(defn subscribe
  ([client opts]
   {:pre [(spec/assert ::s/subscription opts)]}
   (async/>!! (-> client :chs :user-actions)
              {:type :subscribe
               :data (subs/add-defaults opts)}))
  ([client opts ch]
   nil))

(defn unsubscribe
  [client opts]
  {:pre [(spec/assert ::s/subscription opts)]}
  (async/>!! (-> client :chs :user-actions)
             {:type :unsubscribe
              :data (subs/add-defaults opts)}))

; (defn connect [{:keys [chs]}]
;   (async/>!! (:user-actions chs) {:type :connect}))

(defn close [{:keys [chs]}]
  (async/>!! (:user-actions chs) {:type :close}))

(defn client
  ([]
   (client {}))
  ([config]
   (let [;; state
         state (atom {:user-connected true
                      :user-subscriptions #{}
                      :ws-connected false
                      :ws-subscriptions {}})


         state {:connected? (atom false)
                :user-subscriptions (atom #{})
                :ws (atom nil)
                :ws-subscriptions (atom {})
                :cache (atom {})}

         ;; channels
         chs {:user-actions (async/chan)
              :ws-events (async/chan)}
              ; :ws-actions (async/chan)
              ; :cache-write (async/chan)
              ; :data-out (async/chan)}

         sources-in (async/merge
                      [(chs :user-actions) (chs :ws-events)]
                      100)]
     (async/go (async/>! (:user-actions chs) {:type :connect}))
     {:state state
      :chs chs
      :sources-loop (sources/sources-loop sources-in state chs)
      :ws-actions-loop (ws/ws-actions-loop (state :ws) (chs :ws-actions) (chs :ws-events))
      ; :cache-write-loop (cache-loop (state :cache) (chs :cache-write))
      :data-out-pub (async/pub (chs :data-out) :channel)}))

  ([config ch]))
   ;; Subscribe all config to ch after getting client

(def x (client))

; (connect x)
; (subscribe x {:channel :candles :symbol "BTCUSD" :timeframe :1m})
; (prn (-> @((x :state) :ws-subscriptions)))
;
; (close x)
