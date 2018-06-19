(ns bitfinex-clj.core
  (:require [cheshire.core :as json]
            [clojure.spec.alpha :as spec]
            [clojure.core.async :as async]
            [bitfinex-clj.process :as process]
            [bitfinex-clj.websocket :as ws]
            [bitfinex-clj.subscriptions :as subs]
            [bitfinex-clj.utils :as utils]))

; ; Enable assertions for options
(spec/check-asserts true)

;;;; Public API specs
(def channel-checker (utils/field-checker :channel))
;; Config

;; Subscriptions
(spec/def ::symbol string?)

; Candlestick data
(spec/def ::timeframe #{:1m :5m :15m :30m :1h :3h :6h :12h :1D :7D :14D :1M})

; Subscription options
(spec/def ::channel #{:candles :ticker :trades :books})
(spec/def ::common-sub (spec/keys :req-un [::channel]))

(spec/def ::candles-sub (spec/merge ::common-sub
                              (spec/keys :req-un [::symbol ::timeframe])))
(spec/def ::ticker-sub (spec/merge ::common-sub
                              (spec/keys :req-un [::symbol])))
(spec/def ::trades-sub (spec/merge ::common-sub
                              (spec/keys :req-un [::symbol])))
(spec/def ::books-sub (spec/merge ::common-sub
                            (spec/keys :req-un [::symbol]
                                    :opt-un [::prec ::len ::freq])))

(spec/def ::subscription
  (spec/or :candles (spec/and ::candles-sub
                              (sp/channel-checker :candles))

           :ticker (spec/and ::ticker-sub
                             (sp/channel-checker :ticker))

           :trades (spec/and ::trades-sub
                             (sp/channel-checker :trades))

           :books (spec/and ::books-sub
                            (sp/channel-checker :books))))


;;;; Public API
(defn subscribe
  ([client opts]
   {:pre [(spec/assert ::subscription opts)]}
   (async/>!! (:in client)
              {:type :subscribe
               :data (subs/add-defaults opts)}))
  ([client opts ch]
   nil))

(defn unsubscribe
  [client opts]
  {:pre [(spec/assert ::subscription opts)]}
  (async/>!! (:in client)
             {:type :unsubscribe
              :data (subs/add-defaults opts)}))

(defn connect [{:keys [in]}]
  (async/>!! in {:type :connect}))

(defn close [{:keys [in]}]
  (async/>!! in {:type :close}))

(defn client
  ([]
   (client {}))
  ([config]
   (let [in (async/chan 100)
         out (async/chan)
         wsclient (ws/new-websocket-client in)]
     ; (async/go (async/>! (:user-actions chs) {:type :connect}))
     {:in in
      :out out
      :out-put (async/pub out :channel)
      :io-loop (proc/process-loop in out wsclient)}))

  ([config ch]))
   ;; Subscribe all config to ch after getting client

; (def x (client))
;
; (connect x)
; (subscribe x {:channel :candles :symbol "BTCUSD" :timeframe :1m})
; (close x)

;; Public api
;; - subscribe -> no sub in message out? -> send also subscription details?
;; - unsubscribe -> nothing, maybe confirmation of unsub.
;; - snapshot (?) ->
;; Auth api
;; - subscribe -> no sub in message out?
;; - new-order, update-order, cancel-order, cancel-order-multi, order-multi-op
;; - calc
;; - new-offer
