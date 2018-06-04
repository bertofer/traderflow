(ns bitfinex-clj.spec.core
  (:require [clojure.spec.alpha :as s]))

;; Utils and predicates
(defn field-checker
  [prop]
  (fn [value]
    (fn [msg] (= value (prop msg)))))

(def event-checker (field-checker :event))
(def code-checker (field-checker :code))
(def channel-checker (field-checker :channel))

;; Websocket client options
(s/def ::api-key string?)
(s/def ::api-secret string?)
(s/def ::auth (s/keys :req-un [::api-key ::api-secret]))
(s/def ::public-channels (s/keys :opt-un [::candles]))
(s/def ::auth-channels (s/keys :opt-un [::account]))

(s/def ::client-options (s/keys :opt-un [::public-channels ::auth-channels ::auth]))


;; Generic data
(s/def ::symbol string?)

; Candlestick data
(s/def ::timeframe #{:1m :5m :15m :30m :1h :3h :6h :12h :1D :7D :14D :1M})

;; Subscription options
(s/def ::channel #{:candles :ticker :trades :books})
(s/def ::common-sub (s/keys :req-un [::channel]))

(s/def ::candles-sub (s/merge ::common-sub
                              (s/keys :req-un [::symbol ::timeframe])))
(s/def ::ticker-sub (s/merge ::common-sub
                              (s/keys :req-un [::symbol])))
(s/def ::trades-sub (s/merge ::common-sub
                              (s/keys :req-un [::symbol])))

(s/def ::books-sub (s/merge ::common-sub
                            (s/keys :req-un [::symbol]
                                    :opt-un [::prec ::len ::freq])))

(s/def ::subscription
  (s/or :candles (s/and ::candles-sub
                        (channel-checker :candles))

        :ticker (s/and ::ticker-sub
                       (channel-checker :ticker))

        :trades (s/and ::trades-sub
                       (channel-checker :trades))
        :books (s/and ::books-sub
                       (channel-checker :books))))

;; Subscription

;; Event properties
(s/def :msg-field/event string?)
(s/def :msg-field/version integer?)
(s/def :msg-field/code integer?)
(s/def :msg-field/chanId integer?)
(s/def :msg-field/channel string?)
(s/def :msg-field/symbol ::symbol)
(s/def :msg-field/pair string?)
(s/def :msg-field/prec string?)
(s/def :msg-field/freq string?)
(s/def :msg-field/len string?)
(s/def :msg-field/key string?)

;; Events

;; generic messages
(s/def ::message (s/keys :req-un [:msg-field/event]))

;; Info events
(s/def ::info (s/and ::message (event-checker "info")))
(s/def ::info-code (s/and ::info (s/keys :req-un [:msg-field/code])))
(s/def ::version (s/and ::info (s/keys :req-un [:msg-field/version])))

(s/def ::reconnect (s/and ::info-code (code-checker 20051)))
(s/def ::maintenance-start (s/and ::info-code (code-checker 20060)))
(s/def ::maintenance-end (s/and ::info-code (code-checker 20061)))

;; Other events
(s/def ::error (s/and ::message (event-checker "error")))
(s/def ::subscribed (s/and ::message (event-checker "subscribed")))
(s/def ::unsubscribed (s/and ::message (event-checker "unsubscribed")))
(s/def ::pong (s/and ::message (event-checker "pong")))
(s/def ::conf (s/and ::message (event-checker "conf")))

(s/def ::heartbeat (s/and seq? (fn [[x y]] (= y "hb"))))

(s/def ::message-types (s/or :version ::version
                             :maintenance-start ::maintenance-start
                             :maintenance-end ::maintenance-end
                             :reconnect ::reconnect
                             :subscribed ::subscribed
                             :unsubscribed ::unsubscribed
                             :error ::error
                             :pong ::pong
                             :conf ::conf
                             :heartbeat ::heartbeat
                             :data seq?))
