(ns bitfinex-clj.websocket.core
  (:require [gniazdo.core :as ws]
            [cheshire.core :as json]
            [clojure.core.async :refer [go >!]]))

(def ^:private url "wss://api.bitfinex.com/ws/2")

(defprotocol WebSocketActions
  (send [this msg])
  (close [this]))

(defrecord WebSocketClient [conn]
  WebSocketActions
  (send [this msg]
    (ws/send-msg (:conn this) (json/generate-string msg)))
  (close [this]
    (ws/close (:conn this))))

; (defn send
;   [conn msg]
;   (ws/send-msg conn (json/generate-string msg)))
;
; (defn parse-msg [msg]
;   (json/parse-string msg true))

;; use protocol handlers
(defn handlers
  [ch]
  {:on-receive (fn [data] (go (>! ch {:type :on-receive
                                      :data (json/parse-string data true)})))
   :on-connect (fn [_] (go (>! ch {:type :on-connect})))
   :on-error (fn [err] (go (>! ch {:type :on-error
                                   :err err})))
   :on-close (fn [s d] (go (>! ch {:type :on-close})))})

(defn new-websocket-client
  [ch]
  (->WebSocketClient
    (apply ws/connect url (flatten (into [] (handlers ch))))
    ch))

;
; (defn ws-connection [url & {:as handlers}]
;   (let [new-handlers
;         (if-let [on-rcv (handlers :on-receive)]
;           (flatten (into [] (merge handlers {:on-receive (comp on-rcv parse-msg)})))
;           handlers)]
;     (apply ws/connect url new-handlers)))

; (defn close [conn]
;   (ws/close conn))
