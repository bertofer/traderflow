(ns bitfinex-clj.websocket
  (:require [gniazdo.core :as ws]
            [cheshire.core :as json]
            [clojure.core.async :refer [go >!]]))

(def ^:private url "wss://api.bitfinex.com/ws/2")

(defn handlers
  [ch]
  {:on-receive (fn [data] (go (>! ch [:on-receive (json/parse-string data true)])))

   :on-connect (fn [_] (go (>! ch [:on-connect])))

   :on-error (fn [err] (go (>! ch [:on-error err])))

   :on-close (fn [s d] (go (>! ch [:on-close s d])))})

(defprotocol WebSocketActions
  (connect [this])
  (send-msg [this msg])
  (close [this]))

(defrecord WebSocketClient [conn ch]
  WebSocketActions
  (connect [this]
    (let [new-conn (apply ws/connect url (flatten (into [] (handlers ch))))]
      (reset! conn new-conn)))
  (send-msg [this msg]
    (when-let [ws @conn]
      (ws/send-msg ws (json/generate-string msg))))
  (close [this]
    (ws/close @conn)
    (reset! conn nil)))

(defn new-websocket-client
  [ch]
  (map->WebSocketClient {:conn (atom nil) :ch ch}))
