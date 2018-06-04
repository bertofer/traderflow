(ns bitfinex-clj.ws
  (:require [clojure.core.async :refer [go go-loop >! <!]]
            [cheshire.core :as json]
            [bitfinex-clj.websocket.core :as websocket]))

(def ^:private url "wss://api.bitfinex.com/ws/2") ; goes on connector

;;;; Loop for Websocket actions
(defmulti ws-action (fn [msg _ _] (msg :type)))

(defn new-ws-connection
  [url events]
  (websocket/get-ws-connection
    url
    :on-receive (fn [data] (go (>! events {:type :on-receive
                                           :data data})))
    :on-connect (fn [_] (go (>! events {:type :on-connect})))
    :on-error (fn [err] (go (>! events {:type :on-error
                                        :err err})))
    :on-close (fn [s d] (go (>! events {:type :on-close})))))

(defmethod ws-action :connect
  [action ws events]
  (reset! ws (new-ws-connection url events)))

(defmethod ws-action :close
  [action ws _]
  (when-let [conn @ws]
    (websocket/close conn)
    (reset! ws nil)))

(defmethod ws-action :send
  [action ws _]
  (websocket/send-msg @ws (action :data)))

(defmethod ws-action :default
  [action ws _]
  (prn "WebSocket action not found"))

(defn ws-actions-loop
  [ws ch events]
  (go-loop []
    (when-let [msg (<! ch)]
      (ws-action msg ws events)
      (recur))))
