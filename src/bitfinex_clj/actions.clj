(ns bitfinex-clj.actions
  (:require [bitfinex-clj.websocket :as ws]))

;; Utilities
(defn create-action
  ([type]
   {:type type})
  ([type data]
   (merge {:data data} (create-action type)))
  ([type data timeout]
   (merge {:timeout timeout} (create-action type data))))

(defn can-send?
  [{:keys [ws-connected? ws-maintenance?]}]
  (and ws-connected? (not ws-maintenance?)))

; Actions have type, [data], [timeout]
;   - Types of actions:
;     - connect (?) -> creates new connection
;     - close -> close connection
;     - send -> send message via websocket
;     - out -> puts message on the out channel
;   - All of them are impure functions that connect
;    to the outside world)
(defmulti perform-action! (fn [act & _] (:type act)))

(defmethod perform-action! :connect
  [act out wsclient]
  (ws/connect wsclient))

(defmethod perform-action! :send
  [act out wsclient]
  (ws/send-msg wsclient (:data act)))

(defmethod perform-action! :out
  [act out _])
  ;; decide later

(defmethod perform-action! :close
  [act out wsclient]
  (ws/close wsclient))
