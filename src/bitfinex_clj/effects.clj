(ns bitfinex-clj.effects
  (:require [clojure.core.async :as async :refer [go >! <!]]
            [bitfinex-clj.websocket :as ws]
            [bitfinex-clj.core :as core]))

(defmethod core/effect-handler :connect
  [_ {:keys [ws]}]
  (ws/connect ws))


(defmethod core/effect-handler :send
  [[_ msg] {:keys [ws]}]
  (doseq [m (flatten [msg])]
    (ws/send-msg ws m)))


(defmethod core/effect-handler :out
  [[_ msg] {:keys [out]}]
  (go (>! out msg)))


(defmethod core/effect-handler :close
  [_ {:keys [ws]}]
  (ws/close ws))


(defmethod core/effect-handler :db
  [[_ new-state] {:keys [db]}]
  (when-not (= @db new-state)
    (reset! db new-state)))


(defmethod core/effect-handler :dispatch
  [[_ evt] {:keys [queue]}]
  (go (>! queue evt)))

(defmethod core/effect-handler :log
  [[_ data] _]
  (prn data))

(defmethod core/effect-handler :dispatch-later
  [[_ {:keys [ms event]}] {:keys [queue]}]
  (go (<! (async/timeout ms))
      (>! queue event)))
