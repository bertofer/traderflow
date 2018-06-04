(ns bitfinex-clj.utils.messages
  (:require [clojure.spec.alpha :as spec]
            [bitfinex-clj.utils.subscriptions :as subs]
            [bitfinex-clj.spec.core :as s]))


;; new message multimethod
(defmulti new-message
  "Process the message and changes the connection accordingly"
  (fn [msg _ _]
    (if (spec/valid? ::s/message-types msg)
      (first (spec/conform ::s/message-types msg))
      :unknown)))

(defmethod new-message :version
  [msg state ch]
  nil)

(defmethod new-message :maintenance-start
  [msg state ch])
  ; (assoc conn :auth? false :subscriptions {})
  ; conn)

(defmethod new-message :maintenance-end
  [msg state ch])
  ; (ws/close (conn :ws))
  ; (send (client :connection new-connection))
  ; conn)

(defmethod new-message :reconnect
  [msg state ch]
  [(chs :ws-actions) {:type :close}])

(defmethod new-message :subscribed
  [msg state chs]
  ;; Checks if still in user-subscriptions list, if not, unsubscribes
  (let [sub (subs/ws-sub->user-sub msg)]
    (if (contains? @(state :user-subscriptions) sub)
      (swap! (state :ws-subscriptions)
             assoc
             sub
             {:state :subscribed :chan-id (:chanId msg)})
      (do
        (swap! (state :ws-subscriptions)
               assoc-in
               [sub :state]
               :unsubscribing)
        [(chs :ws-actions) {:type :send
                            :data (subs/unsubscribe-msg (msg :chanId))}]))))


(defmethod new-message :unsubscribed
  [msg state ch]
  (prn "unsubscribed! " msg))

(defmethod new-message :error
  [msg state ch]
  (prn "ERROR: " msg))

(defmethod new-message :pong
  [msg state ch]
  (prn "pong"))

(defmethod new-message :conf
  [msg state ch]
  nil)

(defmethod new-message :heartbeat
  [msg state ch]
  (prn "heartbeat" (first msg)))

(defmethod new-message :data
  [msg state ch]
  (let [
        sub (some (fn [[k v]]
                    (when (= (:chan-id v) (:chan-id msg)) k)))])

  (prn "data " (first msg)))


(defmethod new-message :unknown
  [msg state ch]
  (prn "received unknown message " msg))
