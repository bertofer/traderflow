(ns bitfinex-clj.inputs
  (:require [bitfinex-clj.actions :as acts]
            [bitfinex-clj.subscriptions :as subs]
            [bitfinex-clj.messages :as msgs]))

(def RECONNECT_TIMEOUT 2000)

; Inputs come from either the user or the websocket connections
; The input functions are always pure, and conj to a queue
; the necessary actions to be taken later.
(defmulti process-input (fn [_ in] (:type in)))

;; Coming from user
(defmethod process-input :connect
  [[state actions] in]
  (if-not (:user-connected? state)
    [(assoc state :user-connected? true)
     (-> actions
         (conj (acts/create-action :connect)))]
    [state nil]))

(defmethod process-input :close
  [[state actions] in]
  (if (:user-connected? state)
    [(assoc state :user-connected? false
                  :user-subscriptions #{})
     (-> actions
         (conj (acts/create-action :close)))]
    [state nil]))

(defmethod process-input :subscribe
  [[state actions] in]
  (cond-> [state actions]
    true (subs/process-user-subscription in)
    (acts/can-send? state) (subs/process-sync-ws-subscriptions)))

(defmethod process-input :unsubscribe
  [[state actions] in]
  (cond-> [state actions]
    true (process-user-unsubscription in)
    (acts/can-send? state) (subs/process-sync-ws-subscriptions)))

(defmethod process-input :snapshot
  [[state actions] in]
  nil)

;; Coming from ws
(defmethod process-input :on-connect
  [[state actions] in]
  ;; Start subscribing the subscriptions on user config
  (-> [(assoc state :ws-connected? true) actions]
      (subs/process-sync-ws-subscriptions)))

(defmethod process-input :on-close
  [[state actions] in]
  [(assoc state
     :ws-connected? false
     :ws-maintenance? false
     :ws-subscriptions {})
   (cond-> actions
     (:user-connected? state)
     (conj (acts/create-action :connect nil RECONNECT_TIMEOUT)))])

(defmethod process-input :on-receive
  [[state actions] in]
  (msgs/process-message [state actions] (:data in)))

(defmethod process-input :on-error
  [[state actions] in]
  ; Just reconnect
  [state
   (-> actions
     (conj (acts/create-action :close))
     (conj (acts/create-action :connect nil RECONNECT_TIMEOUT)))])
