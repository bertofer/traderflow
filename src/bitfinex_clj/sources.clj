(ns bitfinex-clj.sources
  "Thread that processes the input sources messages.
  There are 2 different sources merged into the sources:
    - user-actions: actions from the library user (connect, subscribe, close,...)
    - ws-events: Websocket events (on-close, on-connect, on-receive,...)

  It handles the state of 2 different entites:
    - user-subscriptions: Current subscriptions of the user
    - ws-state: current state of subscriptions

  A user action usually triggers side-effects and changes in both user-config and ws-state
  It sends actions to the websocket to fulfill user requests."
  (:require [clojure.core.async :refer [go go-loop >! <! timeout]]
            [bitfinex-clj.utils.subscriptions :as subs]
            [bitfinex-clj.utils.messages :as messages]))

(def RECONNECT_TIMEOUT 2000)

(defmulti source-in (fn [msg _ _] (msg :type)))

;; Coming from user
(defmethod source-in :connect
  [msg {:keys [connected?]} {:keys [ws-actions]}]
  (when-not @connected?
    (reset! connected? true)
    [ws-actions {:type :connect}]))

(defmethod source-in :close
  [msg {:keys [connected?]} {:keys [ws-actions]}]
  (when @connected?
    (reset! connected? false)
    [ws-actions {:type :close}]))

(defmethod source-in :subscribe
  [{:keys [data]}
   {:keys [user-subscriptions ws-subscriptions]}
   {:keys [ws-actions]}]

  (swap! user-subscriptions conj data)
  (swap! ws-subscriptions
         (fn [state]
           (merge {data {:state :unsubscribed
                         :chan-in nil}}
                  state))) ;; State overrides, so if it's already there we do nothing

  ;; We collect the :disconnected ones. Presumably, same that came
  ;; on the subscribe, but no guarantees. This way no double-subscription
  ;; is made avoiding error from bitfinex. Allows for future multiple subscribe
  ;; on one call easily.
  (let [pending (->> @ws-subscriptions
                    (into [])
                    (filter
                      (fn [[_ data]] (= (:state data) :unsubscribed)))
                    (map (fn [[sub _]] sub)))]

    (doseq [i pending]
      (swap! ws-subscriptions
             assoc-in
             [i :state]
             :subscribing))
    (for [i pending]
      [ws-actions {:type :send
                   :data (subs/user-sub->ws-sub i)}])))

(defmethod source-in :unsubscribe
  [_ _ _]
  nil)

(defmethod source-in :snapshot
  [_ _ _]
  nil)

;; Coming from ws
(defmethod source-in :on-connect
  [_ _ _]
  ;; Start subscribing the subscriptions on user config
  (prn "websocket connected"))

(defmethod source-in :on-close
  [msg {:keys [ws-subscriptions connected?]} {:keys [ws-actions]}]
  ;; On close ws-subscriptions are lost
  (reset! ws-subscriptions {})
  (when @connected?
    (go (<! (timeout RECONNECT_TIMEOUT))
        (>! ws-actions {:type :connect}))))

(defmethod source-in :on-receive
  [{:keys [data]} state chs]
  (messages/new-message data state chs))

(defmethod source-in :on-error
  [_ _ _]
  nil)

(defn sources-loop
  [sources state chs]
  (go-loop []
    (when-let [in (<! sources)]
      (let [output (source-in in state chs)]
        (when (sequential? output)
          (doseq [[ch msg] (partition 2 (flatten output))]
            (>! ch msg)))
        (recur)))))
