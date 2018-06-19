(ns bitfinex-clj.process
  "Thread that processes the input sources messages.
  There are 2 different sources merged into the sources:
    - user-actions: actions from the library user (connect, subscribe, close,..)
    - ws-events: Websocket events (on-close, on-connect, on-receive,...)

  It handles the state of 2 different entites:
    - user-subscriptions: Current subscriptions of the user
    - ws-subscriptions: current state of public subscriptions

  A user action usually triggers side-effects and changes in both
  user-subscriptions and ws-state. It sends actions to the websocket
  to fulfill user requests."
  (:require [clojure.core.async :refer [go go-loop >! <! timeout]]
            [bitfinex-clj.websocket :as ws]
            [bitfinex-clj.subscriptions :as subs]
            [bitfinex-clj.inputs :as inputs]
            [bitfinex-clj.actions :as acts]))

;; Creates a queue to be used to queue actions to be processed
(defn queue [] (clojure.lang.PersistentQueue/EMPTY))

(defn process-loop
  [in-ch out-ch wsclient]
  (let [init-state {:user-connected? false
                    :ws-maintenance? false
                    :ws-connected? false
                    :user-subscriptions #{}
                    :ws-subscriptions {}}]
    (go-loop [state init-state]
      (when-let [in (<! in-ch)]
        (let [[new-state actions] (inputs/process-input [state (queue)] in)]
          (doseq [act actions]
            (<! (timeout (or (:timeout act) 0)))
            (acts/perform-action! act out-ch wsclient))
          (recur new-state))))))
