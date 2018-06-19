(ns bitfinex-clj.messages
  (:require [clojure.spec.alpha :as spec]
            [bitfinex-clj.subscriptions :as subs]
            [bitfinex-clj.spec.messages :as ms]
            [bitfinex-clj.actions :as acts]
            [bitfinex-clj.utils :as u]))

;; Predicates
(def event-checker (u/field-checker :event))
(def code-checker (u/field-checker :code))

;;;; Messages specification
;; Event properties
(s/def ::event string?)
(s/def ::version integer?)
(s/def ::code integer?)
(s/def ::chanId integer?)
(s/def ::channel string?)
(s/def ::symbol string?)
(s/def ::pair string?)
(s/def ::prec string?)
(s/def ::freq string?)
(s/def ::len string?)
(s/def ::key string?)

;; Messages structures

;; generic messages
(s/def ::message (s/keys :req-un [::event]))

;; Info events
(s/def ::info-message (s/and ::message (sp/event-checker "info")))
(s/def ::info-code-message (s/and ::info-message (s/keys :req-un [::code])))
(s/def ::version-message (s/and ::info-message (s/keys :req-un [::version])))

(s/def ::reconnect-message (s/and ::info-code-message (sp/code-checker 20051)))
(s/def ::maintenance-start-message (s/and ::info-code-message (sp/code-checker 20060)))
(s/def ::maintenance-end-message (s/and ::info-code-message (sp/code-checker 20061)))

;; Other events
(s/def ::error-message (s/and ::message (sp/event-checker "error")))
(s/def ::subscribed-message (s/and ::message (sp/event-checker "subscribed")))
(s/def ::unsubscribed-message (s/and ::message (sp/event-checker "unsubscribed")))
(s/def ::pong-message (s/and ::message (sp/event-checker "pong")))
(s/def ::conf-message (s/and ::message (sp/event-checker "conf")))

(s/def ::heartbeat (s/and seq? (fn [[x y]] (= y "hb"))))

(s/def ::message-types (s/or :version ::version-message
                             :maintenance-start ::maintenance-start-message
                             :maintenance-end ::maintenance-end-message
                             :reconnect ::reconnect-message
                             :subscribed ::subscribed-message
                             :unsubscribed ::unsubscribed-message
                             :error ::error-message
                             :pong ::pong-message
                             :conf ::conf-message
                             :heartbeat ::heartbeat-message
                             :data seq?))

;; Process new message multimethod
(defmulti process-message
  "Process the new message and returns the new state and new actions."
  (fn [[state actions] msg]
    (let [s (spec/conform ::message-types msg)]
      (if (spec/valid? s)
        (first s)
        :unknown))))

(defmethod process-message :version
  [[state actions] msg]
  [state actions])

(defmethod process-message :maintenance-start
  [[state actions] msg]
  [(assoc state :ws-maintenance? true)
   actions])

(defmethod process-message :maintenance-end
  [[state actions] msg]
  [(assoc state :ws-maintenance? false)
   (conj actions (acts/create-action :close))])

(defmethod process-message :reconnect
  [[state actions] msg]
  [state (conj actions (acts/create-action :close))])

(defmethod process-message :subscribed
  [[state actions] msg]
  ;; Checks if still in user-subscriptions list, if not, unsubscribes
  (let [new-state
        (assoc state :ws-subscriptions (subs/ws-on-subscribed ws-subscrptions msg))]
    (-> [new-state actions]
        (subs/process-sync-ws-subscriptions))))

(defmethod process-message :unsubscribed
  [[state actions] msg]
  (let [new-state
        (assoc state :ws-subscriptions (subs/ws-on-unsubscribed ws-subscrptions msg))]
    (-> [new-state actions]
        (subs/process-sync-ws-subscriptions))))

(defmethod process-message :error
  [[state actions] msg]
  [state actions])

(defmethod process-message :pong
  [[state actions] msg]
  [state actions])

(defmethod process-message :conf
  [[state actions] msg]
  [state actions])

(defmethod process-message :heartbeat
  [[state actions] msg]
  [state actions])

(defmethod process-message :data
  [[state actions] msg]
  ; (let [
  ;       sub (some (fn [[k v]]
  ;                   (when (= (:chan-id v) (:chan-id msg)) k)))])

  (prn "data " (first msg))
  [state actions])

(defmethod process-message :unknown
  [[state actions] msg]
  (prn "received unknown message " msg)
  [state actions])
