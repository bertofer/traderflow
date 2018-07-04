(ns bitfinex-clj.connection
  "Mostly event handlers related to the state of the connection"
  (:require [clojure.spec.alpha :as spec]
            [bitfinex-clj.core :as core]
            [clojure.set :as set]))


(def RECONNECT_TIMEOUT 2000)

; Connect ws event --------------------------------------------
;; Just connects websocket without changing db
(defmethod core/event-handler :connect-ws
  [db in]
  {:connect nil})


; User events -------------------------------------------------


(defmethod core/event-handler :connect
  [db in]
  (when-not (:user-connected? db)
    {:db (assoc db :client-connected? true
                   :client-subscriptions #{})
     :connect nil
     :log ":connect"}))


(defmethod core/event-handler :close
  [db in]
  (when (:client-connected? db)
    {:db (assoc db :client-connected? false
                   :client-subscriptions #{})
     :close nil
     :log ":close"}))


; Websocket events --------------------------------------------


(defmethod core/event-handler :on-connect
  [db in]
  {:db (assoc db :ws-connected? true
                 :ws-maintenance? false)
   :dispatch [:sync-subscriptions]
   :log ":on-connect"})


(defmethod core/event-handler :on-close
  [db in]
  (cond-> {}
    true (assoc :db (assoc db
                      :ws-connected? false
                      :ws-maintenance? false
                      :ws-subscriptions {})
                :log ":on-close")

    (:client-connected? db) (assoc :dispatch-later {:ms RECONNECT_TIMEOUT
                                                    :event [:connect-ws]})))


(defmethod core/event-handler :on-error
  [db in]
  {:close nil
   :dispatch-later {:ms RECONNECT_TIMEOUT
                    :event [:connect-ws]}
   :log ":on-error"})


; New message event -------------------------------------------

; specs

(def event-checker (partial core/field-checker :event))
(def code-checker (partial core/field-checker :code))

(spec/def ::event string?)
(spec/def ::version integer?)
(spec/def ::code integer?)
(spec/def ::chanId integer?)
(spec/def ::channel string?)
(spec/def ::symbol string?)
(spec/def ::pair string?)
(spec/def ::prec string?)
(spec/def ::freq string?)
(spec/def ::len string?)
(spec/def ::key string?)

(spec/def ::message (spec/keys :req-un [::event]))

;; Info events
(spec/def ::info-message (spec/and ::message (event-checker "info")))
(spec/def ::info-code-message (spec/and ::info-message (spec/keys :req-un [::code])))
(spec/def ::version-message (spec/and ::info-message (spec/keys :req-un [::version])))

(spec/def ::reconnect-message (spec/and ::info-code-message (code-checker 20051)))
(spec/def ::maintenance-start-message (spec/and ::info-code-message (code-checker 20060)))
(spec/def ::maintenance-end-message (spec/and ::info-code-message (code-checker 20061)))

;; Other events
(spec/def ::error-message (spec/and ::message (event-checker "error")))
(spec/def ::subscribed-message (spec/and ::message (event-checker "subscribed")))
(spec/def ::unsubscribed-message (spec/and ::message (event-checker "unsubscribed")))
(spec/def ::pong-message (spec/and ::message (event-checker "pong")))
(spec/def ::conf-message (spec/and ::message (event-checker "conf")))

(spec/def ::heartbeat-message (spec/and seq? (fn [[x y]] (= y "hb"))))

(spec/def ::message-types (spec/or :version ::version-message
                             :maintenance-start ::maintenance-start-message
                             :maintenance-end ::maintenance-end-message
                             :reconnect ::reconnect-message
                             :subscribed ::subscribed-message
                             :unsubscribed ::unsubscribed-message
                             :error ::error-message
                             :pong ::pong-message
                             :conf ::conf-message
                             :heartbeat ::heartbeat-message
                             :data seq?
                             :new-message ::message))

; Websocket 'on-receive', calls specific handler for message type
(defmethod core/event-handler :on-receive
  [db [evt msg]]
  (let [s (spec/conform ::message-types msg)]
    (when (spec/valid? ::message-types msg)
      (core/event-handler db [(first s) msg]))))


(defmethod core/event-handler :version
  [db in]
  {:log ":version"
   :out [:version in]})


(defmethod core/event-handler :maintenance-start
  [db in]
  {:db (assoc db :ws-maintenance? true)
   :out [:maintenance-start in]})


(defmethod core/event-handler :maintenance-end
  [db in]
  {:db (assoc db :ws-maintenance? false)
   :close nil
   :out [:maintenance-end in]})


(defmethod core/event-handler :reconnect
  [db in]
  {:close nil
   :dispatch-later {:ms RECONNECT_TIMEOUT
                    :event [:connect-ws]}
   :out [:reconnect in]})


(defmethod core/event-handler :error
  [db in]
  {:log ":error"
   :out [:error in]})


(defmethod core/event-handler :pong
  [db in]
  {:log ":pong"}
  :out [:pong in])


(defmethod core/event-handler :conf
  [db in]
  {:log ":conf"
   :out [:conf in]})


(defmethod core/event-handler :heartbeat
  [db in]
  {:log ":heartbeat"
   :out [:heartbeat in]})


;; Subscriptions events
(def filter-by-state (partial core/filter-by :state))

(defn ->event
  ([evt]
   {:event evt})
  ([evt opts]
   (merge (->event evt) opts)))

(defn ->subscription-state
  ([s]
   (->subscription-state s nil))
  ([s chan-id]
   {:state s :chan-id chan-id}))

(defn ->unsubscription-msg
  "Creates an unsubscribe message"
  [chan-id]
  {:event "unsubscribe" :chanId chan-id})

(defn pending-subs
  [client-subs ws-subs]
  (->> (set/difference
         client-subs
         (into #{} (keys ws-subs)))

       (map (fn [k] [k (->subscription-state :subscribing)]))
       (into {})))

(defn pending-unsubs
  [client-subs ws-subs]
  (as-> ws-subs s
    (select-keys s (set/difference
                     (into #{} (keys ws-subs))
                     client-subs))

    (filter-by-state :subscribed s)
    (map
      (fn [[k v]] [k (->subscription-state :unsubscribing (:chan-id v))])
      s)
    (into {} s)))

(defmethod core/event-handler :sync-subscriptions
  [db & _]
  (let [client-subscriptions (:client-subscriptions db)
        ws-subscriptions (:ws-subscriptions db)

        pending-subs  (pending-subs client-subscriptions ws-subscriptions)
        pending-unsubs (pending-unsubs client-subscriptions ws-subscriptions)

        new-ws-subscriptions  (-> ws-subscriptions
                                  (merge pending-subs pending-unsubs))

        subscribe-messages (map #(->event "subscribe" (key %)) pending-subs)
        unsubscribe-messages (map #(->unsubscription-msg (:chan-id (val %))) pending-unsubs)
        log2 (prn "new-ws-subscriptions" new-ws-subscriptions)]
      {:db (assoc db :ws-subscriptions new-ws-subscriptions)
       ; :log (into subscribe-messages unsubscribe-messages)
       :send (concat subscribe-messages unsubscribe-messages)}))
       ; :log "sync-subscriptions"}))

(defmethod core/event-handler :subscribe
  [{:keys [client-subscriptions ws-subscriptions] :as db} [_ sub]]
  (core/event-handler
    (-> db (assoc :client-subscriptions (conj client-subscriptions sub)))
    [:sync-subscriptions]))

(defmethod core/event-handler :unsubscribe
  [{:keys [client-subscriptions ws-subscriptions] :as db} [_ sub]]
  (core/event-handler
    (-> db (update :client-subscriptions disj sub))
    [:sync-subscriptions]))

(defmethod core/event-handler :subscribed
  [{:keys [ws-subscriptions] :as db} [_ msg]]
  ;; Checks if still in user-subscriptions list, if not, unsubscribes
  (let [subscription (some (fn [sub] (when (core/subset? sub msg) sub)) (keys ws-subscriptions))

        new-db (assoc-in
                 db
                 [:ws-subscriptions subscription]
                 (->subscription-state :subscribed (:chanId msg)))]
    (prn "subscribed:" msg subscription)
    (core/event-handler new-db [:sync-subscriptions])))

(defmethod core/event-handler :unsubscribed
  [db [_ msg]]
  (let [subscription (core/find-key #(= (:chanId msg) (:chan-id %)) (:ws-subscriptions db))

        new-db (dissoc
                 db
                 :ws-subscriptions
                 subscription)]
    (prn "unsubscribed:" msg subscription)
    (core/event-handler new-db [:sync-subscriptions])))

;; Data event handler
(defmethod core/event-handler :data
  [{:keys [ws-subscriptions]} [_ msg]]
  (let [chan-id (first msg)

        subscription (core/find-key #(= (:chan-id %) chan-id) ws-subscriptions)]

    {:out [subscription msg]}))
