(ns bitfinex-clj.connection
  (:require [clojure.spec.alpha :as spec]
            [bitfinex-clj.spec :as s]
            [bitfinex-clj.websocket :as ws]
            [clojure.string :as str]))

(def connection-initial-state {:stsate :disconnected
                               :ws nil
                               :config nil
                               :subscriptions {}
                               :auth? false})

(defn config-connection
  [conn config]
  (assoc conn :config config))

(defn new-connection
  [conn]
  (let [{:keys [new-fn handlers url]} (conn :config)
        ws (apply new-fn url handlers)]
    (assoc conn :ws ws)))

(defn close-connection
  [conn]
  (if-let [ws (conn :ws)]
    (let [{:keys [close-fn]} (conn :config)]
      (close-fn ws)))
  connection-initial-state)

(defn subscribe
  "Subscribe to channel if not yet subscribed, and changes the connection accordingly"
  [conn [ch opts]]
  (let [new-opts [ch (merge (add-sub-defaults opts) opts)]]
    (prn new-opts)
    (let [
          send-fn (-> conn :config :send-fn)
          parsed (user-sub->ws-sub new-opts)
          exists? (not (nil? (get-in conn [:subscriptions new-opts])))]
      (when-not exists?
        (do (send-fn (conn :ws) parsed)
            (assoc-in conn [:subscriptions new-opts] {:state :subscribing
                                                      :chan-id nil}))))))

;; new message multimethod
(defmulti new-message
  "Process the message and changes the connection accordingly"
  (fn [_ _ msg]
    (if (spec/valid? ::s/message-types msg)
      (first (spec/conform ::s/message-types msg))
      :unknown)))

(defmethod new-message :version
  [conn client msg]
  conn)

(defmethod new-message :maintenance-start
  [conn _ msg]
  (assoc conn :auth? false :subscriptions {})
  conn)

(defmethod new-message :maintenance-end
  [conn client msg]
  (ws/close (conn :ws))
  (send (client :connection new-connection))
  conn)

(defmethod new-message :reconnect
  [conn client msg]
  (ws/close (conn :ws))
  (send (client :connection new-connection))
  (assoc conn :auth? false :subscriptions {}))

(defmethod new-message :subscribed
  [conn _ msg]
  (assoc-in
    conn
    [:subscriptions (ws-sub->user-sub msg)]
    {:state :subscribed :chan-id (:chanId msg)}))

(defmethod new-message :unsubscribed
  [conn _ msg]
  conn)

(defmethod new-message :error
  [conn _ msg]
  (prn "ERROR: " msg)
  conn)

(defmethod new-message :pong
  [conn _ msg]
  conn)

(defmethod new-message :conf
  [conn _ msg]
  conn)

(defmethod new-message :heartbeat
  [conn _ msg]
  conn)

(defmethod new-message :data
  [conn _ msg]
  (prn "data " (first msg))
  conn)

(defmethod new-message :unknown
  [conn _ msg]
  (prn "received unknown message " msg)
  conn)

(defn get-connection []
  (let [error-mode :fail
        error-handler (fn [agent err]
                        (let [curr-config @(agent :config)]
                          (restart-agent
                           agent
                           (merge connection-initial-state {:config curr-config}))
                          (send agent new-connection)))
        conn (agent connection-initial-state :error-mode error-mode :error-handler error-handler)]
    conn))
